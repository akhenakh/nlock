package natslock

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// DefaultLockTTL is the default time-to-live for a lock key.
	DefaultLockTTL = 30 * time.Second
	// DefaultRetryInterval is the default interval to wait before retrying acquisition.
	DefaultRetryInterval = 250 * time.Millisecond
	// DefaultKeepAliveIntervalFactor is the factor of TTL used for the keep-alive interval (TTL / factor).
	DefaultKeepAliveIntervalFactor = 3
)

var (
	// ErrLockAcquireTimeout is returned when acquiring a lock times out.
	ErrLockAcquireTimeout = errors.New("lock acquisition timed out")
	// ErrLockNotHeld is returned when trying to release or refresh a lock that is not held or lost.
	ErrLockNotHeld = errors.New("lock not held or already released/expired")
	// ErrLockAlreadyLocked is a specific error type when creation fails because the key exists.
	ErrLockAlreadyLocked = errors.New("lock key already exists")
)

// Options configure the LockManager and lock acquisition.
type Options struct {
	TTL            time.Duration
	RetryInterval  time.Duration
	KeepAlive      bool // Enable automatic keep-alive for acquired locks
	Logger         *slog.Logger
	BucketName     string
	BucketReplicas int
}

// Option is a function type for setting LockManager options.
type Option func(*Options)

// WithTTL sets the time-to-live for lock keys.
// Lock keys will be automatically deleted by NATS KV after this duration if not refreshed.
func WithTTL(ttl time.Duration) Option {
	return func(o *Options) {
		if ttl > 0 {
			o.TTL = ttl
		}
	}
}

// WithRetryInterval sets the interval between lock acquisition attempts.
func WithRetryInterval(interval time.Duration) Option {
	return func(o *Options) {
		if interval > 0 {
			o.RetryInterval = interval
		}
	}
}

// WithKeepAlive enables or disables automatic background refresh (keep-alive) for acquired locks.
// Disable this if you manually manage lock refresh/release. Defaults to true.
func WithKeepAlive(enable bool) Option {
	return func(o *Options) {
		o.KeepAlive = enable
	}
}

// WithLogger sets a custom slog logger. Defaults to slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(o *Options) {
		if logger != nil {
			o.Logger = logger
		}
	}
}

// WithBucketName sets the name of the NATS KV bucket used for locks.
// Defaults to "distributed_locks".
func WithBucketName(name string) Option {
	return func(o *Options) {
		if name != "" {
			o.BucketName = name
		}
	}
}

// WithBucketReplicas sets the number of replicas for the lock bucket.
// Defaults to 1. Only relevant in clustered NATS setups.
func WithBucketReplicas(replicas int) Option {
	return func(o *Options) {
		if replicas > 0 {
			o.BucketReplicas = replicas
		}
	}
}

// LockManager manages distributed locks using a NATS KV bucket.
type LockManager struct {
	js     jetstream.JetStream
	kv     jetstream.KeyValue
	opts   Options
	logger *slog.Logger
}

// Lock represents an acquired distributed lock.
type Lock struct {
	manager         *LockManager
	key             string
	ownerID         string
	revision        uint64
	keepAliveWg     sync.WaitGroup
	cancelKeepAlive context.CancelFunc
	mu              sync.RWMutex // Protects revision during keep-alive updates
	logger          *slog.Logger
}

// NewLockManager creates a new LockManager.
// It ensures the necessary NATS KV bucket exists with the configured TTL.
func NewLockManager(ctx context.Context, js jetstream.JetStream, opts ...Option) (*LockManager, error) {
	defOpts := Options{
		TTL:            DefaultLockTTL,
		RetryInterval:  DefaultRetryInterval,
		KeepAlive:      true,
		Logger:         slog.Default(),
		BucketName:     "distributed_locks",
		BucketReplicas: 1,
	}

	for _, opt := range opts {
		opt(&defOpts)
	}

	if defOpts.TTL <= 0 {
		return nil, fmt.Errorf("lock TTL must be positive")
	}
	if defOpts.BucketReplicas <= 0 {
		return nil, fmt.Errorf("bucket replicas must be positive")
	}

	logger := defOpts.Logger.With("nats_kv_bucket", defOpts.BucketName)
	logger.Debug("Initializing LockManager")

	kvConfig := jetstream.KeyValueConfig{
		Bucket:   defOpts.BucketName,
		TTL:      defOpts.TTL,
		History:  1, // Only need the latest state for a lock
		Replicas: defOpts.BucketReplicas,
		Storage:  jetstream.MemoryStorage, // Or FileStorage if persistence across NATS restart is needed
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, kvConfig)
	if err != nil {
		logger.Error("Failed to create or update KV bucket", "error", err)
		return nil, fmt.Errorf("failed to create/update KV bucket %q: %w", defOpts.BucketName, err)
	}
	logger.Info("KV bucket ensured", "ttl", defOpts.TTL, "replicas", defOpts.BucketReplicas)

	return &LockManager{
		js:     js,
		kv:     kv,
		opts:   defOpts,
		logger: logger,
	}, nil
}

// runKeepAlive periodically updates the lock key to refresh its TTL.
func (l *Lock) runKeepAlive(ctx context.Context) {
	defer l.keepAliveWg.Done()
	defer l.logger.Debug("Keep-alive goroutine stopped")

	interval := l.manager.opts.TTL / DefaultKeepAliveIntervalFactor
	if interval <= 0 {
		l.logger.Warn("Keep-alive interval is non-positive, disabling keep-alive")
		return // Should not happen if TTL is positive, but safety check
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	l.logger.Debug("Keep-alive started", "interval", interval)
	ownerIDBytes := []byte(l.ownerID)

	for {
		select {
		case <-ctx.Done():
			l.logger.Info("Keep-alive context cancelled")
			return // Stop keep-alive signaled by Release() or parent context cancellation
		case <-ticker.C:
			l.mu.RLock()
			currentRevision := l.revision
			l.mu.RUnlock()

			if currentRevision == 0 {
				l.logger.Warn("Lock revision is zero, indicating release; stopping keep-alive")
				return // Lock likely released concurrently
			}

			l.logger.Debug("Refreshing lock TTL", "revision", currentRevision)

			// Use context with timeout for the update operation itself
			updateCtx, updateCancel := context.WithTimeout(context.Background(), l.manager.opts.RetryInterval*2) // Use background context for NATS op
			newRevision, err := l.manager.kv.Update(updateCtx, l.key, ownerIDBytes, currentRevision)
			updateCancel()

			if err == nil {
				l.mu.Lock()
				// Only update revision if it hasn't been reset by Release
				if l.revision == currentRevision {
					l.revision = newRevision
					l.logger.Debug("Lock TTL refreshed successfully", "new_revision", newRevision)
				} else {
					l.logger.Warn("Lock revision changed during keep-alive update, likely released", "old_revision", currentRevision, "current_lock_revision", l.revision)
					l.mu.Unlock()
					return // Stop keep-alive as lock state seems inconsistent
				}
				l.mu.Unlock()
			} else {
				// Update failed - lock might be lost (expired, deleted by someone else)
				l.logger.Error("Failed to refresh lock TTL, lock may be lost", "error", err, "revision_tried", currentRevision)
				// Mark the lock as potentially invalid by setting revision to 0? Or just stop keep-alive?
				// Stopping keep-alive is safer, letting Release() handle final cleanup attempt.
				// Optionally: signal lock loss via a channel if needed by the application.
				l.mu.Lock()
				l.revision = 0 // Indicate potential loss
				l.mu.Unlock()
				return // Stop the keep-alive goroutine
			}
		}
	}
}

// Acquire attempts to acquire a lock for the given key.
// It blocks until the lock is acquired or the context is cancelled/times out.
// Returns the acquired Lock or an error.
func (m *LockManager) Acquire(ctx context.Context, key string) (*Lock, error) {
	ownerID := uuid.NewString()
	logger := m.logger.With("lock_key", key, "owner_id", ownerID)
	logger.Debug("Attempting to acquire lock")

	ownerIDBytes := []byte(ownerID)
	retryTicker := time.NewTicker(m.opts.RetryInterval)
	defer retryTicker.Stop()

	for {
		// Attempt to create the lock key atomically
		// Create() fails if key exists, returning ErrKeyExists (which wraps JSErrCodeStreamWrongLastSequence)
		revision, err := m.kv.Create(ctx, key, ownerIDBytes)
		if err == nil {
			// (Lock acquired successfully - code remains the same)
			logger.Info("Lock acquired successfully", "revision", revision)
			lock := &Lock{
				manager:  m,
				key:      key,
				ownerID:  ownerID,
				revision: revision,
				logger:   logger,
			}
			if m.opts.KeepAlive && m.opts.TTL > 0 {
				keepAliveCtx, cancel := context.WithCancel(context.Background())
				lock.cancelKeepAlive = cancel
				lock.keepAliveWg.Add(1)
				go lock.runKeepAlive(keepAliveCtx)
				logger.Debug("Keep-alive goroutine started")
			}
			return lock, nil
		}

		// Check if the error specifically indicates the key already exists.
		// Based on the jetstream errors source, ErrKeyExists wraps the underlying
		// JSErrCodeStreamWrongLastSequence when Create expects sequence 0 but finds > 0.
		if errors.Is(err, jetstream.ErrKeyExists) {
			// Key exists, lock is held by someone else (or potentially stale)
			logger.Debug("Lock already held (received ErrKeyExists), will retry", "error", err)
			// Continue loop after delay, waiting for retry or context cancellation
		} else {
			// A different error occurred (network, permissions, context cancelled during Create, etc.)
			logger.Error("Failed to acquire lock due to unexpected error", "error", err)
			return nil, fmt.Errorf("failed to create lock key %q: %w", key, err)
		}

		// Wait for the next retry tick or context cancellation
		select {
		case <-ctx.Done():
			logger.Warn("Lock acquisition cancelled or timed out", "error", ctx.Err())
			return nil, ErrLockAcquireTimeout
		case <-retryTicker.C:
			logger.Debug("Retrying lock acquisition")
		}
	}
}

// Release attempts to release the acquired lock.
// It stops the keep-alive goroutine (if running) and deletes the lock key from NATS KV.
// It only deletes the key if the revision matches the one held by this Lock instance.
func (l *Lock) Release(ctx context.Context) error {
	l.logger.Debug("Attempting to release lock")

	// Stop the keep-alive goroutine first
	if l.cancelKeepAlive != nil {
		l.logger.Debug("Signalling keep-alive goroutine to stop")
		l.cancelKeepAlive()
		l.keepAliveWg.Wait()
		l.cancelKeepAlive = nil
		l.logger.Debug("Keep-alive goroutine finished")
	}

	// Atomically delete the key if we still hold the correct revision
	l.mu.Lock()
	currentRevision := l.revision
	l.revision = 0 // Mark as released locally immediately
	l.mu.Unlock()

	if currentRevision == 0 {
		l.logger.Warn("Lock already marked as released or lost locally, skipping KV delete")
		return ErrLockNotHeld
	}

	l.logger.Info("Releasing lock by deleting key", "revision", currentRevision)
	// Use LastRevision option to ensure we only delete if we are the last writer
	err := l.manager.kv.Delete(ctx, l.key, jetstream.LastRevision(currentRevision))

	// Handle potential errors during delete
	if err == nil {
		l.logger.Info("Lock released successfully")
		return nil
	}

	if errors.Is(err, jetstream.ErrKeyNotFound) {
		// Key not found - likely expired TTL or deleted by someone else. Still "released".
		l.logger.Warn("Lock key not found during release, likely expired or released by another process", "revision_tried", currentRevision)
		return nil // Considered successful release from this instance's perspective
	}

	// Check if the error indicates the expected revision was wrong.
	// Delete() with LastRevision() option also returns ErrKeyExists if the sequence doesn't match,
	// as ErrKeyExists wraps the underlying JSErrCodeStreamWrongLastSequence code (10071).
	if errors.Is(err, jetstream.ErrKeyExists) {
		l.logger.Error("Failed to release lock: revision mismatch (received ErrKeyExists)", "error", err, "expected_revision", currentRevision)
		return fmt.Errorf("%w: revision mismatch on delete (%v)", ErrLockNotHeld, err) // Include original err for detail
	}

	// Other unexpected errors (network, permissions, context cancelled, etc.)
	l.logger.Error("Failed to delete lock key due to unexpected error", "error", err, "revision_tried", currentRevision)
	return fmt.Errorf("failed to delete lock key %q: %w", l.key, err)
}

// Key returns the key associated with this lock.
func (l *Lock) Key() string {
	return l.key
}

// OwnerID returns the unique ID of the owner who acquired this lock instance.
func (l *Lock) OwnerID() string {
	return l.ownerID
}
