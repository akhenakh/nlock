package natslock

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
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
	// A factor of 3 means the keep-alive will run at 1/3 of the TTL duration.
	DefaultKeepAliveIntervalFactor = 3
)

var (
	// ErrLockAcquireTimeout is returned when acquiring a lock times out.
	ErrLockAcquireTimeout = errors.New("lock acquisition timed out")
	// ErrLockNotHeld is returned when trying to release or refresh a lock that is not held or lost.
	ErrLockNotHeld = errors.New("lock not held or already released/expired")
	// ErrLockAlreadyLocked is a specific error type when creation fails because the key exists.
	ErrLockAlreadyLocked = errors.New("lock key already exists")
	// ErrLockHeld is returned by TryAcquire when the lock is currently held by another owner.
	ErrLockHeld = errors.New("lock already held")
)

// Options configure the LockManager and lock acquisition.
type Options struct {
	TTL                     time.Duration
	RetryInterval           time.Duration
	KeepAlive               bool // Enable automatic keep-alive for acquired locks
	KeepAliveIntervalFactor int  // Factor of TTL for keep-alive interval (TTL / factor)
	Logger                  *slog.Logger
	BucketName              string
	BucketDescription       string
	BucketReplicas          int
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

// WithRetryInterval sets the base interval between lock acquisition attempts.
// A small amount of random jitter will be applied to this interval to prevent thundering herds.
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

// WithKeepAliveIntervalFactor sets the factor of the lock TTL to use for the keep-alive interval.
// The interval is calculated as TTL / factor. A smaller factor means more frequent refreshes.
// Defaults to 3. Must be 2 or greater.
func WithKeepAliveIntervalFactor(factor int) Option {
	return func(o *Options) {
		// Factor must be at least 2 to ensure refresh happens before TTL/2
		if factor >= 2 {
			o.KeepAliveIntervalFactor = factor
		}
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

// WithBucketDescription sets the description for the KV bucket.
func WithBucketDescription(desc string) Option {
	return func(o *Options) {
		o.BucketDescription = desc
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
		TTL:                     DefaultLockTTL,
		RetryInterval:           DefaultRetryInterval,
		KeepAlive:               true,
		KeepAliveIntervalFactor: DefaultKeepAliveIntervalFactor,
		Logger:                  slog.Default(),
		BucketName:              "distributed_locks",
		BucketDescription:       "Distributed locks managed by nlock",
		BucketReplicas:          1,
	}

	for _, opt := range opts {
		opt(&defOpts)
	}

	if defOpts.TTL <= 0 {
		return nil, fmt.Errorf("lock TTL must be positive")
	}
	if defOpts.KeepAliveIntervalFactor < 2 {
		return nil, fmt.Errorf("keep-alive interval factor must be 2 or greater")
	}
	if defOpts.BucketReplicas <= 0 {
		return nil, fmt.Errorf("bucket replicas must be positive")
	}

	logger := defOpts.Logger.With("nats_kv_bucket", defOpts.BucketName)
	logger.Debug("Initializing LockManager")

	kvConfig := jetstream.KeyValueConfig{
		Bucket:      defOpts.BucketName,
		Description: defOpts.BucketDescription,
		TTL:         defOpts.TTL,
		History:     1, // Only need the latest state for a lock
		Replicas:    defOpts.BucketReplicas,
		Storage:     jetstream.MemoryStorage, // Memory is faster; persistence usually not required for ephemeral locks
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

	interval := l.manager.opts.TTL / time.Duration(l.manager.opts.KeepAliveIntervalFactor)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	l.logger.Debug("Keep-alive started", "interval", interval)
	ownerIDBytes := []byte(l.ownerID)

	// Calculate a reasonable network timeout for the update call.
	// It shouldn't rely on RetryInterval, which might be tiny for fast acquisition loops.
	// We use half the keep-alive interval or 2 seconds, whichever is smaller, but at least 100ms.
	updateTimeout := interval / 2
	if updateTimeout > 2*time.Second {
		updateTimeout = 2 * time.Second
	}
	if updateTimeout < 100*time.Millisecond {
		updateTimeout = 100 * time.Millisecond
	}

	for {
		select {
		case <-ctx.Done():
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

			// Use the keep-alive's own context for the update operation.
			updateCtx, updateCancel := context.WithTimeout(ctx, updateTimeout)
			newRevision, err := l.manager.kv.Update(updateCtx, l.key, ownerIDBytes, currentRevision)
			updateCancel()

			if err == nil {
				l.mu.Lock()
				// Only update revision if it hasn't been reset by Release
				if l.revision == currentRevision {
					l.revision = newRevision
					l.logger.Debug("Lock TTL refreshed successfully", "new_revision", newRevision)
				} else {
					// This happens if Release() was called while Update() was in flight.
					// Not strictly an error, just a race condition handled by the lock state.
					l.logger.Debug("Lock revision changed during keep-alive update, likely released")
					l.mu.Unlock()
					return
				}
				l.mu.Unlock()
			} else {
				// Analyze error to decide if we should give up immediately.
				// If the key is not found or revision mismatch, we definitely lost the lock.
				// If it's a timeout, we technically still might hold it until actual TTL expiry,
				// but for safety in a distributed system, we assume issues and stop to fail fast.
				l.logger.Error("Failed to refresh lock TTL, lock considered lost. Stopping keep-alive.", "error", err, "revision_tried", currentRevision)

				// Optionally: trigger a callback here if one was registered to notify the app it lost the lock.
				return
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

	// Use a Timer for retries with jitter to avoid thundering herd.
	timer := time.NewTimer(0)
	defer timer.Stop()

	// Ensure the timer drains if we exit early on the first run
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	// Immediate first try
	firstTry := true

	for {
		if !firstTry {
			// Wait for retry timer or context
			select {
			case <-ctx.Done():
				logger.Warn("Lock acquisition cancelled or timed out", "error", ctx.Err())
				return nil, ErrLockAcquireTimeout
			case <-timer.C:
				logger.Debug("Retrying lock acquisition")
			}
		}
		firstTry = false

		// Attempt to create the lock key atomically
		revision, err := m.kv.Create(ctx, key, ownerIDBytes)
		if err == nil {
			// If context was cancelled right after Create succeeded but before we return,
			// we technically own the lock but the caller will receive a context error.
			// We must rollback (delete) the lock to avoid leaking it until TTL.
			if ctx.Err() != nil {
				logger.Warn("Context cancelled immediately after lock acquisition. Rolling back.", "error", ctx.Err())
				_ = m.kv.Delete(context.Background(), key, jetstream.LastRevision(revision))
				return nil, ctx.Err()
			}

			logger.Info("Lock acquired successfully", "revision", revision)
			lock := &Lock{
				manager:  m,
				key:      key,
				ownerID:  ownerID,
				revision: revision,
				logger:   logger,
			}
			// Start keep-alive if enabled
			if m.opts.KeepAlive && m.opts.TTL > 0 {
				keepAliveCtx, cancel := context.WithCancel(context.Background())
				lock.cancelKeepAlive = cancel
				lock.keepAliveWg.Add(1)
				go lock.runKeepAlive(keepAliveCtx)
				logger.Debug("Keep-alive goroutine started")
			}
			return lock, nil
		}

		// Reset timer for next retry with jitter (±10%)
		jitter := time.Duration(rand.Int63n(int64(m.opts.RetryInterval) / 5)) // 20% spread
		nextWait := m.opts.RetryInterval + jitter - (m.opts.RetryInterval / 10)
		timer.Reset(nextWait)

		// Check specific errors
		if errors.Is(err, jetstream.ErrKeyExists) {
			// Lock held by someone else, continue loop to wait for timer
			continue
		}

		// Unexpected error (network, etc.)
		logger.Error("Failed to acquire lock due to unexpected error", "error", err)
		// For transient network errors, we might want to retry, but for now return error
		return nil, fmt.Errorf("failed to create lock key %q: %w", key, err)
	}
}

// TryAcquire attempts to acquire the lock exactly once without retrying.
// If the lock is available, it returns the Lock object.
// If the lock is already held, it returns ErrLockHeld.
// If a network/infra error occurs, it returns that error.
func (m *LockManager) TryAcquire(ctx context.Context, key string) (*Lock, error) {
	ownerID := uuid.NewString()
	logger := m.logger.With("lock_key", key, "owner_id", ownerID)
	logger.Debug("Attempting to try-acquire lock (one-shot)")

	ownerIDBytes := []byte(ownerID)

	// Attempt to create the lock key atomically
	revision, err := m.kv.Create(ctx, key, ownerIDBytes)
	if err == nil {
		if ctx.Err() != nil {
			logger.Warn("Context cancelled immediately after lock acquisition. Rolling back.", "error", ctx.Err())
			_ = m.kv.Delete(context.Background(), key, jetstream.LastRevision(revision))
			return nil, ctx.Err()
		}

		logger.Info("Lock acquired successfully (try-lock)", "revision", revision)
		lock := &Lock{
			manager:  m,
			key:      key,
			ownerID:  ownerID,
			revision: revision,
			logger:   logger,
		}
		// Start keep-alive if enabled
		if m.opts.KeepAlive && m.opts.TTL > 0 {
			keepAliveCtx, cancel := context.WithCancel(context.Background())
			lock.cancelKeepAlive = cancel
			lock.keepAliveWg.Add(1)
			go lock.runKeepAlive(keepAliveCtx)
			logger.Debug("Keep-alive goroutine started")
		}
		return lock, nil
	}

	if errors.Is(err, jetstream.ErrKeyExists) {
		return nil, ErrLockHeld
	}

	logger.Error("Failed to try-acquire lock due to unexpected error", "error", err)
	return nil, fmt.Errorf("failed to create lock key %q: %w", key, err)
}

// Do executes the provided function 'fn' only if the lock for 'key' can be acquired immediately.
//
// Use this in clustered environments where multiple replicas run the same code,
// but you only want one of them to execute the logic.
//
// Returns:
// - executed (bool): true if the lock was acquired and fn was called, false if lock was busy.
// - err (error): contains errors from NATS (during acquisition) or errors returned by 'fn'.
func (m *LockManager) Do(ctx context.Context, key string, fn func(ctx context.Context) error) (executed bool, err error) {
	lock, err := m.TryAcquire(ctx, key)
	if err != nil {
		if errors.Is(err, ErrLockHeld) {
			// Lock is busy, simply return false (skipped) and no error
			return false, nil
		}
		// Infrastructure/Network error
		return false, err
	}

	// Ensure lock is released when function finishes or panics
	defer func() {
		// Use a detached context for release to ensure it runs even if the parent ctx is cancelled
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if releaseErr := lock.Release(releaseCtx); releaseErr != nil {
			// We log this here because we can't easily return it if 'fn' also returned an error
			m.logger.Error("Failed to release lock in Do", "key", key, "error", releaseErr)
		}
	}()

	// Run the user function
	if runErr := fn(ctx); runErr != nil {
		return true, runErr
	}

	return true, nil
}

// Release attempts to release the acquired lock.
// It stops the keep-alive goroutine (if running) and deletes the lock key from NATS KV.
// It only deletes the key if the revision matches the one held by this Lock instance.
func (l *Lock) Release(ctx context.Context) error {
	l.logger.Debug("Attempting to release lock")

	// Stop the keep-alive goroutine first to prevent race conditions
	if l.cancelKeepAlive != nil {
		l.logger.Debug("Signalling keep-alive goroutine to stop")
		l.cancelKeepAlive()
		l.keepAliveWg.Wait()
		l.cancelKeepAlive = nil
		l.logger.Debug("Keep-alive goroutine finished")
	}

	l.mu.Lock()
	currentRevision := l.revision
	l.revision = 0 // Mark as released locally
	l.mu.Unlock()

	if currentRevision == 0 {
		return ErrLockNotHeld
	}

	l.logger.Info("Releasing lock by deleting key", "revision", currentRevision)

	// Use LastRevision option to ensure we only delete if we are the last writer.
	// This is the "Fencing" mechanism relying on NATS KV revision.
	err := l.manager.kv.Delete(ctx, l.key, jetstream.LastRevision(currentRevision))

	if err == nil {
		l.logger.Info("Lock released successfully")
		return nil
	}

	// Key not found - likely expired via TTL or deleted by another process.
	// From the caller's perspective, the lock is released (we don't hold it anymore).
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		l.logger.Warn("Lock key not found during release, likely expired", "revision_tried", currentRevision)
		return nil
	}

	// Revision mismatch - someone else acquired the lock after our keep-alive failed or expired.
	if errors.Is(err, jetstream.ErrKeyExists) {
		l.logger.Error("Failed to release lock: revision mismatch. The lock was lost to another process.", "error", err)
		return fmt.Errorf("%w: revision mismatch (%v)", ErrLockNotHeld, err)
	}

	l.logger.Error("Failed to delete lock key due to unexpected error", "error", err)
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

// Revision returns the current NATS KV revision of the lock.
// This can be used as a fencing token for external systems.
func (l *Lock) Revision() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.revision
}
