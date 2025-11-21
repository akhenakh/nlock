package natslock

import (
	"context"
	"errors"

	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	// Use the nats-server test package
	nstest "github.com/nats-io/nats-server/v2/test"

	natsclient "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Helper function using nats-server/v2/test package
func setupTestServer(t *testing.T) (jetstream.JetStream, func()) {
	t.Helper()

	// Create a temporary directory managed by the test framework
	// This directory will be automatically removed after the test.
	tmpDir := t.TempDir()

	// Configure server options
	opts := nstest.DefaultTestOptions // Start with test defaults
	opts.Port = -1                    // Request a random available port
	opts.JetStream = true             // Enable JetStream
	opts.StoreDir = tmpDir            // Use the temporary directory for storage
	// opts.Trace = true // Uncomment for detailed server tracing if needed
	// opts.Debug = true // Uncomment for server debug logs if needed
	// opts.LogFile = "/path/to/nats_test.log" // Uncomment and set path to log to file

	// Run the server using the test helper
	srv := nstest.RunServer(&opts)

	// Create NATS client connection to the running server
	nc, err := natsclient.Connect(srv.ClientURL())
	if err != nil {
		srv.Shutdown() // Attempt cleanup even on connect failure
		t.Fatalf("Failed to connect to NATS server at %s: %v", srv.ClientURL(), err)
	}

	// Create JetStream context
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		srv.Shutdown()
		t.Fatalf("Failed to create JetStream context: %v", err)
	}

	// Define cleanup function
	cleanup := func() {
		nc.Close()            // Close client connection first
		srv.Shutdown()        // Shutdown the server
		srv.WaitForShutdown() // Wait for shutdown to complete (important!)
		// No need to explicitly remove tmpDir, t.TempDir() handles it
	}

	return js, cleanup
}

func testLogger() *slog.Logger {
	// Configure logger for tests (e.g., quieter level or specific output)
	logLevel := slog.LevelWarn // Default to quieter level
	if os.Getenv("NATS_LOCK_TEST_DEBUG") != "" {
		logLevel = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))
}

func TestAcquireRelease(t *testing.T) {
	js, cleanup := setupTestServer(t) // Use the new helper
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	manager, err := NewLockManager(ctx, js, WithLogger(testLogger()))
	if err != nil {
		t.Fatalf("Failed to create LockManager: %v", err)
	}

	lockKey := "test-lock-simple"

	// Acquire
	lock, err := manager.Acquire(ctx, lockKey)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}
	if lock == nil {
		t.Fatal("Acquire returned nil lock without error")
	}
	t.Logf("Lock acquired: Key=%s, OwnerID=%s", lock.Key(), lock.OwnerID())

	// Verify key exists in KV
	entry, err := manager.kv.Get(ctx, lockKey)
	if err != nil {
		t.Fatalf("Failed to get lock key from KV after acquire: %v", err)
	}
	if string(entry.Value()) != lock.OwnerID() {
		t.Fatalf("KV value mismatch: expected %s, got %s", lock.OwnerID(), string(entry.Value()))
	}

	// Release
	err = lock.Release(ctx)
	if err != nil {
		t.Fatalf("Failed to release lock: %v", err)
	}
	t.Logf("Lock released")

	// Verify key is deleted from KV
	_, err = manager.kv.Get(ctx, lockKey)
	if err == nil {
		t.Fatal("Lock key still exists in KV after release")
	}
	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("Expected ErrKeyNotFound after release, got: %v", err)
	}
}

func TestAcquireTimeout(t *testing.T) {
	js, cleanup := setupTestServer(t) // Use the new helper
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Short retry interval for faster timeout test
	retryInterval := 50 * time.Millisecond
	manager, err := NewLockManager(ctx, js,
		WithLogger(testLogger()),
		WithRetryInterval(retryInterval),
	)
	if err != nil {
		t.Fatalf("Failed to create LockManager: %v", err)
	}

	lockKey := "test-lock-timeout"

	// Acquire first lock
	lock1, err := manager.Acquire(ctx, lockKey)
	if err != nil {
		t.Fatalf("Failed to acquire first lock: %v", err)
	}
	defer lock1.Release(ctx) // Ensure release even if test fails mid-way

	t.Logf("First lock acquired: Key=%s, OwnerID=%s", lock1.Key(), lock1.OwnerID())

	// Attempt to acquire the same lock with a short timeout
	acquireCtx, acquireCancel := context.WithTimeout(context.Background(), retryInterval/2) // Timeout shorter than retry
	defer acquireCancel()

	_, err = manager.Acquire(acquireCtx, lockKey)

	if err == nil {
		t.Fatal("Second acquire succeeded unexpectedly, should have timed out")
	}

	// Check for specific lock timeout error OR context deadline exceeded
	if !errors.Is(err, ErrLockAcquireTimeout) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Expected ErrLockAcquireTimeout or context.DeadlineExceeded, got: %v", err)
	}

	t.Logf("Second acquire failed as expected: %v", err)
}

func TestAcquireAfterRelease(t *testing.T) {
	js, cleanup := setupTestServer(t) // Use the new helper
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	manager, err := NewLockManager(ctx, js, WithLogger(testLogger()))
	if err != nil {
		t.Fatalf("Failed to create LockManager: %v", err)
	}

	lockKey := "test-lock-reacquire"

	// Acquire and release
	lock1, err := manager.Acquire(ctx, lockKey)
	if err != nil {
		t.Fatalf("Failed to acquire first lock: %v", err)
	}
	t.Logf("First lock acquired: OwnerID=%s", lock1.OwnerID())
	err = lock1.Release(ctx)
	if err != nil {
		t.Fatalf("Failed to release first lock: %v", err)
	}
	t.Logf("First lock released")

	// Acquire again
	lock2, err := manager.Acquire(ctx, lockKey)
	if err != nil {
		t.Fatalf("Failed to acquire second lock: %v", err)
	}
	if lock2.OwnerID() == lock1.OwnerID() {
		t.Fatal("Second lock acquired with the same OwnerID as the first")
	}
	t.Logf("Second lock acquired: OwnerID=%s", lock2.OwnerID())

	err = lock2.Release(ctx)
	if err != nil {
		t.Fatalf("Failed to release second lock: %v", err)
	}
	t.Logf("Second lock released")
}

func TestConcurrentAcquire(t *testing.T) {
	js, cleanup := setupTestServer(t) // Use the new helper
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second) // Longer timeout for concurrency test
	defer cancel()

	manager, err := NewLockManager(ctx, js,
		WithLogger(testLogger()),
		WithRetryInterval(50*time.Millisecond), // Faster retry for test
	)
	if err != nil {
		t.Fatalf("Failed to create LockManager: %v", err)
	}

	lockKey := "test-lock-concurrent"
	numGoroutines := 5
	var acquiredCount atomic.Int32
	var wg sync.WaitGroup

	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			// Use shorter timeout per goroutine to avoid one blocking the whole test if it hangs
			localCtx, localCancel := context.WithTimeout(ctx, 5*time.Second)
			defer localCancel()

			t.Logf("[Goroutine %d] Attempting to acquire lock", id)
			lock, err := manager.Acquire(localCtx, lockKey)
			if err != nil {
				// It's expected that some goroutines will fail/timeout
				if errors.Is(err, ErrLockAcquireTimeout) || errors.Is(err, context.DeadlineExceeded) {
					t.Logf("[Goroutine %d] Failed to acquire lock (timeout expected): %v", id, err)
				} else {
					// Log unexpected errors more prominently
					t.Errorf("[Goroutine %d] Failed to acquire lock with unexpected error: %v", id, err)
				}
				return // Don't proceed if acquisition failed
			}

			// If acquired, increment count, hold lock briefly, then release
			acquiredCount.Add(1)
			t.Logf("[Goroutine %d] Acquired lock: OwnerID=%s", id, lock.OwnerID())

			// Simulate work under lock
			time.Sleep(100 * time.Millisecond)

			t.Logf("[Goroutine %d] Releasing lock", id)
			// Use a short timeout for release as well
			releaseCtx, releaseCancel := context.WithTimeout(ctx, 1*time.Second)
			defer releaseCancel()
			if err := lock.Release(releaseCtx); err != nil {
				t.Errorf("[Goroutine %d] Failed to release lock: %v", id, err)
			} else {
				t.Logf("[Goroutine %d] Released lock successfully", id)
			}

		}(i)
	}

	wg.Wait() // Wait for all goroutines to complete

	// Verification: Check that at least one goroutine acquired the lock sequentially.
	finalCount := acquiredCount.Load()
	t.Logf("Total successful acquisitions: %d", finalCount)
	if finalCount == 0 {
		// This could happen if the per-goroutine timeout is too short or retries are too slow
		t.Error("Expected at least one goroutine to acquire the lock, but none did")
	}
	// A count > 0 indicates the basic mutual exclusion worked sequentially.

	// Final check: ensure the lock is actually released in KV
	// Use a context for the final check
	finalCheckCtx, finalCheckCancel := context.WithTimeout(ctx, 2*time.Second)
	defer finalCheckCancel()
	_, err = manager.kv.Get(finalCheckCtx, lockKey)
	if err == nil {
		t.Error("Lock key still exists in KV after all goroutines finished")
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Errorf("Expected ErrKeyNotFound after test, got: %v", err)
	} else {
		t.Logf("Final KV check confirmed lock key is deleted.")
	}
}

func TestKeepAlive(t *testing.T) {
	js, cleanup := setupTestServer(t) // Use the new helper
	defer cleanup()

	lockTTL := 3 * time.Second  // Short TTL for testing keep-alive
	keepAliveFactor := 2        // More frequent refresh (interval = 3s / 2 = 1.5s)
	testDuration := lockTTL * 2 // Hold lock longer than TTL

	// Overall test context timeout
	ctx, cancel := context.WithTimeout(context.Background(), testDuration+5*time.Second)
	defer cancel()

	manager, err := NewLockManager(ctx, js,
		WithLogger(testLogger()),
		WithTTL(lockTTL),
		WithKeepAlive(true),
		WithKeepAliveIntervalFactor(keepAliveFactor),
	)
	if err != nil {
		t.Fatalf("Failed to create LockManager: %v", err)
	}

	lockKey := "test-lock-keepalive"

	// Acquire lock
	acquireCtx, acquireCancel := context.WithTimeout(ctx, 5*time.Second) // Context for acquisition
	defer acquireCancel()
	lock, err := manager.Acquire(acquireCtx, lockKey)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}
	initialRevision := lock.revision
	t.Logf("Lock acquired: OwnerID=%s, InitialRevision=%d", lock.OwnerID(), initialRevision)

	// Wait longer than the TTL, relying on keep-alive
	t.Logf("Waiting for %v (longer than TTL %v) for keep-alive to work...", testDuration, lockTTL)
	select {
	case <-time.After(testDuration):
		t.Logf("Wait finished.")
	case <-ctx.Done():
		t.Fatalf("Test context cancelled before keep-alive test duration finished: %v", ctx.Err())
	}

	// Check if the lock key still exists and owner matches after waiting
	getCtx, getCancel := context.WithTimeout(ctx, 2*time.Second)
	defer getCancel()
	entry, err := manager.kv.Get(getCtx, lockKey)
	if err != nil {
		t.Fatalf("Failed to get lock key from KV after waiting (expected keep-alive): %v", err)
	}
	if string(entry.Value()) != lock.OwnerID() {
		t.Fatalf("KV value mismatch after keep-alive: expected %s, got %s", lock.OwnerID(), string(entry.Value()))
	}
	if entry.Revision() <= initialRevision {
		t.Fatalf("Lock revision did not increase after keep-alive: initial=%d, current=%d", initialRevision, entry.Revision())
	}
	t.Logf("Lock still held after %v, revision updated to %d", testDuration, entry.Revision())

	// Release the lock
	releaseCtx, releaseCancel := context.WithTimeout(ctx, 2*time.Second)
	defer releaseCancel()
	err = lock.Release(releaseCtx)
	if err != nil {
		t.Fatalf("Failed to release lock after keep-alive: %v", err)
	}
	t.Logf("Lock released successfully after keep-alive test.")

	// Verify key is deleted
	finalGetCtx, finalGetCancel := context.WithTimeout(ctx, 1*time.Second)
	defer finalGetCancel()
	_, err = manager.kv.Get(finalGetCtx, lockKey)
	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("Expected ErrKeyNotFound after final release, got: %v", err)
	}
}

func TestTTLExpiry(t *testing.T) {
	js, cleanup := setupTestServer(t)
	defer cleanup()

	lockTTL := 1 * time.Second                     // Short TTL
	waitDuration := lockTTL + 500*time.Millisecond // Wait slightly longer than TTL

	// Overall test context timeout
	ctx, cancel := context.WithTimeout(context.Background(), waitDuration+5*time.Second)
	defer cancel()

	manager, err := NewLockManager(ctx, js,
		WithLogger(testLogger()),
		WithTTL(lockTTL),
		WithKeepAlive(false), // Explicitly disable keep-alive
	)
	if err != nil {
		t.Fatalf("Failed to create LockManager: %v", err)
	}

	lockKey := "test-lock-expiry"

	// Acquire lock
	acquireCtx1, acquireCancel1 := context.WithTimeout(ctx, 2*time.Second)
	defer acquireCancel1()
	lock1, err := manager.Acquire(acquireCtx1, lockKey)
	if err != nil {
		t.Fatalf("Failed to acquire first lock: %v", err)
	}
	t.Logf("First lock acquired: OwnerID=%s", lock1.OwnerID())
	// Don't release, let it expire

	// Wait for TTL to expire
	t.Logf("Waiting for %v (longer than TTL %v) for lock to expire...", waitDuration, lockTTL)
	select {
	case <-time.After(waitDuration):
		t.Logf("Wait finished.")
	case <-ctx.Done():
		t.Fatalf("Test context cancelled before expiry wait finished: %v", ctx.Err())
	}

	// Try to acquire again - should succeed as the old lock expired
	t.Logf("Attempting to acquire expired lock...")
	acquireCtx2, acquireCancel2 := context.WithTimeout(ctx, 5*time.Second) // Give acquire some time
	defer acquireCancel2()
	lock2, err := manager.Acquire(acquireCtx2, lockKey)
	if err != nil {
		// Check KV state if acquire fails unexpectedly
		checkCtx, checkCancel := context.WithTimeout(ctx, 1*time.Second)
		defer checkCancel()
		entry, kvErr := manager.kv.Get(checkCtx, lockKey)
		if kvErr == nil {
			t.Logf("KV state before failing acquire: Key=%s, Value=%s, Revision=%d, Created=%s", entry.Key(), string(entry.Value()), entry.Revision(), entry.Created())
		} else {
			t.Logf("KV state before failing acquire: %v", kvErr)
		}
		t.Fatalf("Failed to acquire lock after TTL expiry: %v", err)
	}

	if lock2.OwnerID() == lock1.OwnerID() {
		t.Fatal("Acquired lock with the same owner ID after expiry")
	}
	t.Logf("Second lock acquired successfully after expiry: OwnerID=%s", lock2.OwnerID())

	// Release the second lock
	releaseCtx, releaseCancel := context.WithTimeout(ctx, 1*time.Second)
	defer releaseCancel()
	err = lock2.Release(releaseCtx)
	if err != nil {
		t.Fatalf("Failed to release second lock: %v", err)
	}
	t.Logf("Second lock released.")
}

func TestReleaseNotHeld(t *testing.T) {
	js, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	manager, err := NewLockManager(ctx, js, WithLogger(testLogger()))
	if err != nil {
		t.Fatalf("Failed to create LockManager: %v", err)
	}

	lockKey := "test-lock-release-not-held"

	// Acquire and release normally
	acquireCtx, acquireCancel := context.WithTimeout(ctx, 2*time.Second)
	defer acquireCancel()
	lock, err := manager.Acquire(acquireCtx, lockKey)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	releaseCtx1, releaseCancel1 := context.WithTimeout(ctx, 1*time.Second)
	defer releaseCancel1()
	err = lock.Release(releaseCtx1)
	if err != nil {
		t.Fatalf("Failed to release lock: %v", err)
	}

	// Attempt to release again
	t.Logf("Attempting to release lock again...")
	releaseCtx2, releaseCancel2 := context.WithTimeout(ctx, 1*time.Second)
	defer releaseCancel2()
	err = lock.Release(releaseCtx2)
	if err == nil {
		t.Fatal("Second release succeeded unexpectedly")
	}

	if !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("Expected ErrLockNotHeld on second release, got: %v", err)
	}
	t.Logf("Second release failed as expected: %v", err)
}

func TestKeepAliveLoss(t *testing.T) {
	js, cleanup := setupTestServer(t)
	defer cleanup()

	lockTTL := 2 * time.Second
	keepAliveFactor := 2
	keepAliveInterval := lockTTL / time.Duration(keepAliveFactor) // 1s interval

	// Overall test context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	manager, err := NewLockManager(ctx, js,
		WithLogger(testLogger()),
		WithTTL(lockTTL),
		WithKeepAlive(true),
		WithKeepAliveIntervalFactor(keepAliveFactor),
	)
	if err != nil {
		t.Fatalf("Failed to create LockManager: %v", err)
	}

	lockKey := "test-lock-keepalive-loss"

	// Acquire lock
	lock, err := manager.Acquire(ctx, lockKey)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}
	t.Logf("Lock acquired: OwnerID=%s, Revision=%d", lock.OwnerID(), lock.revision)

	// Wait for at least one keep-alive to run successfully
	time.Sleep(keepAliveInterval + 250*time.Millisecond)

	// Manually delete the key from KV to simulate loss/expiry
	t.Logf("Manually deleting lock key from KV...")
	purgeCtx, purgeCancel := context.WithTimeout(ctx, 1*time.Second)
	defer purgeCancel()
	if err := manager.kv.Purge(purgeCtx, lockKey); err != nil {
		t.Fatalf("Failed to purge key to simulate lock loss: %v", err)
	}
	t.Logf("Manual purge successful.")

	// Wait for the next keep-alive attempt to fail and stop the keep-alive goroutine
	t.Logf("Waiting for keep-alive to detect loss (approx %v)...", keepAliveInterval*2)
	time.Sleep(keepAliveInterval * 2)

	// Attempting to release the lock should now fail because the keep-alive
	// would have stopped, and the revision on the server is gone.
	// The `Release` call will attempt a conditional delete on a non-existent key
	// with a specific revision, which will fail.
	releaseCtx, releaseCancel := context.WithTimeout(ctx, 1*time.Second)
	defer releaseCancel()
	err = lock.Release(releaseCtx)
	if err == nil {
		t.Fatalf("Expected release to fail after simulated lock loss, but it succeeded")
	}

	// The error should indicate the lock was not held, likely due to a revision mismatch
	// or the key not being found, which our implementation wraps.
	if !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("Expected ErrLockNotHeld on release after simulated loss, got: %v", err)
	} else {
		t.Logf("Release after simulated loss failed with expected error: %v", err)
	}
}

func TestDo(t *testing.T) {
	js, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	manager, err := NewLockManager(ctx, js, WithLogger(testLogger()))
	if err != nil {
		t.Fatalf("Failed to create LockManager: %v", err)
	}

	lockKey := "do-test-lock"

	// 1. First call should execute
	executed, err := manager.Do(ctx, lockKey, func(ctx context.Context) error {
		t.Log("Executing critical section 1")
		return nil
	})
	if err != nil {
		t.Fatalf("First Do returned error: %v", err)
	}
	if !executed {
		t.Fatal("First Do should have executed but returned false")
	}

	// 2. Acquire lock manually to block the next Do
	holderLock, err := manager.Acquire(ctx, lockKey)
	if err != nil {
		t.Fatalf("Manual acquire failed: %v", err)
	}
	defer holderLock.Release(ctx)

	// 3. Second call should skip (return false, nil) because lock is held
	executed, err = manager.Do(ctx, lockKey, func(ctx context.Context) error {
		t.Fatal("Second Do executed but should have been skipped")
		return nil
	})
	if err != nil {
		t.Fatalf("Second Do returned error: %v", err)
	}
	if executed {
		t.Fatal("Second Do reported execution true, expected false")
	}
	t.Log("Second Do skipped correctly")
}
