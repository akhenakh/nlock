# nlock - Distributed Locks using NATS KV

[![Go Report Card](https://goreportcard.com/badge/github.com/akhenakh/nlock)](https://goreportcard.com/report/github.com/akhenakh/nlock) <!-- Replace with your actual repo path -->
[![Go Reference](https://pkg.go.dev/badge/github.com/akhenakh/nlock.svg)](https://pkg.go.dev/github.com/akhenakh/nlock) <!-- Replace with your actual repo path -->

A simple Go package providing distributed locking capabilities built on top of [NATS JetStream Key-Value (KV) Store](https://docs.nats.io/nats-concepts/jetstream/key-value-store). It leverages atomic KV operations and TTLs for reliable lock management across multiple application instances.

## Features

*   Simple `Acquire`/`Release` API.
*   Uses NATS KV Store for lock persistence and state.
*   Automatic lock renewal (keep-alive) via background goroutine (configurable).
*   Context-aware operations for timeouts and cancellation.
*   Configurable lock Time-To-Live (TTL).
*   Configurable retry intervals for acquisition attempts.
*   Structured logging integration using Go's standard `log/slog` package.

## Installation

```bash
go get github.com/akhenakh/nlock 
```

## Status

This was generated 99% using Gemini, it is useful to me use it at your own risk ;)

## Usage

Here's a basic example demonstrating how to acquire and release a lock:

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/akhenakh/nlock" 
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	// NATS Connection Setup
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		slog.Error("Failed to connect to NATS", "url", natsURL, "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		slog.Error("Failed to create JetStream context", "error", err)
		os.Exit(1)
	}

	// Logger Setup (Optional)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create Lock Manager 
	// Use a context for potentially long-running setup
	setupCtx, cancelSetup := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelSetup()

	manager, err := nlock.NewLockManager(setupCtx, js,
		nlock.WithLogger(logger),         // Provide your slog logger
		nlock.WithTTL(30*time.Second),    // Lock expires after 30s if not renewed
		nlock.WithKeepAlive(true),        // Enable automatic renewal (default)
		nlock.WithBucketName("my_locks"), // Optional: custom KV bucket name
	)
	if err != nil {
		logger.Error("Failed to create lock manager", "error", err)
		os.Exit(1)
	}
	logger.Info("Lock manager created")

	// Acquire a Lock
	lockKey := "data-processing-job-1"
	logger.Info("Attempting to acquire lock", "key", lockKey)

	// Use a context for the acquisition attempt (e.g., with a timeout)
	acquireCtx, cancelAcquire := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancelAcquire()

	lock, err := manager.Acquire(acquireCtx, lockKey)
	if err != nil {
		// Handle lock acquisition failure
		if errors.Is(err, nlock.ErrLockAcquireTimeout) {
			logger.Error("Could not acquire lock within timeout", "key", lockKey)
		} else {
			logger.Error("Failed to acquire lock", "key", lockKey, "error", err)
		}
		os.Exit(1) // Or retry logic
	}
	// IMPORTANT: Always ensure the lock is released, even on panic
	defer func() {
		releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelRelease()
		logger.Info("Releasing lock in defer", "key", lock.Key())
		if err := lock.Release(releaseCtx); err != nil {
			logger.Error("Failed to release lock in defer", "key", lock.Key(), "error", err)
		}
	}()

	logger.Info("Lock acquired successfully!", "key", lock.Key(), "owner", lock.OwnerID())

	// Critical Section 
	// Only one instance holding the lock for 'lockKey' can execute this code at a time.
	logger.Info("Performing work while holding the lock...")
	time.Sleep(10 * time.Second) // Simulate work
	logger.Info("Work finished.")
	// Keep-alive goroutine (if enabled) refreshes the lock TTL in the background.

	// Lock is released by the deferred call upon function exit.
	// If you need to release it earlier, you can call lock.Release(ctx) explicitly,
	// but the defer is crucial for safety.
}

```

## Configuration Options

The `NewLockManager` function accepts functional options:

*   `WithTTL(time.Duration)`: Sets the time-to-live for lock keys in the KV store. Defaults to `30s`. Locks are automatically deleted by NATS after this duration if not renewed.
*   `WithRetryInterval(time.Duration)`: Sets the interval between lock acquisition attempts when the lock is already held. Defaults to `250ms`.
*   `WithKeepAlive(bool)`: Enables (default `true`) or disables the automatic background renewal (keep-alive) goroutine for acquired locks. If disabled, you must manually call `Release` before the TTL expires.
*   `WithLogger(*slog.Logger)`: Sets a custom `slog` logger instance. Defaults to `slog.Default()`.
*   `WithBucketName(string)`: Sets the name of the NATS KV bucket used to store locks. Defaults to `"distributed_locks"`.
*   `WithBucketReplicas(int)`: Sets the number of replicas for the lock bucket (relevant for clustered NATS). Defaults to `1`.

## Error Handling

The `Acquire` method can return:

*   `nlock.ErrLockAcquireTimeout`: If the lock could not be acquired before the provided context timed out or was cancelled.
*   Other errors related to NATS communication or KV store operations.

The `Release` method can return:

*   `nlock.ErrLockNotHeld`: If the lock was already released, expired, or lost (e.g., due to keep-alive failure or external deletion).
*   Other errors related to NATS communication or KV store operations.

Always check for errors returned by `Acquire` and `Release`.

## Testing

```bash
go test ./...
```


## Details 

lockmanager.go:

Options and Option Pattern: Provides a flexible way to configure the lock manager (TTL, Retry Interval, Logger, Bucket Name, KeepAlive, etc.).

LockManager: Holds the JetStream context, the KV bucket handle, configured options, and the logger.

Lock: Represents an acquired lock. It stores references back to the manager, the lock key, a unique ownerID (UUID) for this instance, the KV revision number when acquired, and synchronization primitives (sync.WaitGroup, context.CancelFunc, sync.RWMutex) for managing the optional keep-alive goroutine.

### NewLockManager

Sets up default options and applies user overrides.

Validates options (positive TTL/Replicas).

Creates/updates the NATS KV bucket (CreateOrUpdateKeyValue) with History: 1 (we only care about the latest lock state) and the configured TTL. Using MemoryStorage for simplicity in tests, but FileStorage might be preferred for production if lock state needs to survive NATS restarts (though TTL usually handles cleanup).

### Acquire

Generates a unique ownerID.

Enters a loop:

Attempts kv.Create(): This is the core atomic operation. It succeeds only if the key doesn't exist.

On success: Creates the Lock struct, starts the keep-alive goroutine (if enabled), and returns the lock.

On ErrKeyExists: Logs that the lock is held and waits for the retry interval or context cancellation.

On other errors: Returns the error immediately.

Uses a time.NewTicker and select on the ticker and the context's Done() channel for retries and cancellation handling.

### runKeepAlive

Runs in a separate goroutine if WithKeepAlive(true) (default).

Calculates an interval (e.g., TTL / 3) to refresh the lock well before it expires.

Periodically calls kv.Update() with the current revision (jetstream.LastRevision is implicitly used by Update when revision is provided). Update refreshes the TTL.

Updates the lock.revision upon successful update.

If Update fails (e.g., ErrKeyNotFound or revision mismatch), it means the lock was lost (expired, deleted elsewhere). It logs an error and stops the keep-alive. It also sets the local revision to 0 as an indicator.

Stops when the context passed to it is cancelled (usually by Release).

### Release

Signals the keep-alive goroutine to stop (if running) and waits for it to finish using context.CancelFunc and sync.WaitGroup.

Atomically deletes the lock key using kv.Delete() with the jetstream.LastRevision() option. This ensures that this instance only deletes the lock if it was the last one to successfully write/update it. This prevents accidentally deleting a lock acquired by another process after this one lost it (e.g., due to network partition or keep-alive failure).

Handles errors like ErrKeyNotFound (meaning it was already gone, which is acceptable for release) and revision mismatches.

## lockmanager_test.go

### NewInProcessNATSServer
The helper function provided in the prompt, slightly adjusted for robustness (waits, error checking, closing client connection).

### Tests Cover various scenarios

Basic acquire/release.

Acquisition timeout when the lock is held.

Acquiring after a previous release.

Concurrent acquisition attempts by multiple goroutines.

Keep-alive functionality (holding lock longer than TTL).

TTL expiry (when keep-alive is disabled).

Attempting to release a lock not held (e.g., after already releasing).

Simulated keep-alive failure (by manually deleting the key).

## Questions

What if a process holding a nlock is crashing without calling the defer?

No clean release occurs.  
The lock remains held in the NATS KV store.  
Other processes cannot acquire the lock immediately.  
The NATS KV TTL feature is the recovery mechanism. The lock automatically becomes available after the TTL expires (counting down from the last write/update time).

Implications:

Availability Delay: There will be a delay (up to the configured TTL) before the resource protected by the lock becomes available again.

Importance of TTL: Choosing an appropriate TTL is critical.

A shorter TTL reduces the unavailability window after a SIGKILL but increases the risk of a lock expiring unexpectedly during normal operation if keep-alive fails or network latency is high (though keep-alive mitigates this significantly).

A longer TTL makes the lock safer during temporary disruptions but increases the potential downtime if a process crashes uncleanly.

