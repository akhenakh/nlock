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
*   **Configurable keep-alive interval.**
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
		nlock.WithLogger(logger),              // Provide your slog logger
		nlock.WithTTL(30*time.Second),         // Lock expires after 30s if not renewed
		nlock.WithKeepAlive(true),             // Enable automatic renewal (default)
		nlock.WithKeepAliveIntervalFactor(3),  // Refresh lock at 1/3 of TTL (e.g., every 10s)
		nlock.WithBucketName("my_locks"),      // Optional: custom KV bucket name
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
}
```

## Configuration Options

The `NewLockManager` function accepts functional options:

*   `WithTTL(time.Duration)`: Sets the time-to-live for lock keys in the KV store. Defaults to `30s`. Locks are automatically deleted by NATS after this duration if not renewed.
*   `WithRetryInterval(time.Duration)`: Sets the interval between lock acquisition attempts when the lock is already held. Defaults to `250ms`.
*   `WithKeepAlive(bool)`: Enables (default `true`) or disables the automatic background renewal (keep-alive) goroutine for acquired locks. If disabled, you must manually call `Release` before the TTL expires.
*   `WithKeepAliveIntervalFactor(int)`: Sets the factor of the lock TTL to use for the keep-alive interval. The interval is calculated as `TTL / factor`. A smaller factor means more frequent refreshes. Defaults to `3`, must be `2` or greater.
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

**Options and Option Pattern**: Provides a flexible way to configure the lock manager (TTL, Retry Interval, Logger, Bucket Name, KeepAlive, etc.).

**LockManager**: Holds the JetStream context, the KV bucket handle, configured options, and the logger.

**Lock**: Represents an acquired lock. It stores references back to the manager, the lock key, a unique ownerID (UUID) for this instance, the KV revision number when acquired, and synchronization primitives for managing the optional keep-alive goroutine.

### NewLockManager

Sets up default options and applies user overrides.

Validates options (e.g., positive TTL, keep-alive factor >= 2).

Creates/updates the NATS KV bucket (`CreateOrUpdateKeyValue`) with `History: 1` (we only care about the latest lock state) and the configured TTL.

### Acquire

Generates a unique ownerID for the lock instance.

Enters a retry loop:

1.  Attempts `kv.Create()`: This is the core atomic operation. It succeeds only if the key doesn't exist.
2.  **On success**: Creates the `Lock` struct, starts the keep-alive goroutine (if enabled), and returns the lock.
3.  **On `ErrKeyExists`**: Logs that the lock is held and waits for the next retry tick or context cancellation.
4.  **On other errors**: Returns the error immediately.

The loop is managed by a `time.Ticker` and a `select` statement that also listens for context cancellation.

### runKeepAlive

Runs in a separate goroutine if `WithKeepAlive(true)` (the default).

Calculates an interval (TTL / KeepAliveIntervalFactor) to refresh the lock well before it expires.

Periodically calls `kv.Update()` with the lock's current revision. This operation is atomic and refreshes the key's TTL in the bucket.

If the `Update` call succeeds, the local revision number on the `Lock` struct is updated.

**If `Update` fails** (e.g., due to a network error, or if the key was deleted externally), it means the lock has been lost. The goroutine logs an error and **stops running**. It does not modify the local lock state; it simply ceases refresh attempts.

The goroutine is also stopped when its context is cancelled, which is done by the `Release` method.

### Release

Signals the keep-alive goroutine to stop (if running) and waits for it to finish.

Atomically deletes the lock key using `kv.Delete()` with the `jetstream.LastRevision()` option. This is a critical safety check: this instance will only delete the lock if its local revision number matches the one on the server. This prevents a process from incorrectly releasing a lock that it has already lost (e.g., after its keep-alive failed and another process acquired the lock).

Handles errors gracefully, such as `ErrKeyNotFound` (meaning the lock was already gone, which is acceptable) and revision mismatches (which correctly return `ErrLockNotHeld`).

## lockmanager_test.go

The tests use an in-memory NATS server from the `nats-server/v2/test` package for fast and isolated testing.

Tests cover various scenarios:

*   Basic acquire and release.
*   Acquisition timeout when a lock is already held.
*   Sequential acquisition and release.
*   Concurrent acquisition attempts by multiple goroutines to test mutual exclusion.
*   **Keep-alive functionality**: Verifies a lock is held for longer than its TTL because it is being actively refreshed.
*   **TTL expiry**: Verifies a lock is automatically released when TTL expires if keep-alive is disabled.
*   **Release-not-held**: Verifies that releasing a lock twice returns `ErrLockNotHeld`.
*   **Simulated keep-alive failure**: A test manually deletes a lock key from the server and then verifies that a subsequent `Release` call fails with `ErrLockNotHeld`, confirming the lock's state is correctly identified as lost.

## Questions

### What if a process holding a lock crashes without calling `defer`?

If a process crashes (e.g., via `SIGKILL`), the deferred `Release` call does not run.

1.  **Lock Remains Held**: The lock key persists in the NATS KV store.
2.  **Other Processes Block**: Other processes attempting to `Acquire` the same lock will block until their acquisition context times out.
3.  **TTL is the Failsafe**: The Time-To-Live (TTL) configured for the lock is the ultimate recovery mechanism. The lock key will be automatically deleted from the NATS KV store by the server once the TTL expires (measured from the last successful write or keep-alive refresh). After the key is deleted, another process can successfully acquire the lock.

**Implication**: Choosing an appropriate TTL is a critical design decision.
*   A **shorter TTL** (e.g., 15 seconds) reduces the unavailability window after a crash but increases the risk of a lock expiring during a long-running operation if the keep-alive goroutine is starved of CPU or experiences network issues.
*   A **longer TTL** (e.g., 5 minutes) makes the lock more resilient to temporary disruptions but increases the downtime of the protected resource if a process crashes uncleanly.
