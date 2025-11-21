# nlock - Distributed Locks using NATS KV

[![Go Report Card](https://goreportcard.com/badge/github.com/akhenakh/nlock)](https://goreportcard.com/report/github.com/akhenakh/nlock)
[![Go Reference](https://pkg.go.dev/badge/github.com/akhenakh/nlock.svg)](https://pkg.go.dev/github.com/akhenakh/nlock)

A simple Go package providing distributed locking capabilities built on top of [NATS JetStream Key-Value (KV) Store](https://docs.nats.io/nats-concepts/jetstream/key-value-store). It leverages atomic KV operations and TTLs for reliable lock management across multiple application instances.

## Features

*   **Standard Locking**: Simple `Acquire`/`Release` API with retries.
*   **Execute-Once (Try-Lock)**: High-level `Do` API for running tasks only if the lock is free (perfect for clustered cron jobs).
*   **Native Persistence**: Uses NATS KV Store for lock state.
*   **Auto Renewal**: Automatic lock renewal (keep-alive) via background goroutine.
*   **Context-Aware**: Operations support timeouts and cancellation.
*   **Configurable**: Customizable TTL, retry intervals, and keep-alive frequencies.
*   **Observability**: Structured logging integration via `log/slog`.

## Installation

```bash
go get github.com/akhenakh/nlock 
```

## Status

This was generated 99% using Gemini, it is useful to me use it at your own risk ;)

## Usage

### Basic Locking (Acquire & Release)

Use this pattern when you need to wait for a lock to become available before proceeding.

```go
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/akhenakh/nlock" 
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	// 1. Connect to NATS
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	js, _ := jetstream.New(nc)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// 2. Create Lock Manager
	manager, _ := nlock.NewLockManager(context.Background(), js,
		nlock.WithLogger(logger),
		nlock.WithTTL(30*time.Second),
	)

	// 3. Acquire Lock (blocks until acquired or context timeout)
	lockKey := "database-migration"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lock, err := manager.Acquire(ctx, lockKey)
	if err != nil {
		if errors.Is(err, nlock.ErrLockAcquireTimeout) {
			logger.Error("Could not acquire lock in time")
		}
		return
	}
	
	// 4. Ensure Release
	defer func() {
		if err := lock.Release(context.Background()); err != nil {
			logger.Error("Failed to release", "error", err)
		}
	}()

	logger.Info("Critical section: doing work...")
	time.Sleep(2 * time.Second)
}
```

### Distributed Task Execution (Try-Lock)

Use the `Do` method when you have multiple replicas of a service (e.g., a clustered cron job), and you want **only one** instance to execute a task, while the others skip it immediately if the lock is busy.

```go
func RunCronJob(manager *nlock.LockManager) {
    ctx := context.Background()
    lockKey := "hourly-cleanup-job"

    // Attempt to run the function. 
    // If lock is free: acquires lock -> runs func -> releases lock -> returns executed=true
    // If lock is busy: returns executed=false immediately (no waiting)
    executed, err := manager.Do(ctx, lockKey, func(ctx context.Context) error {
        
        slog.Info("I am the leader, performing cleanup...")
        
        // Simulate heavy lifting
        time.Sleep(500 * time.Millisecond)
        
        return nil
    })

    if err != nil {
        // Error acquiring lock (network issue) or error returned by the closure
        slog.Error("Job failed", "error", err)
        return
    }

    if !executed {
        slog.Info("Job skipped: Lock currently held by another instance")
    } else {
        slog.Info("Job completed successfully")
    }
}
```

## Configuration Options

The `NewLockManager` function accepts functional options:

*   `WithTTL(time.Duration)`: Sets the time-to-live for lock keys. Defaults to `30s`. Locks are auto-deleted by NATS after this time if not renewed.
*   `WithRetryInterval(time.Duration)`: Sets the interval between acquisition attempts in `Acquire`. Defaults to `250ms`.
*   `WithKeepAlive(bool)`: Enables (default `true`) automatic background renewal. If disabled, you must finish work before TTL expires.
*   `WithKeepAliveIntervalFactor(int)`: Refresh frequency. Interval = `TTL / factor`. Defaults to `3`.
*   `WithLogger(*slog.Logger)`: Sets a custom `slog` logger.
*   `WithBucketName(string)`: Sets the NATS KV bucket name. Defaults to `"distributed_locks"`.
*   `WithBucketReplicas(int)`: Sets the number of replicas for the bucket (for clustered NATS). Defaults to `1`.

## Error Handling

*   `nlock.ErrLockAcquireTimeout`: Returned by `Acquire` if context expires before lock is obtained.
*   `nlock.ErrLockHeld`: Returned internally by `TryAcquire` (and handled by `Do`) to indicate the lock is busy.
*   `nlock.ErrLockNotHeld`: Returned by `Release` if the lock expired or was lost before release was called.

## Design Details

1.  **Persistence**: Locks are stored as keys in a NATS KV Bucket.
2.  **Atomic Creation**: Acquisition uses `kv.Create()`, which only succeeds if the key does not exist.
3.  **Atomic Release**: Release uses `kv.Delete(..., LastRevision(rev))`, ensuring you only delete the lock if you are still the owner.
4.  **Failsafe**: If a process crashes, the NATS KV TTL ensures the lock key is automatically removed after the configured duration, allowing other processes to eventually acquire it.
