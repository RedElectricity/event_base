# Shutdown Strategies

`event_base` provides 7 shutdown strategies for gracefully or forcefully terminating workers. Shutdown can be triggered programmatically or via OS signals.

---

## Shutdown channel

A broadcast channel coordinates shutdown across all workers:

```rust
use event_base::core::shutdown::shutdown_channel;

let (shutdown_tx, _) = shutdown_channel();
// shutdown_tx: broadcast::Sender<()>
```

The sender is returned by `start_queue_system!` and can be used to initiate shutdown.

---

## The 7 strategies

All strategies are defined in `ShutdownStrategy`:

```rust
pub enum ShutdownStrategy {
    TwoStage { poll_interval_ms: u64, force_timeout_secs: u64 },
    Graceful { worker_name: String, poll_interval_ms: u64 },
    Force,
    Timeout { total_timeout_secs: u64 },
    StateBasedIdle,
    Batched { batch_size: usize, interval_ms: u64 },
}
```

### 1. TwoStage — `shutdown_all_workers_two_stage`

The most robust strategy. Sends a shutdown signal, waits for all workers to finish within a timeout, then force-removes any survivors.

```rust
use event_base::core::shutdown::methods::shutdown_all_workers_two_stage;
use std::time::Duration;

shutdown_all_workers_two_stage(
    shutdown_tx,
    Duration::from_secs(30),     // force timeout
    Duration::from_millis(100),   // poll interval
).await?;
```

| Parameter | Description |
|---|---|
| `shutdown_tx` | Broadcast sender to signal all workers |
| `timeout` | Max total time before forcing termination |
| `poll_interval` | How often to check if workers are done |

**Flow**: Signal → poll → all done? → done. If timeout → force-remove remaining.

### 2. Force — `shutdown_force`

Immediately stops all workers without waiting. Active messages may be lost.

```rust
use event_base::core::shutdown::methods::shutdown_force;

shutdown_force().await;
```

**Use case**: Emergency shutdown, testing, or when you don't care about in-flight messages.

### 3. Timeout — `shutdown_timeout`

Waits a fixed duration, then force-shuts down all workers.

```rust
use event_base::core::shutdown::methods::shutdown_timeout;
use std::time::Duration;

shutdown_timeout(Duration::from_secs(10)).await;
```

**Use case**: Give workers a grace period, then force-stop.

### 4. Graceful — `graceful_shutdown`

Shuts down a **single worker** by name, waiting until it becomes idle.

```rust
use event_base::core::shutdown::methods::graceful_shutdown;
use std::time::Duration;

graceful_shutdown("worker-orders-abc123", Duration::from_millis(50)).await?;
```

| Parameter | Description |
|---|---|
| `worker_id` | Name of the worker to shut down |
| `poll_interval` | How often to poll the worker's status |

**Use case**: Selective worker removal (e.g., for updates or scaling down).

### 5. StateBasedIdle — `shutdown_idle_only`

Shuts down only workers that are currently idle (not processing a message).

```rust
use event_base::core::shutdown::methods::shutdown_idle_only;

shutdown_idle_only().await;
```

**Use case**: Gradual scale-down without interrupting active work.

### 6. Batched — `shutdown_batched`

Shuts down workers in batches with a delay between batches.

```rust
use event_base::core::shutdown::methods::shutdown_batched;
use std::time::Duration;

shutdown_batched(5, Duration::from_millis(200)).await;
```

| Parameter | Description |
|---|---|
| `batch_size` | Workers to stop per batch |
| `interval` | Delay between batches |

**Use case**: Controlled shutdown to avoid resource spikes.

### 7. Timeout (from ShutdownCommand)

The `ShutdownCommand` message can carry any strategy and be sent over the system bus. This is how the gRPC management API triggers shutdowns.

```rust
use event_base::core::shutdown::messages::{ShutdownCommand, ShutdownStrategy};

let cmd = ShutdownCommand {
    strategy: ShutdownStrategy::TwoStage {
        poll_interval_ms: 100,
        force_timeout_secs: 30,
    },
};
```

---

## Signal handling

Connect OS signals to trigger shutdown:

```rust
use tokio::signal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ... system setup ...

    tokio::select! {
        _ = signal::ctrl_c() => {
            println!("Received SIGINT, shutting down...");
        }
        _ = signal::unix::signal(signal::unix::SignalKind::terminate()) => {
            println!("Received SIGTERM, shutting down...");
        }
    }

    shutdown_all_workers_two_stage(
        shutdown_tx,
        Duration::from_secs(30),
        Duration::from_millis(100),
    ).await?;

    Ok(())
}
```

### Shutdown acknowledgment

Workers send a `ShutdownAck` to the `_system.shutdown_ack` topic after completing shutdown:

```rust
pub struct ShutdownAck {
    pub worker_name: String,
    pub status: ShutdownStatus,  // Completed, Failed, Timeout
    pub timestamp: SystemTime,
    pub error: Option<String>,
}
```

The `ShutdownAckHandler` (system handler) processes these acknowledgments on the Host node.

---

## Worker shutdown internals

When a worker receives a shutdown notification:

1. The worker's `shutdown()` method is called with `check_interval` and `timeout`.
2. If `check_interval > 0`, the worker polls until idle or until timeout.
3. The worker sets `shutdown_complete = true`.
4. The `ConsumerRouter` deletes the worker from its registry.

```rust
// Worker::shutdown (conceptual)
pub async fn shutdown(&self, check_interval: Duration, timeout: Option<Duration>) {
    self.shutdown_notify.notify_one();
    // If timeout, wait up to `timeout` for idle
    // Mark shutdown_complete
}
```

---

## Choosing a strategy

| Scenario | Recommended Strategy |
|---|---|
| Production graceful shutdown | `TwoStage` (30s timeout, 100ms poll) |
| Emergency stop | `Force` |
| Rolling update (one worker at a time) | `Graceful` |
| Scale down during low load | `StateBasedIdle` |
| Controlled drain | `Batched` (5 workers, 200ms delay) |
| Give workers a fixed grace period | `Timeout` (10s) |

---

## Next steps

- [Distributed Mode](distributed.md) — Host/Worker node model and discovery
- [Architecture](../internals/architecture.md) — Module structure and data flow
