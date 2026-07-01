# Distributed Mode

`event_base` supports a distributed node model with two roles: **Host** and **Worker**. Nodes communicate via system topics to discover each other, sync topics, and coordinate shutdown.

---

## Node roles

```rust
pub enum NodeType {
    Host,   // Coordinator node
    Worker, // Processing node
}
```

### Host

The Host node is the **coordinator**. Responsibilities:

- Runs the **WAL** (single source of truth for message states)
- Runs **system handlers** (audit, trace, shutdown coordination, metrics)
- Runs the **delay scheduler** (delivers messages with `deliver_at` set)
- Responds to **worker discovery** requests
- Manages **topic synchronization**
- Handles **shutdown coordination** (collects `ShutdownAck` from workers)

Only **one Host** should be active at a time (singleton coordinator).

### Worker

Worker nodes are the **processors**. Responsibilities:

- Subscribe to topics and process messages
- Send **heartbeats** to the Host
- Report **WAL state changes** to the Host
- Send **shutdown acknowledgments** when stopping

Multiple workers can run simultaneously, potentially on different machines.

---

## System topics

System topics (prefixed with `_system.`) are reserved for internal communication:

| Topic | Direction | Purpose |
|---|---|---|
| `_system.audit` | Worker → Host | Audit log events |
| `_system.trace` | Worker → Host | Distributed tracing spans |
| `_system.shutdown` | Host → Worker | Shutdown commands |
| `_system.shutdown_ack` | Worker → Host | Shutdown acknowledgments |
| `_system.wal_sync` | Worker → Host | WAL state sync (Processing → Complete) |
| `_system.worker_discovery` | Worker → Host | Worker registration |
| `_system.worker_heartbeat` | Worker → Host | Periodic heartbeat |
| `_system.metrics` | Worker → Host | Node metrics |
| `_system.topic_discovery` | Worker → Host | Topic list sync |
| `_system.topic_sync` | Host → Worker | Topic configuration sync |

---

## Worker discovery

When a Worker node starts, it sends a `WorkerDiscoveryMessage` to the `_system.worker_discovery` topic:

```rust
pub struct WorkerDiscoveryMessage {
    pub worker_name: String,
    pub topic: String,
    pub started_at: SystemTime,
}
```

The Host's `WorkerDiscoveryHandler` processes this message:

1. Records the worker in the `WorkerRegistry`
2. Persists the registry to the WAL
3. The worker is now addressable for message delivery

### Heartbeats

Workers periodically send heartbeats to the `_system.worker_heartbeat` topic:

```rust
pub struct WorkerHeartbeatMessage {
    pub worker_name: String,
    pub timestamp: SystemTime,
}
```

The Host updates the worker's `last_heartbeat` timestamp. Stale workers (heartbeat too old) can be detected and cleaned up.

```rust
// WorkerRegistry: cleanup stale workers
pub async fn cleanup_stale_workers(&self, heartbeat_timeout: Duration) -> Result<Vec<String>, CoreError>;
```

---

## Topic synchronization

### Topic discovery

When a Worker starts, the `start_system!` macro sends a `TopicDiscoveryMessage` to `_system.topic_discovery`:

```rust
pub struct TopicDiscoveryMessage {
    pub has_topics: Vec<String>,  // Topics this Worker knows about
}
```

### Topic sync

The Host processes topic discovery messages and can push configuration updates back to workers via `_system.topic_sync`. This ensures all nodes agree on the active topic set.

---

## Configuration

### Starting a Host node

```rust
use event_base::prelude::*;
use event_base::flume::MemoryQueueFactory;
use event_base::memory_wal::MemoryWal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    set_node_name("host-1".to_string());

    start_queue_system! {
        factory: MemoryQueueFactory::new(1000),
        wal: Some(MemoryWal::new()),
    }

    // Host is running — handles system topics and delay scheduler
    tokio::signal::ctrl_c().await?;
    Ok(())
}
```

### Starting a Worker node

```rust
use event_base::prelude::*;
use event_base::flume::MemoryQueueFactory;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    set_node_name("worker-1".to_string());

    start_queue_system! {
        factory: MemoryQueueFactory::new(1000),
        wal: Some(MemoryWal::new()),
    }

    // Worker is running — will discover Host and start processing
    tokio::signal::ctrl_c().await?;
    Ok(())
}
```

> In a distributed setup, nodes must share a queue backend (e.g., Redis, Kafka via custom `QueueFactory`). The built-in `MemoryQueueFactory` only works for single-process deployments.

---

## WorkerRegistry

The `WorkerRegistry` is a global singleton that tracks all active workers:

```rust
// Register a worker
WorkerRegistry::global()
    .register(WorkerInfo {
        worker_name: "worker-orders-abc".into(),
        topic: "orders".into(),
        last_heartbeat: SystemTime::now(),
    })
    .await?;

// Query workers for a topic
let workers = WorkerRegistry::global()
    .get_workers("orders")
    .await?;

// Get all workers
let all = WorkerRegistry::global()
    .get_all_workers()
    .await?;
```

The registry is persisted to the WAL, so worker information survives restarts.

---

## Distributed shutdown

In a distributed setup:

1. Shutdown command is sent to `_system.shutdown` (by gRPC API or programmatically)
2. All Workers receive the command via their system handler
3. Each Worker shuts down using the specified `ShutdownStrategy`
4. Each Worker sends a `ShutdownAck` back to `_system.shutdown_ack`
5. The Host's `ShutdownAckHandler` collects all acks and confirms shutdown is complete

---

## Best practices

1. **One Host per deployment** — Avoid split-brain scenarios.
2. **Use a shared queue backend** — Each node needs access to the same queue infrastructure.
3. **Set unique node names** — `set_node_name()` must produce unique identifiers.
4. **Monitor heartbeats** — Implement stale-worker cleanup for resilience.
5. **Enable persistent WAL** — The Host should use `PersistentWal` for crash recovery.

---

## Next steps

- [Architecture](../internals/architecture.md) — Module structure and crate dependencies
