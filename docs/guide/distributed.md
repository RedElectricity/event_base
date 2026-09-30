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

| Topic | Direction | Purpose | Delivery on shared backends |
|---|---|---|---|
| `_system.audit` | Worker → Host | Audit log events | competing (Host-only consumer) |
| `_system.trace` | Worker → Host | Distributed tracing spans | competing (Host-only consumer) |
| `_system.shutdown` | Host → every node | Shutdown commands | **fan-out** (per-node group) |
| `_system.shutdown_ack` | Worker → Host | Shutdown acknowledgments | competing (Host-only consumer) |
| `_system.wal_sync` | Worker → Host | WAL state sync (Processing → Complete) | competing (Host-only consumer) |
| `_system.worker_discovery` | every node → Host | Worker registration | competing (Host-only consumer) |
| `_system.worker_heartbeat` | every node → Host | Periodic heartbeat | competing (Host-only consumer) |
| `_system.metrics` | every node → everywhere | Node metrics | **fan-out** (each node stores all) |
| `_system.topic_discovery` | Worker → Host | Topic list sync | competing (Host-only consumer) |
| `_system.topic_sync` | Host → Worker | Topic configuration sync | **fan-out** (per-node group) |

---

## Worker discovery

Every worker created through `ConsumerRouter::create_worker` publishes a
`WorkerDiscoveryMessage` to `_system.worker_discovery`:

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
3. The worker is now addressable: broadcast fan-out, gRPC `list_workers` and stale cleanup all read from this registry

Announcements use `try_send` (worker creation never blocks on coordination chatter); every `DISCOVERY_REANNOUNCE_EVERY` heartbeat cycles the heartbeat loop re-announces any local worker missing from the registry, so a dropped announcement self-heals. Workers on `_system.*` topics are process plumbing, not business consumers, and are deliberately **not** announced; ephemeral one-shot workers never are either.

### Node identity in worker names

When `set_node_name` has been called, worker names are node-qualified: `{node}@worker-{topic}-{uuid}`. Qualified names are globally unique, which is what makes cross-node `to_worker` targeting meaningful — see [Targeted delivery](#targeted-delivery).

### Heartbeats

`start_system!` spawns a heartbeat loop on **every** node role. Each pass publishes one `WorkerHeartbeatMessage` per live (non-system) local worker, at `WORKER_HEARTBEAT_INTERVAL` (15 s):

```rust
pub struct WorkerHeartbeatMessage {
    pub worker_name: String,
    pub timestamp: SystemTime,
}
```

The Host refreshes each worker's `last_heartbeat`. A Host-only cleanup task sweeps the registry every `WORKER_CLEANUP_INTERVAL` (30 s) and evicts entries whose heartbeat is older than `WORKER_STALE_TIMEOUT` (90 s ≈ 6 missed beats), so workers on a dead node stop receiving targeted traffic. The constants live in `event_base_core::system_handlers::worker`, together with a manually-drivable `heartbeat_once(reannounce: bool)` used by tests.

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Host role: owns system topics, the delay scheduler, registry cleanup.
    let node = Bootstrap::host("host-1").start().await?;
    node.wait().await;
    Ok(())
}
```

### Starting a Worker node

```rust
use event_base::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Worker role: registers its handlers, heartbeats to the Host, processes.
    let node = Bootstrap::worker("worker-1").start().await?;
    node.wait().await;
    Ok(())
}
```

> In a distributed setup, nodes must share a queue backend. The default in‑memory backend only works for a single process — for multiple processes enable the `redis` feature and point every node at the same Redis. `Bootstrap` reads the backend from `eb.toml`, so the whole topology is config, not code:

```rust
use event_base::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = Bootstrap::from_file("eb.toml")?.start().await?;
    node.wait().await;
    Ok(())
}
```

```toml
# eb.toml — build event_base with --features redis for this to boot
[node]
name = "host-1"
role = "host"

[queue]
backend = "redis"
url = "redis://127.0.0.1:6379"
prefix = "my-cluster"

[wal]
backend = "redis"      # reuses queue.url when wal.url is empty
```

Every node uses a distinct `name` (it becomes the consumer-group / node identity on the shared streams); business topics compete across nodes while `_system.*` coordination topics fan out per node.

Every topic becomes a Redis stream (`{prefix}:{topic}`). Business topics share
one consumer group, so workers on different machines **compete** for messages;
the fan-out coordination topics in the table above get a per-node group
(`{group}@{node}`, identified by `set_node_name` or
`RedisQueueConfig::with_node`), so **every** node receives its own copy of
shutdown commands, topic sync and metrics. Fan-out groups are created at `$`
— a node that joins later never replays stale control traffic; all other
groups start at `0`, preserving pre-boot buffered messages. `unregistered
_system.*` topics are still published (their consumers live on other nodes),
so set `RedisQueueConfig::maxlen` on long-lived deployments. Use a node-unique
WAL `prefix` (`RedisWal::with_prefix`) unless you deliberately want one shared
write-ahead log across nodes. Kafka and friends remain possible via a custom
`QueueFactory`/`Wal` implementation.

---

## Targeted delivery

Messages may carry `to_worker = Some(name)` (set explicitly, or stamped by the
broadcast fan-out from the registry). On shared backends each topic has one
dispatcher per node; its rules are:

* the target matches a local worker (qualified name, or bare name resolving to
  `{this-node}@{name}`) → deliver to that worker's inbox;
* the target names a worker that is **not** local (another node's, or a dead
  one) → ack, bump `attempts`, and re-inject on the topic stream so the
  owning node gets a fresh claim. This claim-and-requeue is the cross-node
  router for targeted traffic; after `MAX_TARGETED_HOPS` (64) relays the
  message is dropped with a warning, so a dead target can never wedge the
  queue;
* no target → round-robin across the node's workers (the selected name is
  stamped into `to_worker`).

The practical rule: **address workers by what `list_workers` / the
`WorkerRegistry` reports** (node-qualified names), not by reconstructed
strings.

---

## Securing the control plane

The gRPC surface (`event_base_grpc::serve`) exposes topic/worker/metrics state
and — critically — the `shutdown` RPC. Pick by exposure:

```rust
// loopback / single machine only — NO authentication:
event_base_grpc::serve(addr).await?;

// require `authorization: Bearer <token>` on every RPC (constant-time check):
event_base_grpc::serve_with_token(addr, "sekret").await?;

// TLS (cargo feature `tls` on event_base_grpc, or `grpc-tls` on the umbrella
// crate), optionally combined with the token inside the encrypted channel:
event_base_grpc::serve_tls(addr, cert_pem, key_pem, Some("sekret".into())).await?;
```

`serve_with_token`/`serve_tls` return `tonic::transport::Error` (a `Send`
error) so they can be `tokio::spawn`ed directly; `serve` keeps its historical
`Box<dyn Error>` signature.

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
