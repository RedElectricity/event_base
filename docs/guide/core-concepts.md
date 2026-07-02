# Core Concepts

This document explains the fundamental abstractions of `event_base`: the message envelope, handlers, acknowledgments, and how messages flow through the system.

---

## EMessage

`EMessage` is the universal message envelope. Every piece of data flowing through the system is wrapped in an `EMessage`.

```rust
pub struct EMessage {
    pub id: String,             // Unique UUID
    pub topic: MessageTopic,    // Topic routing key
    pub payload: MessagePayload, // Raw byte payload
    pub metadata: MessageMetadata,
    pub attempts: u32,          // Processing attempt count
    pub delivery_mode: DeliveryMode,
    pub consumed_count: u32,
    pub deliver_at: Option<SystemTime>,
    pub to_worker: Option<String>,
    pub version: u32,
}
```

### Fields

| Field | Type | Description |
|---|---|---|
| `id` | `String` | Auto-generated UUID v4 |
| `topic` | `MessageTopic` | Routing key (wrapper around `String`) |
| `payload` | `MessagePayload` | Opaque byte payload (`Vec<u8>`) |
| `metadata` | `MessageMetadata` | Timestamps, trace IDs, correlation IDs |
| `attempts` | `u32` | Number of processing attempts so far |
| `delivery_mode` | `DeliveryMode` | Standard, Broadcast, or Repeated(N) |
| `consumed_count` | `u32` | Times consumed (for Repeated mode) |
| `deliver_at` | `Option<SystemTime>` | Scheduled delivery timestamp |
| `to_worker` | `Option<String>` | Target specific worker by name |
| `version` | `u32` | Schema version number |

### MessageMetadata

```rust
pub struct MessageMetadata {
    pub created_at: SystemTime,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub source: Option<String>,
}
```

### DeliveryMode

```rust
pub enum DeliveryMode {
    Standard,          // Competing consumers — one worker processes it
    Repeated(u32),     // Processed exactly N times (by any workers)
    Broadcast,         // Every worker subscribed to the topic processes it
}
```

### Construction

```rust
use event_base::prelude::*;

let msg = EMessage::new(
    "order.created",                // topic
    b"serialized-data".to_vec(),    // payload
    DeliveryMode::Standard,         // delivery mode
    None,                           // to_worker (None = any worker)
);
```

---

## Handler (EHandler)

A handler is an async function that receives an `&EMessage` and returns an `Ack`.

### The trait

```rust
#[async_trait]
pub trait EHandler: Send + Sync {
    async fn handler(&self, msg: &EMessage) -> Ack;
}
```

### The `#[handler]` macro

In practice you rarely implement `EHandler` manually. Use the attribute macro:

```rust
#[handler(topic = "order", workers = 4)]
async fn handle_order(msg: &EMessage) -> Ack {
    // ... business logic ...
    Ack::Ack
}
```

This generates a handler struct, the `EHandler` impl, and registers everything at compile time. See the [Handler guide](handler.md) for details.

---

## Ack

`Ack` is the return type of every handler. It tells the system what to do with the message after processing.

```rust
pub enum Ack {
    /// Success — message is acknowledged and marked Complete in the WAL.
    Ack,

    /// Transient failure — retry later with optional backoff.
    NoAck {
        retry_after: Option<Duration>,
        max_retries: u32,
    },

    /// Fatal failure — move to the Dead Letter Queue immediately.
    Dead {
        dead_reason: DeadReason,
    },
}
```

### Ack variants

| Variant | Meaning | WAL state | Next action |
|---|---|---|---|
| `Ack` | Processed successfully | `Complete` | Removed from queue |
| `NoAck` | Temporary error, retry | `Pending` | Re-queued after delay |
| `Dead` | Unrecoverable error | `Failed` | Moved to DLQ |

### DeadReason

```rust
pub enum DeadReason {
    MaxRetriesExceeded,  // Exhausted retry attempts
    Explicit,            // Handler explicitly returned Dead
    Timeout,             // Handler exceeded its timeout
    NoHandler,           // No handler registered for the topic
    Other(String),       // Arbitrary error description
}
```

---

## TopicRouter

The `TopicRouter` is the **entry point** for all messages. It is a global singleton initialized during system startup.

Responsibilities:

- Accept messages via `send()`, `try_send()`, `send_timeout()`
- Append messages to the WAL before enqueuing
- Handle **delayed delivery** (`deliver_at`) by scheduling in the WAL
- Handle **Broadcast** delivery by fanning out to all registered workers
- **Replay** pending WAL records after a crash recovery

```rust
// Send with optional try-send and timeout
TopicRouter::global()
    .send("orders", msg, Some(true), None)
    .await?;
```

---

## ConsumerRouter

The `ConsumerRouter` is the **dispatch engine**. It runs in its own Tokio task, continuously claiming messages from the main consumer and routing them to workers.

Responsibilities:

- Claim messages from the main consumer (in batches)
- Select the target worker (by `to_worker` or by idle-worker selection)
- Forward the message to the worker's internal producer
- Acknowledge (ack) or negatively acknowledge (nack) claims (batched)
- Manage worker lifecycle (create, register, shutdown, delete)

### Batch claim dispatch

Messages are claimed in batches (up to `batch_size`, default 64) to amortise lock contention. The dispatch loop:

1. **Claim**: Lock the main consumer once → claim up to `batch_size` messages → unlock
2. **Dispatch**: For each message, select a worker and forward (no consumer lock held)
3. **Ack/nack**: Lock the main consumer once → ack/nack the entire batch → unlock

```rust
ConsumerRouter::init(consumer, factory, None)?;         // default batch_size = 64
ConsumerRouter::init(consumer, factory, Some(128))?;    // custom
```

### Worker selection logic

```
1. If msg.to_worker is set → deliver to that specific worker by name
2. Otherwise → select the first idle worker subscribed to the message's topic
3. If no idle worker is available → nack the message (it will be re-queued)
```

### Idle-worker lifecycle

Workers register themselves as idle/working via `ConsumerRouter::set_idle()` / `set_working()`:

```
Worker starts        → set_idle(worker_name)   → added to idle list
Worker receives msg  → set_working(worker_name) → removed from idle list
Worker finishes      → set_idle(worker_name)   → added back to idle list
```

---

## Message flow

Below is the complete path a message takes through the system:

```
┌──────────┐
│  Sender  │  send_msg!("orders", msg)
└────┬─────┘
     │
     ▼
┌──────────────┐
│ TopicRouter  │  1. Appends msg to WAL (Pending state)
│              │  2. Pushes msg to the queue via EProducer
└──────┬───────┘
       │
       ▼
┌──────────────────┐
│  Main Consumer   │  3. ConsumerRouter.recv() claims up to
│  (dispatch loop) │     batch_size messages at once (claim_batch)
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│ ConsumerRouter   │  4. For each msg in batch: selects a worker
│                  │     for the topic & forwards to worker's
│                  │     internal producer
│                  │  5. Batch-acks all successfully dispatched
│                  │     claims (single consumer-lock)
└──────┬───────────┘
       │
       ▼
┌──────────────┐
│   Worker     │  7. Worker receives msg from its internal consumer
│              │  8. Sets status to Working
│              │  9. Runs the middleware pipeline
│              │ 10. Runs the final handler
│              │ 11. WAL: marks as Processing → Complete (or Failed)
│              │ 12. Sends audit event to _system.audit
│              │ 13. Sets status back to Idle
└──────────────┘
       │
       ▼
┌──────────────┐
│    Ack       │  Returned to the system; retry or DLQ if needed
└──────────────┘
```

### Key design points

- **WAL always first**: Messages are persisted *before* they are enqueued. This guarantees that a crash between enqueue and processing does not lose the message.
- **Batch claim dispatch**: The main consumer claims up to `batch_size` messages in a single lock acquisition. The default batch size is 64, configurable via `ConsumerRouter::init()`. Each queue backend can override `EConsumer::claim_batch()` for an optimized single-lock implementation.
- **Idle-worker selection**: Messages are load-balanced across workers of the same topic. Workers notify the router via `set_idle()`/`set_working()` when they become available or busy. If all workers are busy, the message is nacked and stays in the queue.
- **System message template caching**: `Worker` and `WalClient` cache `EMessage` templates for `_system.audit` and `_system.wal_sync` topics to avoid re-creating fixed fields (topic, delivery mode) on every audit / WAL sync send.

---

## Node types

```rust
pub enum NodeType {
    Host,   // Coordinator: manages WAL, system topics, delay scheduler
    Worker, // Message processor: consumes and handles messages
}
```

- **Host** nodes run system handlers (audit, trace, shutdown coordination, worker discovery) and the delay scheduler for timed messages.
- **Worker** nodes only process messages. They discover Host nodes via the `_system.worker_discovery` topic.

See the [Distributed mode](distributed.md) guide for details.

---

## Next steps

- [Handlers](handler.md) — Parameter reference and Ack patterns
- [Middleware](middleware.md) — Build and compose middleware pipelines
- [Sending Messages](sending.md) — Delivery modes and API
