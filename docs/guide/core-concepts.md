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

- Claim messages from the main consumer
- Select the target worker (by `to_worker` or by idle-worker selection)
- Forward the message to the worker's internal producer
- Acknowledge (ack) or negatively acknowledge (nack) the claim
- Manage worker lifecycle (create, register, shutdown, delete)

### Worker selection logic

```
1. If msg.to_worker is set → deliver to that specific worker by name
2. Otherwise → select the first idle worker subscribed to the message's topic
3. If no idle worker is available → nack the message (it will be re-queued)
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
│  Main Consumer   │  3. ConsumerRouter.recv() claims the message
│  (dispatch loop) │
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│ ConsumerRouter   │  4. Selects a worker for the topic
│                  │  5. Forwards to worker's internal producer
│                  │  6. Acks the claim (removes from main queue)
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
- **Claim-based dispatch**: The main consumer claims a message; if the worker fails before acking the claim, the message becomes available again (depends on queue implementation).
- **Idle-worker selection**: Messages are load-balanced across workers of the same topic. If all workers are busy, the message is nacked and stays in the queue.

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
