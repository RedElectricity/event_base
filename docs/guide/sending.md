# Sending Messages

This guide covers how to send messages to handlers, the three delivery modes, and targeted delivery to specific workers.

---

## Building a message

Every message is an `EMessage`. Its topic and payload are newtypes:

```rust
use event_base::prelude::*;

let msg = EMessage::new(
    MessageTopic("order.created".into()), // or `"order.created".into()`
    MessagePayload(b"order-data".to_vec()),
    DeliveryMode::Standard,
    None, // optional `to_worker`
);
```

`MessageTopic` implements `From<&str>` / `From<String>`, so `"order.created".into()` works anywhere the type is expected. The fourth argument is the target worker name (`None` = let the router pick).

## The `send_msg!` macro

`send_msg!` is a thin wrapper around `TopicRouter::global().send()` that reads the topic **from the message** (there is no topic argument). It expands to an already‑awaited expression, so call sites must **not** add `.await`.

```rust
send_msg!(msg, None, None)?; // msg, try_send, timeout — all already moved into msg
```

### Signature

```rust
// Macro expansion (conceptual):
pub async fn send_msg_impl(
    msg: EMessage,
    try_send: Option<bool>,
    time_out: Option<Duration>,
) -> Result<(), CoreError>;
```

### Parameters

| Parameter | Type | Description |
|---|---|---|
| msg | `EMessage` | The envelope — its `topic` field is the destination |
| try_send | `Option<bool>` | `Some(true)` = non-blocking try-send (errors if the queue is full) |
| time_out | `Option<Duration>` | Max time to wait for the send; `None` = block until space |

### With try_send and timeout

```rust
use std::time::Duration;

// Non-blocking send — returns an error immediately if the queue is full.
send_msg!(msg, Some(true), None)?;

// Send with a 100 ms bound (surfaces backpressure as an error).
send_msg!(msg, None, Some(Duration::from_millis(100)))?;
```

> A whole node can opt into a default send bound once at boot via `TopicRouter::set_default_send_timeout`; then any `send_msg!(msg, None, None)` that supplies neither `try_send` nor a timeout is bounded by it instead of blocking forever.

### Error handling

```rust
match send_msg!(msg, None, None) {
    Ok(()) => println!("Message sent"),
    Err(e) => eprintln!("Send failed: {e}"),
}
```

The most common error is `CoreError::Queue(QueueError::Full)` when using `try_send` against a full queue.

---

## Delivery modes

The `DeliveryMode` on each `EMessage` determines how it is delivered:

```rust
pub enum DeliveryMode {
    Standard,       // One worker processes it (competing consumers)
    Repeated(u32),  // Exactly N workers process it
    Broadcast,      // All workers on the topic process it
}
```

### Standard (default)

The message is delivered to **one** worker subscribed to the topic. If multiple workers exist, one is selected (idle-worker round-robin).

```rust
let msg = EMessage::new("task".into(), data, DeliveryMode::Standard, None);
```

**Use case**: Competing consumers — scale processing by adding more workers.

### Repeated(N)

The message is delivered exactly **N times**, potentially to different workers. The `consumed_count` field tracks how many times it has been consumed.

```rust
// Processed by exactly 3 workers.
let msg = EMessage::new("notification".into(), data, DeliveryMode::Repeated(3), None);
```

**Use case**: Fan-out to a fixed number of processors (e.g., send to 3 validation services).

### Broadcast

The message is delivered to **every** worker currently subscribed to the topic.

```rust
let msg = EMessage::new("system.event".into(), data, DeliveryMode::Broadcast, None);
```

On a `Host` node, the `TopicRouter` resolves all workers for the topic and sends to each one. If no workers exist, the message is dropped. A `Worker` node may not send a broadcast (it returns `CoreError::Unsupported`).

**Use case**: Cache invalidation, configuration updates, system-wide notifications.

---

## Targeted delivery: `to_worker`

You can route a message to a **specific worker** by name (the fourth `EMessage::new` argument):

```rust
let msg = EMessage::new(
    "private".into(),
    data,
    DeliveryMode::Standard,
    Some("orders-host@worker-orders-abc123".into()), // to_worker
);
```

The `ConsumerRouter` checks `to_worker` during dispatch. If the worker exists, it receives the message directly; otherwise the message is nacked and re-routed.

> Worker names are node-qualified — `{node}@worker-{topic}-{uuid}` — so they stay unique across a fleet. Use `WorkerRegistry` or `ebctl workers` to discover active names.

---

## Direct API without macros

You can drive the `TopicRouter` yourself (note the async `RwLock` — take a read guard first):

```rust
use event_base::core::topic::TopicRouter;
use std::time::Duration;

// Standard send.
TopicRouter::global().read().await.send("orders", msg.clone(), None, None).await?;

// Try send (non-blocking).
TopicRouter::global().read().await.send("orders", msg.clone(), Some(true), None).await?;

// Send with a timeout.
TopicRouter::global()
    .read()
    .await
    .send("orders", msg, None, Some(Duration::from_secs(1)))
    .await?;
```

---

## What happens when you send a message

```text
send_msg!(msg, None, None)
    │
    ▼
TopicRouter::send()
    │
    ├── 1. If deliver_at is set → schedule in the WAL, return
    │
    └── 2. Push msg to the queue via EProducer (Standard or Repeated)
         OR fan-out to all workers (Broadcast)
              │
              ▼
         ConsumerRouter claims → dispatches to a worker
```

`TopicRouter::send` does **not** append to the WAL itself — durability is the caller's responsibility (typically the `Worker`/`ConsumerRouter` records state via the WAL‑sync path). See [Persistence & WAL](persistence.md).

---

## Scheduled (delayed) delivery

Set `deliver_at` on the message to delay delivery:

```rust
use std::time::{Duration, SystemTime};

let mut msg = EMessage::new("reminder".into(), data, DeliveryMode::Standard, None);
msg.deliver_at = Some(SystemTime::now() + Duration::from_secs(3600)); // +1 hour

send_msg!(msg, None, None)?;
```

The message is stored in the WAL's scheduled record store. On `Host` nodes, a delay scheduler (started by `Bootstrap` / `start_system!`) periodically checks for ready messages and delivers them.

---

## Best practices

1. **Prefer `send_msg!`** — it's concise and reads the topic from the message.
2. **Use `try_send` for high-throughput paths** — avoids blocking when the queue is saturated.
3. **Use a `send_timeout`** (per call, or a node-wide default) — prevents an indefinite block on a full queue.
4. **Choose the right delivery mode** — Standard for load balancing, Broadcast for fan-out, Repeated for exactly‑N.
5. **Send only to registered topics** — the in‑process `crossfire` backend rejects an unknown topic, so the message must route to a handler‑created queue.

---

## Next steps

- [Persistence & WAL](persistence.md) — How messages survive crashes
- [Shutdown Strategies](shutdown.md) — Graceful and forceful shutdown
- [gRPC Control Plane & ebctl](grpc.md) — Publish from the shell with `ebctl publish`
