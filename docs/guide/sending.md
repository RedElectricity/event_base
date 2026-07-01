# Sending Messages

This guide covers how to send messages to handlers, the three delivery modes, and targeted delivery to specific workers.

---

## The `send_msg!` macro

`send_msg!` is the primary way to send a message. It is a thin wrapper around `TopicRouter::global().send()`.

```rust
use event_base::prelude::*;

let msg = EMessage::new(
    "order.created",
    b"order-data".to_vec(),
    DeliveryMode::Standard,
    None,
);

send_msg!("order.created", msg).await?;
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
| topic | `&str` | Topic to send to (must match a registered handler's topic) |
| msg | `EMessage` | The message envelope |
| (implicit `try_send`) | `Option<bool>` | `Some(true)` = non-blocking try-send |
| (implicit `time_out`) | `Option<Duration>` | Max time to wait for the send |

### With try_send and timeout

```rust
use std::time::Duration;

// Non-blocking send — returns error immediately if queue is full
send_msg!("orders", msg, Some(true), None).await?;

// Send with 100ms timeout
send_msg!("orders", msg, None, Some(Duration::from_millis(100))).await?;

// Both
send_msg!("orders", msg, Some(true), Some(Duration::from_millis(50))).await?;
```

### Error handling

```rust
match send_msg!("orders", msg).await {
    Ok(()) => println!("Message sent"),
    Err(e) => eprintln!("Send failed: {}", e),
}
```

The most common error is `CoreError::Queue(QueueError::Full)` when using `try_send` with a full queue.

---

## Delivery modes

The `DeliveryMode` enum on each `EMessage` determines how the message is delivered:

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
let msg = EMessage::new("task", data, DeliveryMode::Standard, None);
```

**Use case**: Competing consumers — scale processing by adding more workers.

### Repeated(N)

The message is delivered exactly **N times**, potentially to different workers. The `consumed_count` field tracks how many times it has been consumed.

```rust
// This message will be processed by exactly 3 workers
let msg = EMessage::new("notification", data, DeliveryMode::Repeated(3), None);
```

**Use case**: Fan-out to a fixed number of processors (e.g., send to 3 validation services).

### Broadcast

The message is delivered to **every** worker currently subscribed to the topic.

```rust
let msg = EMessage::new("system.event", data, DeliveryMode::Broadcast, None);
```

On a `Host` node, the `TopicRouter` resolves all workers for the topic and sends to each one. If no workers exist, the message is dropped.

**Use case**: Cache invalidation, configuration updates, system-wide notifications.

---

## Targeted delivery: `to_worker`

You can route a message to a **specific worker** by name:

```rust
let msg = EMessage::new(
    "private",
    data,
    DeliveryMode::Standard,
    Some("worker-orders-abc123".into()), // to_worker
);
```

The `ConsumerRouter` checks the `to_worker` field during dispatch. If the worker exists, it receives the message directly. If not, the message is nacked.

> Worker names are auto-generated as `worker-{topic}-{uuid}`. Use the `WorkerRegistry` to discover active worker names.

---

## Direct API without macros

You can also use the `TopicRouter` directly:

```rust
use event_base::core::topic::TopicRouter;
use std::time::Duration;

// Standard send
TopicRouter::global().send("orders", msg, None, None).await?;

// Try send (non-blocking)
TopicRouter::global().send("orders", msg, Some(true), None).await?;

// Send with timeout
TopicRouter::global()
    .send("orders", msg, None, Some(Duration::from_secs(1)))
    .await?;
```

---

## What happens when you send a message

```text
send_msg!("orders", msg)
    │
    ▼
TopicRouter::send()
    │
    ├── 1. Append msg to WAL (state: Pending)
    │
    ├── 2. If deliver_at is set → schedule in WAL, return
    │
    └── 3. Push msg to the queue via EProducer (Standard or Repeated)
         OR fan-out to all workers (Broadcast)
              │
              ▼
         ConsumerRouter claims → dispatches to worker
```

---

## Scheduled (delayed) delivery

Set `deliver_at` on the message to delay delivery:

```rust
use std::time::{SystemTime, Duration};

let mut msg = EMessage::new("reminder", data, DeliveryMode::Standard, None);
msg.deliver_at = Some(SystemTime::now() + Duration::from_secs(3600)); // +1 hour

send_msg!("reminder", msg).await?;
```

The message is stored in the WAL's scheduled record store. On `Host` nodes, a delay scheduler periodically checks for ready messages and delivers them.

---

## Best practices

1. **Prefer `send_msg!`** — it's concise and handles topic extraction automatically.
2. **Use `try_send` for high-throughput paths** — avoids blocking when the queue is saturated.
3. **Use `send_timeout` for bounded waits** — prevents indefinite blocking.
4. **Choose the right delivery mode** — Standard for load balancing, Broadcast for fan-out, Repeated for exact N processing.
5. **Validate the topic exists** — sending to an unregistered topic will result in a nack.

---

## Next steps

- [Persistence & WAL](persistence.md) — How messages survive crashes
- [Shutdown Strategies](shutdown.md) — Graceful and forceful shutdown
