# Handlers

Handlers are the core processing unit in `event_base`. They receive messages and return acknowledgments.

---

## The `#[handler]` attribute macro

The `#[handler]` attribute transforms an async function into a fully registered message handler. It generates:

1. A handler struct (`{FunctionName}Handler`)
2. An `EHandler` trait implementation that delegates to your function
3. A static entry in the `HANDLER_REGISTRY` distributed slice (via `linkme`)
4. A registration function that creates the pipeline and workers

### Minimal example

```rust
use event_base::prelude::*;

#[handler(topic = "user.signup")]
async fn handle_signup(msg: &EMessage) -> Ack {
    tracing::info!("Signup: {:?}", msg);
    Ack::Ack
}
```

### Full parameter example

```rust
#[handler(
    topic = "order.created",
    workers = 4,
    timeout = 30,
    shutdown_timeout = 10,
    shutdown_check_interval = 100,
    middleware = [LoggerMiddleware]
)]
async fn handle_order(msg: &EMessage) -> Ack {
    // Process the order...
    Ack::Ack
}
```

---

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `topic` | `&str` | **(required)** | Topic string this handler subscribes to |
| `workers` | `usize` | `1` | Number of concurrent worker tasks |
| `timeout` | `u64` | `None` | Max processing time per message (seconds) |
| `shutdown_timeout` | `u64` | `None` | Max wait for graceful shutdown (seconds) |
| `shutdown_check_interval` | `u64` | `50` | Polling interval for idle check (milliseconds) |
| `middleware` | `[...]` | `None` | Array of middleware types to apply |

### `topic` (required)

The topic name used for routing. Messages sent to this topic will be delivered to this handler.

> System topics starting with `_system.` are reserved. Do not use them for user handlers.

### `workers`

Controls the degree of concurrency. More workers means more messages processed in parallel, but also more resource consumption.

```rust
#[handler(topic = "email.send", workers = 10)]
async fn handle_email(msg: &EMessage) -> Ack {
    // 10 concurrent email sends
    send_email(msg).await;
    Ack::Ack
}
```

### `timeout`

If a handler takes longer than `timeout` seconds, the message is automatically dead-lettered with `DeadReason::Timeout`.

```rust
#[handler(topic = "payment.process", timeout = 5)]
async fn handle_payment(msg: &EMessage) -> Ack {
    // Must complete within 5 seconds
    process_payment(msg).await;
    Ack::Ack
}
```

### `shutdown_timeout`

How long the system waits for this worker to finish its current message during shutdown. After this, the worker is force-stopped.

### `shutdown_check_interval`

How often (in milliseconds) the system checks whether the worker has become idle during graceful shutdown.

### `middleware`

An array of middleware types applied before the handler. See the [Middleware guide](middleware.md).

---

## Return values: Ack

Every handler must return an `Ack` variant:

### `Ack::Ack`

The message was processed successfully. The WAL marks it as `Complete`.

```rust
#[handler(topic = "data.ingest")]
async fn handle_data(msg: &EMessage) -> Ack {
    store_in_database(msg).await;
    Ack::Ack  // ✅ Success
}
```

### `Ack::NoAck { retry_after, max_retries }`

The message could not be processed but may succeed later. It will be retried.

```rust
#[handler(topic = "api.call")]
async fn handle_api_call(msg: &EMessage) -> Ack {
    match call_external_api(msg).await {
        Ok(_) => Ack::Ack,
        Err(err) if err.is_retryable() => Ack::NoAck {
            retry_after: Some(Duration::from_secs(5)),  // Wait 5s before retry
            max_retries: 3,                              // Max 3 retry attempts
        },
        Err(_) => Ack::Dead { dead_reason: DeadReason::Explicit },
    }
}
```

| Field | Type | Description |
|---|---|---|
| `retry_after` | `Option<Duration>` | Delay before next retry. `None` = use default backoff. |
| `max_retries` | `u32` | Max retry attempts before automatic dead-lettering. |

### `Ack::Dead { dead_reason }`

The message cannot be processed and is moved to the Dead Letter Queue.

```rust
#[handler(topic = "validation")]
async fn handle_validation(msg: &EMessage) -> Ack {
    if is_valid(msg) {
        Ack::Ack
    } else {
        Ack::Dead {
            dead_reason: DeadReason::Other("Invalid payload format".into()),
        }
    }
}
```

| `DeadReason` | When to use |
|---|---|
| `MaxRetriesExceeded` | Automatic — returned by the system when retries exhausted |
| `Explicit` | Handler explicitly decides the message is poison |
| `Timeout` | Automatic — returned by the system on handler timeout |
| `NoHandler` | Automatic — no handler registered for the topic |
| `Other(String)` | Any custom reason |

### Where a dead letter actually goes

There is no dead-letter store to query. A message that dies is republished on the
topic `dead_letter.<topic>` — so a poison message on `validation` reappears on
`dead_letter.validation` — and the WAL record for it is marked `Failed` with your
`dead_reason` as the error, alongside a `DeadLetter` audit record carrying the same
reason.

That means **a dead letter is only seen if a handler is registered on the
`dead_letter.*` topic**. Without one, the message stays claimable on that queue and
nothing delivers it: the only trace left is the `Failed` WAL record:

```rust
#[handler(topic = "dead_letter.validation")]
async fn shelf_validation(msg: &EMessage) -> Ack {
    inspect_and_alert(msg).await;
    Ack::Ack // consume it here; the bus has no second copy
}
```

Register one shelf handler per topic you care about. `dead_letter.*` messages keep
the original payload, message id, and attempt count; the routing stamp that pinned
the failed message to a particular worker is cleared on the way out, so any worker
registered on the shelf topic may receive it.

---

## Manual EHandler implementation

You can also implement `EHandler` directly without the macro:

```rust
use async_trait::async_trait;
use event_base::prelude::*;

struct MyHandler;

#[async_trait]
impl EHandler for MyHandler {
    async fn handler(&self, msg: &EMessage) -> Ack {
        println!("Processing: {:?}", msg);
        Ack::Ack
    }
}
```

However, you must then manually register the handler and create workers. The `#[handler]` macro is strongly recommended.

---

## Best practices

1. **Keep handlers focused** — One handler per business operation.
2. **Use `Ack::NoAck` for transient errors** — Network timeouts, rate limits, temporary unavailability.
3. **Use `Ack::Dead` for permanent errors** — Invalid payload, unauthorized, non-existent entity.
4. **Set a `timeout`** — Prevents a stuck handler from blocking the worker forever.
5. **Match `workers` to your workload** — I/O-bound handlers benefit from more workers; CPU-bound handlers benefit from fewer.

---

## Next steps

- [Middleware](middleware.md) — Add logging, metrics, or auth to your handlers
- [Sending Messages](sending.md) — How to send messages to handlers
