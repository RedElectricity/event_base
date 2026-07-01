# Middleware

Middleware lets you intercept, inspect, or modify messages **before** they reach the handler. Common use cases include logging, metrics, validation, authentication, and rate limiting.

---

## How it works

Middleware is a chain of components that wraps the final handler. Each middleware can:

- Inspect the message before processing
- Modify the message (e.g., add metadata)
- Short-circuit the chain by returning an `Ack` directly
- Measure and log processing time

The chain follows the **onion model**:

```
Message → Middleware1 → Middleware2 → ... → Handler → Ack → Middleware2 → Middleware1
```

Each middleware calls `next.run(msg)` to pass control to the next component. Code after `next.run()` runs on the way back (post-processing).

---

## The `Middleware` trait

```rust
#[async_trait]
pub trait Middleware: Send + Sync {
    async fn handle(&self, msg: &mut EMessage, next: Next<'_>) -> Ack;
}
```

### `Next`

```rust
pub struct Next<'a> {
    next: &'a [Box<dyn Middleware>],
    index: usize,
    handler: &'a dyn EHandler,
}

impl<'a> Next<'a> {
    /// Proceed to the next middleware or the final handler.
    pub async fn run(&self, msg: &mut EMessage) -> Ack;
}
```

---

## Writing a middleware

### Example: Logger middleware

```rust
use async_trait::async_trait;
use event_base::prelude::*;
use std::time::Instant;

pub struct LoggerMiddleware;

#[async_trait]
impl Middleware for LoggerMiddleware {
    async fn handle(&self, msg: &mut EMessage, next: Next<'_>) -> Ack {
        let start = Instant::now();
        tracing::info!("[BEFORE] Processing message: {}", msg.id);

        let ack = next.run(msg).await;

        tracing::info!("[AFTER] {} completed in {:?}: {:?}", msg.id, start.elapsed(), ack);
        ack
    }
}
```

### Example: Metrics middleware

```rust
pub struct MetricsMiddleware {
    counter: std::sync::atomic::AtomicU64,
}

#[async_trait]
impl Middleware for MetricsMiddleware {
    async fn handle(&self, msg: &mut EMessage, next: Next<'_>) -> Ack {
        self.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let ack = next.run(msg).await;
        let count = self.counter.load(std::sync::atomic::Ordering::Relaxed);
        tracing::info!("Messages processed so far: {}", count);
        ack
    }
}
```

### Example: Auth middleware (short-circuit)

```rust
pub struct AuthMiddleware {
    valid_token: String,
}

#[async_trait]
impl Middleware for AuthMiddleware {
    async fn handle(&self, msg: &mut EMessage, next: Next<'_>) -> Ack {
        // Check for an auth token in the message metadata
        let has_auth = msg.metadata.source.as_deref() == Some(&self.valid_token);
        if !has_auth {
            return Ack::Dead {
                dead_reason: DeadReason::Other("Unauthorized".into()),
            };
        }
        next.run(msg).await
    }
}
```

---

## Configuring middleware on a handler

Use the `middleware` parameter in the `#[handler]` attribute:

```rust
#[handler(
    topic = "order.created",
    workers = 2,
    middleware = [LoggerMiddleware, MetricsMiddleware]
)]
async fn handle_order(msg: &EMessage) -> Ack {
    // Business logic here
    Ack::Ack
}
```

Middleware types must be importable at the call site. They are instantiated automatically by the generated code.

### Built-in middleware

When the `middleware` feature is enabled, you get:

```rust
use event_base::middleware::logger::LoggerMiddleware;
```

`LoggerMiddleware` logs every message before and after processing with duration.

---

## The Pipeline

Internally, middleware and handler are composed into a `Pipeline`:

```rust
pub struct Pipeline {
    middlewares: Vec<Box<dyn Middleware>>,
    handler: Arc<dyn EHandler>,
}

impl Pipeline {
    pub fn new(handler: Box<dyn EHandler>) -> Self;
    pub fn with(mut self, middleware: impl Middleware + 'static) -> Self;
    pub async fn run(&self, msg: &mut EMessage) -> Ack;
}
```

You can construct pipelines manually:

```rust
use event_base::prelude::*;

let pipeline = Pipeline::new(Box::new(MyHandler))
    .with(LoggerMiddleware)
    .with(MetricsMiddleware);

let mut msg = EMessage::new("test", b"data".to_vec(), DeliveryMode::Standard, None);
let ack = pipeline.run(&mut msg).await;
```

---

## Order of execution

Middleware run in the order they are declared:

```rust
#[handler(middleware = [A, B, C])]
```

Execution order: `A.handle → B.handle → C.handle → handler → C → B → A`

If middleware `A` returns an `Ack` without calling `next.run()`, neither `B`, `C`, nor the handler will execute.

---

## Best practices

1. **Keep middleware stateless** where possible — use atomic counters or external storage for state.
2. **Don't panic** in middleware — always return an `Ack`.
3. **Use short-circuiting sparingly** — it can make debugging harder.
4. **Order matters** — place auth/validation middleware first, logging/metrics last (to capture full processing time).

---

## Next steps

- [Sending Messages](sending.md) — Delivery modes and API
- [Persistence & WAL](persistence.md) — Durable message storage
