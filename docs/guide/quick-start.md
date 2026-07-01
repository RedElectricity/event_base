# Quick Start

This guide walks you through your first `event_base` application — defining a handler, sending a message, and starting the system.

---

## Prerequisites

- Rust 2024 edition or later
- `tokio` runtime (multi-thread recommended)

## Add the dependency

```bash
cargo add event_base
cargo add tokio --features full
```

## Step 1: Define a handler

Use the `#[handler]` attribute macro to turn an async function into a message handler:

```rust
use event_base::prelude::*;

#[handler(topic = "greeting", workers = 2)]
async fn handle_greeting(msg: &EMessage) -> Ack {
    let text = String::from_utf8_lossy(&msg.payload.0);
    println!("[{}] Got: {}", msg.id, text);
    Ack::Ack
}
```

The macro generates a handler struct, implements `EHandler`, and registers it in the global handler registry at compile time via `linkme`.

## Step 2: Start the system

Use the `start_queue_system!` macro to initialize all global components:

```rust
use event_base::flume::MemoryQueueFactory;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wal = event_base::memory_wal::MemoryWal::new();

    start_queue_system! {
        factory: MemoryQueueFactory::new(1000),
        wal: Some(wal),
    }

    // System is now running — send messages
    send_msg!("greeting", EMessage::new("greeting", b"Hello, world!".to_vec())).await?;

    // Keep the process alive
    tokio::signal::ctrl_c().await?;
    Ok(())
}
```

## Step 3: Run it

```bash
cargo run
```

You should see output like:

```
[some-uuid-here] Got: Hello, world!
```

---

## Complete runnable example

Here is the full `src/main.rs`:

```rust
use event_base::prelude::*;
use event_base::flume::MemoryQueueFactory;
use event_base::memory_wal::MemoryWal;

/// A handler that processes messages on the "greeting" topic.
///
/// `workers = 2` means two concurrent worker tasks process messages
/// from this topic in parallel (competing consumers).
#[handler(topic = "greeting", workers = 2)]
async fn handle_greeting(msg: &EMessage) -> Ack {
    let text = String::from_utf8_lossy(&msg.payload.0);
    println!("[{}] Received: {}", msg.id, text);

    // Return Ack::Ack to mark the message as successfully processed.
    // The WAL will record it as Complete.
    Ack::Ack
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── 1. Create a WAL for durability ──────────────────────────────
    // MemoryWal keeps records in RAM. Swap with PersistentWal for
    // disk-backed persistence that survives restarts.
    let wal = MemoryWal::new();

    // ── 2. Start the system ─────────────────────────────────────────
    // This initializes:
    //   - TopicRouter (routes messages by topic)
    //   - ConsumerRouter (dispatches to workers)
    //   - WorkerRegistry (tracks active workers)
    //   - All system handlers (audit, trace, shutdown, discovery, etc.)
    //   - The main consumer loop (claims and routes messages)
    //   - The tracing layer (emits TraceRecords to _system.trace)
    //   - The delay scheduler (Host only — delivers delayed messages)
    start_queue_system! {
        factory: MemoryQueueFactory::new(1000),
        wal: Some(wal),
    };

    // ── 3. Send a message ───────────────────────────────────────────
    // Messages are envelopes carrying a topic, payload, and metadata.
    // The TopicRouter appends to WAL, then pushes to the queue.
    send_msg!("greeting", EMessage::new(
        "greeting",                     // topic
        b"Hello from event_base!".to_vec(), // payload (raw bytes)
    ))
    .await?;

    // ── 4. Wait for shutdown signal ─────────────────────────────────
    tokio::signal::ctrl_c().await?;
    println!("Shutting down...");

    Ok(())
}
```

Add to `Cargo.toml`:

```toml
[dependencies]
event_base = "0.1.0"
tokio = { version = "1", features = ["full"] }
```

---

## What just happened?

1. The `#[handler]` macro registered `handle_greeting` for topic `"greeting"` with 2 workers.
2. `start_queue_system!` initialized all globals and started the consumer dispatch loop.
3. `send_msg!` created an `EMessage`, appended it to the WAL, and pushed it onto the queue.
4. The `ConsumerRouter` claimed the message, selected an idle worker, and forwarded it.
5. The worker ran the handler, which printed the payload and returned `Ack::Ack`.
6. The WAL recorded the message as `Complete`.

---

## Next steps

- [Core Concepts](core-concepts.md) — Understand the EMessage, Handler, Ack model
- [Handlers](handler.md) — Deep dive into `#[handler]` parameters and Ack variants
- [Sending Messages](sending.md) — Standard, Broadcast, and Repeated delivery
