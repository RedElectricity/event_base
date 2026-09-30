# Quick Start

This guide walks you through your first `event_base` application — defining a handler, publishing a message, and starting the system with one line. Every snippet here is compiled by CI (see `examples/quick_start.rs`), so it builds against the released crate.

---

## Prerequisites

- Rust 2024 edition or later
- `tokio` runtime (multi-thread)

## Add the dependencies

```bash
cargo add event_base --features full
cargo add tokio --features full
cargo add async-trait linkme
```

`async-trait` and `linkme` are required because the `#[handler]` macro expands to `::async_trait` and `::linkme` at your crate root (linkme's proc-macro is unhygienic, so it cannot be hidden behind a re-export).

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

The macro generates a handler struct, implements `EHandler`, and registers it in the global handler registry at compile time via `linkme`. `workers = 2` means two concurrent worker tasks compete for messages on this topic.

## Step 2: Start the system

One `Bootstrap` call initializes the global routers, the system handlers, your registered handlers, the consumer dispatch loop, the tracing layer, and (on a Host) the delay scheduler:

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = Bootstrap::host("my-node").start().await?;

    // System is now running — send messages.
    // ...

    node.wait().await; // block until Ctrl-C or a `Shutdown` command
    Ok(())
}
```

`Bootstrap::host` boots an in-memory single-process node. To drive the role, queue/WAL backend, and an optional gRPC control plane from a file, see [Configuration](configuration.md).

## Step 3: Send a message

Build an `EMessage` and route it through the `TopicRouter`:

```rust
let msg = EMessage::new(
    MessageTopic("greeting".into()),
    MessagePayload(b"Hello, world!".to_vec()),
    DeliveryMode::Standard,
    None,
);

event_base::core::topic::TopicRouter::global()
    .read()
    .await
    .send("greeting", msg, None, None)
    .await?;
```

The four `EMessage::new` arguments are topic, payload, delivery mode, and an optional target worker. The `send` arguments are topic, message, `try_send` (`None` = blocking), and `timeout` (`None`).

You can also use the `send_msg!` macro from the prelude, which wraps the same call:

```rust
send_msg!(msg, None, None)?;
```

## Step 4: Run it

```bash
cargo run
```

You should see output like:

```
my-node running
[some-uuid-here] Got: Hello, world!
```

---

## Complete runnable example

The full program lives at [`examples/quick_start.rs`](https://github.com/RedElectricity/event_base/blob/main/examples/quick_start.rs) and runs with:

```bash
cargo run --example quick_start
```

It boots a Host, registers one handler, publishes a message, waits for the worker to see it, and shuts down cleanly. A `template/` directory scaffolds the same shape via `cargo generate`.

---

## What just happened?

1. The `#[handler]` macro registered `handle_greeting` for topic `"greeting"` with 2 workers (compile time).
2. `Bootstrap::start` initialized all globals and started the consumer dispatch loop.
3. `TopicRouter::send` pushed the message onto the `greeting` queue.
4. The `ConsumerRouter` claimed the message, selected an idle worker, and forwarded it.
5. The worker ran the handler, which printed the payload and returned `Ack::Ack`.

---

## Next steps

- [Configuration](configuration.md) — `eb.toml` and the full `Bootstrap` surface
- [Core Concepts](core-concepts.md) — Understand the EMessage, Handler, Ack model
- [Handlers](handler.md) — Deep dive into `#[handler]` parameters and Ack variants
- [Sending Messages](sending.md) — Standard, Broadcast, and Repeated delivery
- [gRPC Control Plane & ebctl](grpc.md) — Inspect and drive a running node
