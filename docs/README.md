# EventBase

> **An application-level event-driven framework for Rust — type-safe, macro-driven, and built for reliability.**

`event_base` is a lightweight, macro-driven event-driven framework for building reliable, observable, and scalable applications in Rust. You declare handlers, chain middleware, and define reliable workflows — all at compile time.

It is **not** a distributed message queue. It focuses on **in-process event orchestration** with persistence, observability, and graceful shutdown built in.

---

## Quick Start

```rust
use event_base::prelude::*;

#[handler(topic = "greeting", workers = 2)]
async fn handle_greeting(msg: &EMessage) -> Ack {
    println!("Got: {:?}", msg);
    Ack::Ack
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    start_queue_system! {
        factory: MemoryQueueFactory::new(1000),
        wal: Some(MemoryWal::new()),
    }
    send_msg!("greeting", EMessage::new("greeting", b"Hello!".to_vec())).await?;
    Ok(())
}
```

## Installation

```bash
cargo add event_base
```

Enable full features:

```toml
[dependencies]
event_base = { version = "0.1", features = ["full"] }
```

## Guides

| Guide | Description |
|---|---|
| [Quick Start](guide/quick-start.md) | Complete walkthrough — first handler, first message |
| [Core Concepts](guide/core-concepts.md) | EMessage, Handler, Ack, routing, message flow |
| [Handlers](guide/handler.md) | `#[handler]` macro — parameters, Ack variants |
| [Middleware](guide/middleware.md) | Write and compose middleware pipelines |
| [Sending Messages](guide/sending.md) | Standard, Broadcast, Repeated delivery |
| [Persistence & WAL](guide/persistence.md) | Crash recovery, MemoryWal vs PersistentWal |
| [Shutdown Strategies](guide/shutdown.md) | 7 graceful/forceful shutdown patterns |
| [Distributed Mode](guide/distributed.md) | Host/Worker nodes, discovery, heartbeats |

## Internals

| Document | Description |
|---|---|
| [Architecture](internals/architecture.md) | Module structure, crate deps, data flow diagrams |

## Performance

| Benchmark | Throughput |
|---|---|
| `TopicRouter::send` (50K msgs) | **2.08 Melem/s** |
| `queue_send_mpmc` (1M msgs) | **1.01 Melem/s** |
| `handler-only` (10K msgs) | **102.83 Kelem/s** |
| `handler+1mw` (10K msgs) | **39.42 Kelem/s** |

Full benchmark reports: [`target/criterion/`](https://github.com/RedElectricity/event_base)

## License

BSD-3-Clause