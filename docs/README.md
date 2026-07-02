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

### Queue benchmarks

| Benchmark | Throughput |
|---|---|
| `queue_send/flume` (50K) | **13.15 Melem/s** |
| `queue_send/mpmc` (50K) | **13.82 Melem/s** |
| `queue_send/crossfire` (50K) | **13.85 Melem/s** |
| `queue_recv/flume` (50K) | **17.63 Melem/s** |
| `queue_recv/mpmc` (50K) | **17.98 Melem/s** |
| `queue_recv/crossfire` (50K) | **18.27 Melem/s** |
| `queue_claim_ack/flume` (10K) | **929.82 Kelem/s** |
| `queue_claim_ack/mpmc` (10K) | **931.83 Kelem/s** |
| `queue_claim_ack/crossfire` (10K) | **933.77 Kelem/s** |

### System benchmarks

| Benchmark | Throughput | Notes |
|---|---|---|
| `TopicRouter::send` (50K) | **13.92 Melem/s** | WAL append + producer send |
| `system_send_queue/flume` (50K) | **2.93 Melem/s** | WAL append + queue producer.send |
| `system_send_queue/mpmc` (50K) | **3.15 Melem/s** | WAL append + queue producer.send |
| `system_send_queue/crossfire` (50K) | **3.15 Melem/s** | WAL append + queue producer.send |
| `handler-only` (10K) | **229.74 Kelem/s** | Handler + WAL sync + audit |
| `handler+cpu` (10K) | **230.32 Kelem/s** | CPU-bound handler processing |
| `handler+1mw` (10K) | **226.28 Kelem/s** | Handler with one middleware |
| `handler-only-4w` (10K) | **78.43 Melem/s** | 4 parallel workers |
| `handler-only-8w` (10K) | **123.35 Melem/s** | 8 parallel workers |
| `handler+1mw-4w` (10K) | **55.94 Melem/s** | 4 workers + middleware |
| `handler+1mw-8w` (10K) | **92.27 Melem/s** | 8 workers + middleware |
| `system_full_pipeline_backends_4w/crossfire` (10K) | **21.16 Melem/s** | Full pipeline, 4 workers |
| `system_full_pipeline_backends_8w/crossfire` (10K) | **28.77 Melem/s** | Full pipeline, 8 workers |

Full benchmark reports: [`target/criterion/`](https://github.com/RedElectricity/event_base)

## License

BSD-3-Clause