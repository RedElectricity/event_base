# EventBase

[![Crates.io](https://img.shields.io/crates/v/event_base)](https://crates.io/crates/event_base)
[![docs.rs](https://img.shields.io/docsrs/event_base)](https://docs.rs/event_base)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)
[![Coverage](https://img.shields.io/badge/coverage-90%25-brightgreen)](https://github.com/RedElectricity/event_base/actions)

> **An application-level event-driven framework for Rust — type-safe, macro-driven, and built for reliability.**

`event_base` lets you define message handlers, compose middleware pipelines, and orchestrate events inside your Rust application. It is **not** a distributed message queue — it focuses on **in-process event orchestration** with persistence, observability, and graceful shutdown built in.

---

## Features

| Category | Feature | Description |
|---|---|---|
| **Events** | EMessage + Handler + Ack | Type-safe message envelope with async handlers and explicit ack semantics |
| **DX** | Macro-driven | `#[handler]`, `send_msg!`, `start_queue_system!` — zero boilerplate |
| **Delivery** | 3 modes | Standard (competing consumers), Broadcast (all workers), Repeated (N times) |
| **Persistence** | WAL | Write-ahead log with crash recovery — `MemoryWal` and `PersistentWal` |
| **Resilience** | Dead Letter Queue | Automatic DLQ after max retries or explicit `Ack::Dead` |
| **Resilience** | Backpressure | `try_send` (non-blocking) and `send_timeout` |
| **Distributed** | Host/Worker | Node roles with discovery (`_system.worker_discovery`) and topic sync |
| **Shutdown** | 7 strategies | TwoStage, Force, Timeout, Graceful, StateBasedIdle, Batched, Timeout |
| **Observability** | Audit | Built-in audit logging (`_system.audit`) with ring buffer and custom writers |
| **Observability** | Tracing | Distributed tracing via `tracing` crate + `TraceLayer` |
| **Observability** | Metrics | Per-node and system-level metrics |
| **Middleware** | Composable | `impl Middleware` — logger, metrics, auth, or custom |
| **Management** | gRPC API | Query node status, list workers, trigger shutdown, stream metrics (optional) |

---

## Quick Start

```rust
use event_base::prelude::*;

#[handler(topic = "order", workers = 2)]
async fn handle_order(msg: &EMessage) -> Ack {
    println!("Received order: {:?}", msg);
    Ack::Ack
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wal = event_base::memory_wal::MemoryWal::new();
    start_queue_system! {
        factory: MemoryQueueFactory::new(1000),
        wal: Some(wal),
    }
    send_msg!("order", EMessage::new("order", b"hello".to_vec())).await?;
    Ok(())
}
```

---

## Installation

```bash
cargo add event_base
```

Enable features as needed:

```toml
[dependencies]
event_base = { version = "0.1", features = ["full"] }
```

### Feature flags

| Feature | Description | Included in default |
|---|---|---|
| `memory` | In-memory queue (`flume`) and WAL (`MemoryWal`) | ✅ |
| `macro` | `#[handler]` attribute and `send_msg!` / `start_system!` macros | ✅ |
| `persistent` | File-based `PersistentWal` | ❌ |
| `middleware` | Built-in middleware (Logger, etc.) | ❌ |
| `gRPC` | gRPC management API (query, shutdown, metrics) | ❌ |
| `audit` | Audit logging subsystem | ❌ |

---

## Performance

Benchmarks measured on a 1M message workload (see `event_base_test/benches/`):

| Benchmark | Throughput | Notes |
|---|---|---|
| `TopicRouter::send` | **2.08 Melem/s** | Full pipeline: WAL append + producer send |
| `queue_send_mpmc` | **1.01 Melem/s** | MPMC queue send-only |
| `queue_send_flume` | **0.98 Melem/s** | Flume queue send-only |
| `handler-only (10k)` | **102.83 Kelem/s** | Pipeline: handler + WAL sync + audit |
| `handler+cpu (10k)` | **76.16 Kelem/s** | CPU-bound handler processing |
| `handler+1mw (10k)` | **39.42 Kelem/s** | Handler with one middleware |

---

## Documentation

- [Quick Start](docs/guide/quick-start.md)
- [Core Concepts](docs/guide/core-concepts.md)
- [Handlers](docs/guide/handler.md)
- [Middleware](docs/guide/middleware.md)
- [Sending Messages](docs/guide/sending.md)
- [Persistence & WAL](docs/guide/persistence.md)
- [Shutdown Strategies](docs/guide/shutdown.md)
- [Distributed Mode](docs/guide/distributed.md)
- [Architecture](docs/internals/architecture.md)
- [API Reference](https://docs.rs/event_base)

---

## License

BSD-3-Clause. See [LICENSE](LICENSE).