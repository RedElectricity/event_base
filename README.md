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
| **DX** | Macro-driven | `#[handler]`, `send_msg!`, `start_system!`, and a one-line `Bootstrap` — zero boilerplate |
| **Delivery** | 3 modes | Standard (competing consumers), Broadcast (all workers), Repeated (N times) |
| **Persistence** | WAL | Write-ahead log with crash recovery — `MemoryWal`, `PersistentWal`, and `RedisWal` (feature `redis`) |
| **Resilience** | Dead Letter Queue | Automatic DLQ after max retries or explicit `Ack::Dead` |
| **Resilience** | Backpressure | `try_send` (non-blocking) and `send_timeout` |
| **Distributed** | Host/Worker | Node roles with worker discovery (`_system.worker_discovery`), heartbeats + stale eviction, topic sync, and node-qualified worker names; share queues across processes with the Redis backend (feature `redis`) — competing consumers for business topics, per-node fan-out groups for coordination |
| **Shutdown** | 7 strategies | TwoStage, Force, Timeout, Graceful, StateBasedIdle, Batched, Timeout |
| **Observability** | Audit | Built-in audit logging (`_system.audit`) with ring buffer and custom writers |
| **Observability** | Tracing | Distributed tracing via `tracing` crate + `TraceLayer` |
| **Observability** | Metrics | Per-node and system-level metrics |
| **Middleware** | Composable | `impl Middleware` — logger, metrics, auth, or custom |
| **Management** | gRPC API | `ServeConfig` serves status/metrics/workers, `Publish`, and shutdown, with gRPC Server Reflection + the standard Health service on by default; `token` / TLS gate it. Drive it from the shell with the [`ebctl`](ebctl/) CLI |

---

## Quick Start

A complete, compiling node in ~25 lines. Boot it with `cargo run`, or scaffold a project from it with [`cargo generate`](template/).

```rust
use event_base::prelude::*;

#[handler(topic = "order", workers = 2)]
async fn handle_order(msg: &EMessage) -> Ack {
    println!("[{}] received order", msg.id);
    Ack::Ack
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // One line wires the router, consumer loop, system handlers, your
    // #[handler]s, and (optionally) the gRPC control plane.
    let node = Bootstrap::host("demo").start().await?;

    // Publish a message on the `order` topic.
    let msg = EMessage::new(
        MessageTopic("order".into()),
        MessagePayload(b"hello".to_vec()),
        DeliveryMode::Standard,
        None,
    );
    event_base::core::topic::TopicRouter::global()
        .read()
        .await
        .send("order", msg, None, None)
        .await?;

    node.wait().await; // block until Ctrl-C or a `Shutdown` command
    Ok(())
}
```

`#[handler]` expands to `::async_trait` and `::linkme`, so declare them as direct
dependencies:

```toml
[dependencies]
event_base = { version = "0.8", features = ["full"] }
tokio = { version = "1", features = ["full"] }
async-trait = "0.1"
linkme = "0.3"
```

For config-driven boots, drop an `eb.toml` next to the binary and use
`Bootstrap::from_file("eb.toml")?` instead of `Bootstrap::host(..)` — see the
[Configuration guide](docs/guide/configuration.md).

---

## Installation

```bash
cargo add event_base
```

Enable features as needed:

```toml
[dependencies]
event_base = { version = "0.8", features = ["full"] }
```

### Feature flags

| Feature | Description | Included in default |
|---|---|---|
| `memory` | In-memory queue (`flume`) and WAL (`MemoryWal`) | ✅ |
| `macro` | `#[handler]` attribute and `send_msg!` / `start_system!` macros | ✅ |
| `config` | `eb.toml` `NodeConfig` + one-line `Bootstrap` (pulls `toml`/`serde`/`tokio`) | ✅ |
| `persistent` | File-based `PersistentWal` | ❌ |
| `redis` | Redis Streams queue factory + `RedisWal` backend | ❌ |
| `middleware` | Built-in middleware (Logger, etc.) | ❌ |
| `gRPC` | Re-export of the gRPC control‑plane module | ❌ |
| `grpc-tls` | TLS for the gRPC server (`ServeConfig::tls`) | ❌ |
| `audit` | Audit logging subsystem | ❌ |

---

## Performance

Benchmarks measured with `criterion` (see `event_base_test/benches/`).

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

### System send benchmarks

| Benchmark | Throughput | Notes |
|---|---|---|
| `TopicRouter::send` (50K) | **13.92 Melem/s** | WAL append + producer send |
| `system_send_queue/flume` (50K) | **2.93 Melem/s** | WAL append + queue producer.send |
| `system_send_queue/mpmc` (50K) | **3.15 Melem/s** | WAL append + queue producer.send |
| `system_send_queue/crossfire` (50K) | **3.15 Melem/s** | WAL append + queue producer.send |

### System process benchmarks (10K messages)

| Benchmark | Throughput | Notes |
|---|---|---|
| `handler-only` | **229.74 Kelem/s** | Pipeline: handler + WAL sync + audit |
| `handler+cpu` | **230.32 Kelem/s** | CPU-bound handler processing |
| `handler+1mw` | **226.28 Kelem/s** | Handler with one middleware |

### Parallel processing (10K messages)

| Benchmark | Throughput |
|---|---|
| `handler-only-4w` | **78.43 Melem/s** |
| `handler+cpu-4w` | **78.04 Melem/s** |
| `handler+1mw-4w` | **55.94 Melem/s** |
| `handler-only-8w` | **123.35 Melem/s** |
| `handler+cpu-8w` | **123.29 Melem/s** |
| `handler+1mw-8w` | **92.27 Melem/s** |

### Full pipeline (10K messages)

| Benchmark | Throughput |
|---|---|
| `system_full_pipeline_backends_4w/flume` | **20.74 Melem/s** |
| `system_full_pipeline_backends_4w/mpmc` | **20.97 Melem/s** |
| `system_full_pipeline_backends_4w/crossfire` | **21.16 Melem/s** |
| `system_full_pipeline_backends_8w/flume` | **27.56 Melem/s** |
| `system_full_pipeline_backends_8w/mpmc` | **25.91 Melem/s** |
| `system_full_pipeline_backends_8w/crossfire` | **28.77 Melem/s** |
| `system_full_pipeline_cr/4w` | **160.39 Kelem/s** |
| `system_full_pipeline_cr/8w` | **156.35 Kelem/s** |

---

## Documentation

- [Quick Start](docs/guide/quick-start.md)
- [Configuration (eb.toml & Bootstrap)](docs/guide/configuration.md)
- [Core Concepts](docs/guide/core-concepts.md)
- [Handlers](docs/guide/handler.md)
- [Middleware](docs/guide/middleware.md)
- [Sending Messages](docs/guide/sending.md)
- [Persistence & WAL](docs/guide/persistence.md)
- [Shutdown Strategies](docs/guide/shutdown.md)
- [Distributed Mode](docs/guide/distributed.md)
- [gRPC Control Plane & ebctl](docs/guide/grpc.md)
- [Architecture](docs/internals/architecture.md)
- [API Reference](https://docs.rs/event_base)

---

## License

BSD-3-Clause. See [LICENSE](LICENSE).