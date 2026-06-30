# EventBase

> <div style="text-align: center;">Stop wiring channels. Start orchestrating events.</div>
>
> **A type-safe, macro-driven event-driven framework for Rust applications.**

**What it is:**
`event_base` is a lightweight, macro-driven event-driven framework for building reliable, observable, and scalable applications in Rust. With `event_base`, you declare handlers, chain middleware, and define reliable workflows—all at compile time.

**What it is not:**
event_base is not a distributed message queue. It does not transport data between services. I`event_base` focuses on **orchestrating events inside your application**: defining events, declaring handlers, managing state, and ensuring reliability.

## ✨ Features

- **🔒 Type-safe events** — Define events as plain Rust structs. The compiler guarantees correctness.
- **🔌 Pluggable backends** — `memory` (flume), `file`, `redis`, `kafka`, `mqtt`. Swap without changing your handlers.
- **⚡ Macro-driven DX** — `#[handler]` turns any async function into a message handler. No boilerplate.
- **📦 Multiple delivery modes** — Standard (competing consumers), Broadcast (all workers), and Repeated (N times).
- **💾 Application-level WAL** — Durable persistence with crash recovery. Your events survive restarts.
- **🔄 Automatic retry & Dead Letter Queue** — Failed messages retry with configurable backoff, then land in DLQ.
- **📊 Built-in observability** — Audit logging (`_system.audit`) and distributed tracing (`_system.trace`) enabled by default.
- **🎯 Middleware support** — Compose logging, metrics, retries, and custom logic via middleware pipeline.
- **🌐 Distributed-ready** — Host/Worker node model with built-in discovery and shutdown coordination.
- **🛑 Graceful shutdown** — 7 shutdown strategies, including two-stage drain and force timeout.
- **🧩 gRPC management API** — Query node status, list workers, trigger shutdown, stream metrics.