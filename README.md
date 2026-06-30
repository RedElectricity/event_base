# EventBase

> <div style="text-align: center;">Stop wiring channels. Start orchestrating events.</div>
>
> **A type-safe, macro-driven event-driven framework for Rust applications.**

[![Crates.io](https://img.shields.io/crates/v/event_base)](https://crates.io/crates/event_base)
[![docs.rs](https://img.shields.io/docsrs/event_base)](https://docs.rs/event_base)

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

## ⭐ Why event_base?

  In Rust, you have message queue clients (`lapin`, `rdkafka`) and event sourcing libraries (`eventastic`, `sourcerer`). But nothing bridges the gap — **application-level event orchestration with macro-driven DX** and **enterprise-grade reliability built in**.

  `event_base` is for you if:

  - You want type-safe, compile-time guaranteed event handlers.
  - You need persistence and crash recovery without running a separate message broker.
  - You want observability (audit + trace) enabled by default, not as an afterthought.
  - You're building a Rust application and want **the best developer experience** for event-driven architecture.

## 🏭 Production Ready

- **Dogfooded** — We use `event_base` in my next project. It won't be abandoned.
- **Auditable** — Every line is human-written, reviewable, and explainable. ***No vibe coding.***
- **Long-term maintenance** — This is not a side project. We depend on it too.

## 📚 Documentation

- [API Reference](TODO)
- [Architecture Guide](TODO)
- [Template - Normal](TODO)
- [Template - Slint](TODO)

## 🧩 Templates

Get started in seconds with `cargo generate`:

```bash
cargo install cargo-generate

# Core template
cargo generate --git TODO

# GUI template (Slint + event_base)
cargo generate --git TODO
```

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## 📄 License

See the [LICENSE](LICENSE) file for details.

---

*Built with ❤️ by RedElectricity.*