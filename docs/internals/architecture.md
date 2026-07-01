# Architecture

This document describes the internal module structure, crate dependencies, and data flow of `event_base`.

---

## Workspace structure

`event_base` is a Cargo workspace with 8 crates:

```
event_base/                     # Umbrella crate — re-exports all public APIs
├── event_base_core/            # Core types, traits, routing, workers, WAL trait
├── event_base_wal/             # WAL implementations (MemoryWal, PersistentWal)
├── event_base_queue/           # Queue implementations (Flume, MPMC)
├── event_base_middleware/      # Built-in middleware (Logger)
├── event_base_macro_attr/      # #[handler] procedural macro
├── event_base_macro_func/      # send_msg! and start_system! macros
├── event_base_audit/           # Audit logging subsystem
├── event_base_grpc/            # gRPC management API (optional)
└── event_base_test/            # Integration tests and benchmarks
```

---

## Crate dependency graph

```
                        ┌─────────────────────┐
                        │    event_base        │  (umbrella)
                        │  (feature gates)     │
                        └──────┬──────┬────────┘
                               │      │
           ┌───────────────────┘      └───────────────────┐
           ▼                                                ▼
┌──────────────────────┐                    ┌──────────────────────────┐
│  event_base_core     │◄─── depends ────┤  event_base_macro_attr   │
│                      │                  │  (#[handler])             │
│ • EMessage           │                  └──────────────────────────┘
│ • EHandler / Ack     │
│ • Middleware / Next   │                    ┌──────────────────────────┐
│ • TopicRouter        │◄─── depends ────┤  event_base_macro_func   │
│ • ConsumerRouter     │                  │  (send_msg!,start_system!)│
│ • Worker             │                  └──────────────────────────┘
│ • WorkerRegistry     │
│ • Shutdown strategies│                    ┌──────────────────────────┐
│ • Wal trait          │◄─── impl ────────┤  event_base_wal          │
│ • AuditManager       │                  │  • MemoryWal              │
│ • TraceLayer         │                  │  • PersistentWal          │
│ • Metrics            │                  └──────────────────────────┘
│ • SystemHandlers     │
│ • Error types        │                    ┌──────────────────────────┐
└──────────────────────┘◄─── impl ────────┤  event_base_queue        │
        │                                  │  • flume (MemoryQueue)   │
        │  ┌───────────────────────┐       │  • mpmc (Multi-prod/cons)│
        ├──┤ event_base_middleware │       └──────────────────────────┘
        │  │ • LoggerMiddleware    │
        │  └───────────────────────┘       ┌──────────────────────────┐
        │                                  │  event_base_audit        │
        ├──────────────────────────────────┤  • Audit writers         │
        │                                  └──────────────────────────┘
        │  ┌───────────────────────┐
        └──┤ event_base_grpc       │
           │ • Management API      │
           └───────────────────────┘
```

---

## Module breakdown (`event_base_core`)

`event_base_core` is the central crate. Its modules:

| Module | Responsibility |
|---|---|
| `message` | `EMessage`, `MessageTopic`, `MessagePayload`, `MessageMetadata`, `DeliveryMode` |
| `handler` | `EHandler` trait, `Ack` enum |
| `middleware` | `Middleware` trait, `Next`, `Pipeline` |
| `topic` | `TopicRouter` — message entry point, WAL append, delayed delivery |
| `queues` | `EProducer`/`EConsumer` traits, `QueueFactory`, `ConsumerFactory`, `ConsumerRouter` |
| `worker` | `Worker` — message processing loop, audit, WAL sync |
| `worker_registry` | `WorkerRegistry` — worker tracking, heartbeats, persistence |
| `registry` | `HANDLER_REGISTRY` — compile-time handler registration via `linkme` |
| `shutdown` | `ShutdownSender`/`ShutdownReceiver`, 7 shutdown strategies |
| `wal` | `Wal` trait, `WalRecord`, `WalRecordState`, codec, `WalClient` |
| `dead_letter` | `DeadLetterMessage`, `DeadReason` |
| `audit` | `AuditManager`, `AuditRecord`, `AuditWriter` trait |
| `trace` | `TraceRecord`, `TraceLevel` |
| `trace_layer` | `TraceLayer` — `tracing::Layer` implementation that emits trace messages |
| `metrics` | `MetricsManager`, `MetricsStore`, `NodeMetrics` |
| `system_handlers` | Built-in handlers for system topics (audit, trace, shutdown, discovery, etc.) |
| `constant` | System topic name constants (e.g., `_system.audit`) |
| `error` | Unified `CoreError` type, sub-module errors |
| `traits` | Re-exports of key traits |

---

## Data flow diagrams

### Message send flow

```
┌─────────┐     send_msg! / TopicRouter::send()
│ Sender  │──────────────────────────────────────►┌──────────────┐
└─────────┘                                       │ TopicRouter  │
                                                   │              │
                                                   │ 1. WAL.append│──►┌────────┐
                                                   │    (Pending) │   │  WAL   │
                                                   │              │   └────────┘
                                                   │ 2. If delayed│──►┌────────┐
                                                   │    → schedule│   │Schedule│
                                                   │              │   └────────┘
                                                   │ 3. producer  │
                                                   │    .send()   │──►┌──────────────┐
                                                   └──────────────┘   │ Queue (flume)│
                                                                      └──────────────┘
```

### Message processing flow

```
┌──────────────────┐    claim()    ┌──────────────────┐
│   Main Consumer  │──────────────►│ ConsumerRouter   │
│  (dispatch loop) │               │                  │
└──────────────────┘               │ 1. Select worker │
                                   │    (by topic)    │
                                   │ 2. Forward msg   │
                                   │ 3. Ack claim     │
                                   └────────┬─────────┘
                                            │
                                    worker.producer.send()
                                            │
                                            ▼
                                   ┌──────────────────┐
                                   │    Worker        │
                                   │                  │
                                   │ 1. receive()     │
                                   │ 2. status=Working│
                                   │ 3. WAL: mark     │
                                   │    Processing    │
                                   │ 4. Pipeline.run()│
                                   │    ├─ Middleware1 │
                                   │    ├─ Middleware2 │
                                   │    └─ Handler    │──► Ack
                                   │ 5. WAL: mark     │
                                   │    Complete/Fail │
                                   │ 6. Send audit    │──► _system.audit
                                   │ 7. status=Idle   │
                                   └──────────────────┘
```

### Crash recovery flow

```
┌─────────────┐    startup     ┌──────────────────┐
│  System     │───────────────►│ TopicRouter      │
│  Startup    │                │ .replay()        │
└─────────────┘                │                  │
                               │ 1. Load Pending  │──►┌────────┐
                               │    from WAL      │   │  WAL   │
                               │                  │   └────────┘
                               │ 2. For each msg: │
                               │    ├─ Future?    │──► Re-schedule
                               │    └─ Now?      │──► Re-send via
                               │                   │  TopicRouter
                               │                   └──────────► Queue
                               └──────────────────┘
```

### System topic interaction (distributed)

```
┌──────────┐                    ┌──────────┐
│  Host    │                    │  Worker  │
└────┬─────┘                    └────┬─────┘
     │                               │
     │◄──── worker_discovery ────────┤  Worker registers itself
     │                               │
     │◄──── worker_heartbeat ────────┤  Periodic heartbeat
     │                               │
     │◄──── wal_sync ────────────────┤  WAL state updates
     │                               │
     │◄──── audit ───────────────────┤  Audit log events
     │◄──── trace ───────────────────┤  Tracing spans
     │                               │
     │──── shutdown ────────────────►│  Shutdown command
     │◄──── shutdown_ack ────────────┤  Shutdown confirmation
     │                               │
     │──── topic_sync ──────────────►│  Topic configuration
     │◄──── topic_discovery ─────────┤  Worker's topic list
```

---

## Global singletons

The system uses several global singletons (initialized once at startup):

| Singleton | Type | Initialized by |
|---|---|---|
| `TOPIC_ROUTER` | `OnceLock<Arc<TopicRouter>>` | `TopicRouter::init()` |
| `CONSUMER_ROUTER` | `OnceLock<Arc<ConsumerRouter>>` | `ConsumerRouter::init()` |
| `WORKER_REGISTRY` | `OnceLock<Arc<WorkerRegistry>>` | `WorkerRegistry::init()` |
| `AUDIT_MANAGER` | `OnceLock<Arc<AuditManager>>` | `AuditManager::init()` |
| `NODE_NAME` | `OnceLock<Arc<String>>` | `set_node_name()` |
| `NODE_TYPE` | `RwLock<Option<Arc<NodeType>>>` | `set_node_type()` |

---

## Error handling

All errors are unified under `CoreError`:

```
CoreError
├── Queue(QueueError)
├── Wal(WalError)
├── SerdeSerialize(serde_json::Error)
├── Middleware(MiddlewareError)
├── Audit(AuditError)
├── Shutdown(ShutdownError)
├── Serialize(SerializeError)
├── Topic(TopicError)
├── Handler(HandlerError)
├── IoError(std::io::Error)
├── QueueSendError(String)
├── InvalidParameter(String)
├── AlreadyInitialized
├── NotFound(String)
├── Timeout
├── SystemShutdown
├── ErrorTime
└── Unknown(String)
```

---

## Feature flags

The umbrella crate (`event_base`) uses feature flags to conditionally include sub-crates:

```toml
[features]
default = ["memory", "macro"]
full = ["gRPC", "middleware", "macro", "memory", "persistent", "audit"]

gRPC = []         # Includes event_base_grpc
middleware = []   # Includes event_base_middleware
audit = []        # Includes event_base_audit
memory = []       # Includes event_base_wal::memory + event_base_queue::flume
persistent = []   # Includes event_base_wal::persistent
macro = []        # Includes event_base_macro_attr + event_base_macro_func
```

---

## Key design decisions

1. **WAL-first** — Messages are persisted before enqueuing. This guarantees at-least-once delivery semantics.
2. **Compile-time registration** — The `#[handler]` macro uses `linkme` distributed slices to collect all handlers at compile time, eliminating runtime registration boilerplate.
3. **Singleton architecture** — Core components (TopicRouter, ConsumerRouter, WorkerRegistry) are global singletons. This simplifies the API but limits multi-instance deployments to separate processes.
4. **Trait-based pluggability** — `Wal`, `QueueFactory`, `EProducer`/`EConsumer`, `Middleware`, and `AuditWriter` are all traits, allowing custom implementations.
5. **System topics** — Internal communication uses the same message infrastructure as user messages, ensuring consistent reliability and observability.

---

## Next steps

- [Quick Start](../guide/quick-start.md) — Build your first event_base application
- [Core Concepts](../guide/core-concepts.md) — Deep dive into the event model
