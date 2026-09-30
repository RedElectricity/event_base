# Configuration — `eb.toml` & `Bootstrap`

The default `config` feature adds a typed `eb.toml` format and `Bootstrap`, the one-call way to start a node. `Bootstrap` wraps the exact `start_system!` recipe (global init → queue factory → WAL → system handlers → user handlers → consumer loop → scheduler/heartbeat) behind config, and optionally exposes the gRPC control plane. It changes no physics; it removes the boilerplate.

## The types

```rust
use event_base::prelude::*;

// From a file next to the binary:
let node = Bootstrap::from_file("eb.toml")?.start().await?;

// Or from an embedded string (tests, config servers):
let node = Bootstrap::from_toml_str(include_str!("../eb.toml"))?.start().await?;

// Or fully in code with sane defaults (what the Quick Start uses):
let node = Bootstrap::host("my-node").start().await?;
let worker = Bootstrap::worker("my-worker").start().await?;
```

`Running` (the handle `start()` returns) gives you:

- `node_name()` — the resolved node name.
- `wait().await` — block until Ctrl-C **or** a shutdown signal, then stop the gRPC task.
- `shutdown()` — trigger shutdown; returns the number of receivers signalled.
- `shutdown_sender()` — clone the sender to hand to your own signal handler.

## `eb.toml`

Unknown keys are **rejected** at load, so a typo fails immediately instead of being silently ignored.

```toml
[node]
name = "orders-host"   # omitted → the process hostname
role = "host"          # host | worker

[queue]
backend = "memory"     # memory | redis
capacity = 100000      # memory backend only

[wal]
backend = "memory"     # memory | persistent | redis
# path = "orders.wal"  # persistent backend

# Optional control plane. Absent = no gRPC server.
[grpc]
addr = "0.0.0.0:50051"
token = ""             # empty → unauthenticated (localhost admin only)
reflection = true      # gRPC Server Reflection; disable in production
health = true          # standard grpc.health.v1.Health service
```

### Backends and features

A backend in `eb.toml` is validated at parse time, but *constructing* it needs the matching cargo feature:

| Backend | Section | Cargo feature |
|---|---|---|
| `queue.backend = "redis"` | `[queue]` | `redis` |
| `wal.backend = "persistent"` | `[wal]` | `persistent` |
| `wal.backend = "redis"` | `[wal]` | `redis` |
| `[grpc]` served | `[grpc]` | (module always linked; `grpc-tls` for TLS) |

Selecting a backend whose feature is disabled returns a clear `BootError` at `start()` — e.g. *"redis queue backend requested but the `redis` feature is disabled — build event_base with --features redis"*. It never panics and never silently downgrades.

For the Redis queue backend, `queue.url`, `queue.prefix`, `queue.block_ms`, and `queue.node` (the consumer-group node id, defaults to `node.name`) are all settable; the WAL reuses `queue.url` when `wal.url` is left empty.

## Why two WAL handles

`start_system` uses one WAL for the worker **registry snapshots** and another for the **`_system.wal_sync`** record-state updates. `Bootstrap` opens both from your single `[wal]` section. On `memory` these are independent RAM stores (matching the raw macro recipe); on `redis` both handles share the same keyspace, so the two concerns stay consistent. This is why Redis is the recommended backend for multi-handle setups.

## See also

- [gRPC Control Plane & ebctl](grpc.md)
- [Distributed Mode](distributed.md) — what `host` vs `worker` roles coordinate
