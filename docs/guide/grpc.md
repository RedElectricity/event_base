# gRPC Control Plane & `ebctl`

Every node can expose a gRPC control plane for observability and remote control. The protocol is served by the `event_base_grpc` crate; `ebctl` is the operator CLI that speaks it.

## Serving a node

`ServeConfig` is the single builder for the control plane. It replaced the three near-duplicate `serve` / `serve_with_token` / `serve_tls` functions (those remain for compatibility).

```rust
use event_base_grpc::ServeConfig;

let addr = "0.0.0.0:50051".parse()?;

ServeConfig::new(addr)
    .token("s3cr3t")     // require `authorization: Bearer <token>`
    .reflection(false)   // withhold the API schema in production
    .serve()
    .await?;
```

Defaults: **Server Reflection ON**, **Health service ON**, **no auth**. Turn reflection off for production; turn auth on the moment the port is reachable by another process. When a token is set, the auth layer wraps every service — including reflection and health.

With the `config` feature you don't call this directly — a `[grpc]` section in `eb.toml` makes `Bootstrap` spawn the server for you (see [Configuration](configuration.md)).

### TLS

Build with the `grpc-tls` feature, then:

```rust
ServeConfig::new(addr)
    .tls(cert_pem, key_pem)
    .token("s3cr3t")
    .serve()
    .await?;
```

## RPCs

| RPC | Purpose |
|---|---|
| `RespCheck` | liveness |
| `GetNodeStatus` | this node + the fleet it tracks (role, ready, topic/worker counts, per-node metrics) |
| `ListTopics` | registered topics |
| `ListWorkers` | workers for a topic (Host only) |
| `GetTopicStats` | per-topic enqueue/complete/fail/retry + latency |
| `GetNodeMetrics` | metrics for one node by name |
| `Publish` | inject a message on a business topic (refuses empty topics and `_system.*`) |
| `Shutdown` | request a fleet shutdown with one of the built-in strategies |

`Publish` targets business topics only — `_system.*` belongs to the coordination plane and dedicated RPCs (`Shutdown`), so an operator can't forge discovery or heartbeat frames.

## Connecting from Rust

```rust
use event_base_grpc::{connect, connect_with_token};

// Unauthenticated server:
let mut client = connect("127.0.0.1:50051").await?;

// Token-gated server:
let mut client = connect_with_token("127.0.0.1:50051", "s3cr3t").await?;
```

For TLS, `connect_tls(addr, ca_pem, token)` (with the `tls` feature) verifies the server against your CA. The scheme is inferred: a bare `host:port` becomes `http://` (or `https://` for `connect_tls`).

## `ebctl`

The `ebctl` binary drives the same RPCs from a shell. Install with `cargo install ebctl`, point it at a node with `--addr` (default `127.0.0.1:50051`) and `--token` (or `EBCTL_ADDR` / `EBCTL_TOKEN`), and run a subcommand:

```bash
ebctl -a 127.0.0.1:50051 status
ebctl -a 127.0.0.1:50051 topics
ebctl -a 127.0.0.1:50051 workers -T orders
ebctl -a 127.0.0.1:50051 publish -T orders -p '{"id": 7}'
ebctl -t "$EB_TOKEN" -a 127.0.0.1:50051 shutdown --force
```

Subcommands: `ping`, `status`, `topics`, `workers`, `metrics`, `node-metrics`, `publish`, `shutdown`. `publish` reads the payload from stdin when you pass `-p -`. Commands exit non-zero and print the server's gRPC status on failure.

Because `ebctl` always builds an authenticated client (an empty token sends no header), the same code path works against both unauthenticated and token-gated nodes.

## Introspecting with `grpcurl` / `buf`

With reflection on (the default), you can explore a running node without the `.proto`:

```bash
grpcurl -plaintext 127.0.0.1:50051 list
grpcurl -plaintext 127.0.0.1:50051 describe event_base.EventBase
```

If a token is set, add `-H 'authorization: Bearer <token>'`.

## See also

- [Configuration](configuration.md) — enabling `[grpc]` from `eb.toml`
- [Shutdown Strategies](shutdown.md) — the strategies `ebctl shutdown` maps to
- [Distributed Mode](distributed.md) — the coordination plane the control plane observes
