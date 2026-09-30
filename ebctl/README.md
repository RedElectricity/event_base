# ebctl

Operator / debugging CLI for an [event_base](https://crates.io/crates/event_base)
node's gRPC control plane. It speaks the same protocol the framework serves, so
you can inspect and drive a running node from a shell — no client code required.

## Install

```bash
cargo install ebctl
```

## Point it at a node

Every command takes `--addr` (default `127.0.0.1:50051`) and `--token` (or the
`EBCTL_ADDR` / `EBCTL_TOKEN` environment variables).

```bash
ebctl -a 127.0.0.1:50051 status
ebctl -t "$EB_TOKEN" topics
```

## Commands

- `ping` — liveness (`RespCheck`).
- `status` — this node + the fleet it knows (`GetNodeStatus`): role, ready,
  topic/worker counts, and per‑node metrics.
- `topics` — registered topics (`ListTopics`).
- `workers [-T topic]` — workers, all topics if `-T` is omitted (`ListWorkers`).
- `metrics` — per‑topic enqueue/complete/fail/retry counts (`GetTopicStats`).
- `node-metrics -n <node>` — metrics for one node (`GetNodeMetrics`).
- `publish -T <topic> [-p <text>| -p -] [--broadcast] [--to-worker <w>]` —
  inject a message; `-p -` reads the payload from stdin (`Publish`). System
  topics (`_system.*`) are refused — they belong to the coordination plane.
- `shutdown <strategy>` — request a fleet shutdown (`Shutdown`). Strategies:
  `force`, `idle`, `graceful -w <worker>`, `two-stage`, `timeout`, `batched`.

Commands exit non‑zero and print the server's gRPC status on failure.

## How it connects

`ebctl` uses the crate's client helpers: it always builds an authenticated
client (`connect_with_token`); an empty token simply sends no `authorization`
header, so the same code path works against unauthenticated and token‑gated
nodes alike. The token comparison on the server is constant‑time.
