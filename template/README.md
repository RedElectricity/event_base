# event_base starter template

A minimal, compiling skeleton for an [event_base](https://crates.io/crates/event_base)
application. Copy it and you have a bootable node with one handler.

## Scaffold it

```bash
cargo install cargo-generate      # once
cargo generate --path ./template  # or: cargo generate <repo-url>
```

Then, in the new project:

```bash
cargo run
```

You should see the handler process the sample message. `Ctrl‑C` shuts the node
down cleanly.

## What's inside

- `src/main.rs` — a `#[handler]` on the `jobs` topic, booted through
  `Bootstrap::from_file("eb.toml")` with a graceful fallback to an in‑memory
  Host when no config is present.
- `eb.toml` — the node configuration: role, queue/WAL backends, and an
  (optional, commented) gRPC control‑plane section. Unknown keys are rejected,
  so typos fail at load.
- `Cargo.toml` — uses `event_base = { path = ".." }` so it builds in‑tree. Once
  you move the project out, switch that line to the released version, e.g.
  `event_base = "0.8"`.

## Inspect a running node with `ebctl`

Uncomment the `[grpc]` block in `eb.toml`, run the app, then in another shell:

```bash
ebctl -a 127.0.0.1:50051 status
ebctl -a 127.0.0.1:50051 publish -T jobs -p '{"id": 1}'
ebctl -a 127.0.0.1:50051 workers -T jobs
```
