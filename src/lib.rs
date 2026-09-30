pub use event_base_core as core;

#[cfg(feature = "audit")]
pub use event_base_audit as audit;

#[cfg(feature = "macro")]
pub use event_base_macro_attr as macro_attr;
#[cfg(feature = "macro")]
pub use event_base_macro_func as macro_func;

// Flat re-exports of the user-facing macros so `use event_base::prelude::*` (or
// the crate root) makes `#[handler]`, `send_msg!`, and `start_system!` directly
// callable — previously only reachable as `event_base::macro_attr::handler`,
// which no doc ever showed.
#[cfg(feature = "macro")]
pub use event_base_macro_attr::handler;
#[cfg(feature = "macro")]
pub use event_base_macro_func::{send_msg, start_system};

/// Typed `eb.toml` node configuration (behind the default‑on `config` feature).
#[cfg(feature = "config")]
pub mod config;
/// One‑call [`Bootstrap`](bootstrap::Bootstrap) startup (default‑on `config`).
#[cfg(feature = "config")]
pub mod bootstrap;

#[cfg(feature = "config")]
pub use bootstrap::{BootError, Bootstrap, Running};
#[cfg(feature = "config")]
pub use config::{GrpcSection, NodeConfig, QueueSection, Role, WalSection};

/// The conveniences a first program needs, in one glob import.
pub mod prelude;

#[cfg(feature = "gRPC")]
pub use event_base_grpc as grpc;

#[cfg(feature = "middleware")]
pub use event_base_middleware as middleware;

pub use event_base_queue::crossfire;
#[cfg(feature = "memory")]
pub use event_base_queue::flume;
pub use event_base_queue::mpmc;

#[cfg(feature = "memory")]
pub use event_base_wal::memory as memory_wal;

#[cfg(feature = "persistent")]
pub use event_base_wal::persistent;

/// Redis Streams queue backend — [`RedisStreamQueueFactory`](event_base_queue::redis_streams::RedisStreamQueueFactory)
/// is reachable through this module when the `redis` feature is enabled.
#[cfg(feature = "redis")]
pub use event_base_queue::redis_streams;

/// Redis WAL backend — [`RedisWal`](event_base_wal::redis::RedisWal).
#[cfg(feature = "redis")]
pub use event_base_wal::redis as redis_wal;
