//! Write‑Ahead Log (WAL) for message durability and recovery.
//!
//! This module provides the core WAL trait, record types, serialization codecs,
//! and a client for synchronizing message states between workers and the host.

pub mod codec;
pub mod sync;
// `wal::wal` is the path every consumer (and the published docs) already use;
// renaming the submodule would be a breaking change for zero benefit.
#[allow(clippy::module_inception)]
pub mod wal;
