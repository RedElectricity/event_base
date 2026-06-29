//! Write‑Ahead Log (WAL) for message durability and recovery.
//!
//! This module provides the core WAL trait, record types, serialization codecs,
//! and a client for synchronizing message states between workers and the host.

pub mod codec;
pub mod sync;
pub mod wal;
