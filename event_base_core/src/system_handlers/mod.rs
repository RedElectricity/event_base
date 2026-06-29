//! System‑level message handlers for internal control topics.
//!
//! This module provides handlers for system topics such as audit, metrics,
//! shutdown, tracing, WAL sync, worker discovery, and topic synchronization.
//! These handlers are typically registered by the [`SystemHandlerBuilder`](system::SystemHandlerBuilder)
//! during system startup.

pub mod audit;
pub mod metrics;
pub mod shutdown;
pub mod system;
pub mod topic;
pub mod trace;
pub mod wal;
pub mod worker;
