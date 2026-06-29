//! Metrics collection and aggregation for the message system.
//!
//! This module provides:
//! - In‑memory aggregation of audit events into business metrics.
//! - Collection of node‑level system metrics (CPU, memory, worker count).
//! - A global manager that combines both and exposes snapshots.
//! - A store for the latest node metrics.

pub mod aggregator;
pub mod manager;
pub mod node;
pub mod node_store;
