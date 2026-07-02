//! Message types for shutdown commands and acknowledgments.
//!
//! This module defines the `ShutdownStrategy` enumeration for describing
//! how workers should be terminated, the `ShutdownCommand` envelope, and
//! the `ShutdownAck` response that workers send back.

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// A shutdown strategy that can be applied to workers.
///
/// These strategies are typically sent as part of a `ShutdownCommand` message
/// to the system, or used directly by the shutdown methods in this crate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "config")]
pub enum ShutdownStrategy {
    /// Two‑stage shutdown: send a signal, wait for all workers to finish
    /// within a timeout, then force‑remove any remaining workers.
    TwoStage {
        /// Interval (milliseconds) between polls while waiting for workers.
        poll_interval_ms: u64,
        /// Total timeout (seconds) before forcing termination.
        force_timeout_secs: u64,
    },

    /// Graceful shutdown for a specific worker, waiting until it becomes idle.
    Graceful {
        /// Name of the worker to shut down.
        worker_name: String,
        /// Polling interval (milliseconds) to check the worker's status.
        poll_interval_ms: u64,
    },

    /// Immediate forceful shutdown of all workers without waiting.
    Force,

    /// Shutdown after a fixed timeout, then force‑removing all workers.
    Timeout {
        /// Total timeout (seconds) before forcing termination.
        total_timeout_secs: u64,
    },

    /// Shutdown only workers that are currently idle (not processing a message).
    StateBasedIdle,

    /// Shutdown workers in batches with a delay between batches.
    Batched {
        /// Number of workers to shut down per batch.
        batch_size: usize,
        /// Delay (milliseconds) between batches.
        interval_ms: u64,
    },
}

/// A command to initiate shutdown with a specific strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownCommand {
    /// The strategy to use for this shutdown request.
    pub strategy: ShutdownStrategy,
}

/// An acknowledgment sent by a worker after it has completed shutdown.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ShutdownAck {
    /// Name of the worker that is acknowledging.
    pub worker_name: String,
    /// Final status of the shutdown attempt.
    pub status: ShutdownStatus,
    /// Timestamp when the acknowledgment was sent.
    pub timestamp: SystemTime,
    /// Optional error message if the shutdown failed.
    pub error: Option<String>,
}

/// The final status of a worker's shutdown.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum ShutdownStatus {
    /// Shutdown completed successfully.
    Completed,
    /// Shutdown failed (e.g., due to an internal error).
    Failed,
    /// Shutdown timed out and was forced.
    Timeout,
}
