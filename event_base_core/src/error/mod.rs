//! Core error types for the entire system.
//!
//! This module defines the unified [`CoreError`] enumeration, which aggregates
//! all possible errors from sub‑modules. It also re‑exports the individual
//! error enums for convenience.

pub mod audit;
pub mod handler;
pub mod middleware;
pub mod queue;
pub mod serialize;
pub mod shutdown;
pub mod topic;
pub mod wal;

use crate::error::audit::AuditError;
use crate::error::handler::HandlerError;
use crate::error::middleware::MiddlewareError;
use crate::error::queue::QueueError;
use crate::error::serialize::SerializeError;
use crate::error::shutdown::ShutdownError;
use crate::error::topic::TopicError;
use crate::error::wal::WalError;
use std::time::{Duration, SystemTimeError};
use thiserror::Error;

/// The unified error type for the entire message system.
///
/// It wraps errors from all subsystems and adds system‑level errors like
/// timeouts, invalid parameters, and shutdown signals.
#[derive(Debug, Error)]
pub enum CoreError {
    /// An error from the queue subsystem.
    #[error("Queue error: {0}")]
    Queue(#[from] QueueError),

    /// An error from the Write‑Ahead Log subsystem.
    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    /// A JSON serialization/deserialization error.
    #[error("Serde Serialize Error: {0}")]
    SerdeSerialize(#[from] serde_json::Error),

    /// An error from the middleware chain.
    #[error("Middleware error: {0}")]
    Middleware(#[from] MiddlewareError),

    /// An error from the audit subsystem.
    #[error("Audit error: {0}")]
    Audit(#[from] AuditError),

    /// An error from the shutdown subsystem.
    #[error("Shutdown error: {0}")]
    Shutdown(#[from] ShutdownError),

    /// A serialization/deserialization error (e.g., bincode).
    #[error("Serialization error: {0}")]
    Serialize(#[from] SerializeError),

    /// An error from the topic registry.
    #[error("Topic error: {0}")]
    Topic(#[from] TopicError),

    /// An error from the message handler.
    #[error("Handler error: {0}")]
    Handler(#[from] HandlerError),

    /// An I/O error (e.g., file system or network).
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// A send operation on a queue failed.
    #[error("Queue Send Error: {0}")]
    QueueSendError(String),

    /// An invalid parameter was provided to a function.
    #[error("Invalid Parameter: {0}")]
    InvalidParameter(String),

    /// The data provided was invalid or malformed.
    #[error("Invalid Type: {0}")]
    InvalidData(String),

    /// A task join operation failed.
    #[error("Task Join Error: {0}")]
    TaskJoinError(String),

    /// An attempt was made to initialize a global singleton more than once.
    #[error("Object already exists")]
    AlreadyInitialized,

    /// An error occurred while calculating processing time.
    #[error("Process Time Error: {0}")]
    ProcessTimeError(#[from] SystemTimeError),

    /// A time‑related error (e.g., invalid delivery time).
    #[error("Error Time")]
    ErrorTime,

    /// An operation timed out after the specified duration.
    #[error("Timeout: {0:?}")]
    Timeout(Duration),

    /// The requested worker was not found.
    #[error("Worker Not Found: {0}")]
    WorkerNotFound(String),

    /// An operation is unsupported in the current context.
    #[error("Unsupported: {0}")]
    Unsupported(String),

    /// The system is shutting down and cannot accept new operations.
    #[error("Shutting down")]
    ShuttingDown,

    /// A catch‑all for other, uncategorized errors.
    #[error("Other: {0}")]
    Other(String),
}
