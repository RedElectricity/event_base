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
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("Queue error: {0}")]
    Queue(#[from] QueueError),

    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    #[error("Middleware error: {0}")]
    Middleware(#[from] MiddlewareError),

    #[error("Audit error: {0}")]
    Audit(#[from] AuditError),

    #[error("Shutdown error: {0}")]
    Shutdown(#[from] ShutdownError),

    #[error("Serialization error: {0}")]
    Serialize(#[from] SerializeError),

    #[error("Topic error: {0}")]
    Topic(#[from] TopicError),

    #[error("Handler error: {0}")]
    Handler(#[from] HandlerError),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Queue Send Error: {0}")]
    QueueSendError(String),

    #[error("Invalid Parameter: {0}")]
    InvalidParameter(String),

    #[error("Invalid Type: {0}")]
    InvalidData(String),

    #[error("Task Join Error: {0}")]
    TaskJoinError(String),

    #[error("Object already exists")]
    AlreadyInitialized,

    #[error("Error Time")]
    ErrorTime,

    #[error("Timeout: {0:?}")]
    Timeout(Duration),

    #[error("Worker Not Found: {0}")]
    WorkerNotFound(String),

    #[error("Unsupported: {0}")]
    Unsupported(String),

    #[error("Shutting down")]
    ShuttingDown,

    #[error("Other: {0}")]
    Other(String),
}
