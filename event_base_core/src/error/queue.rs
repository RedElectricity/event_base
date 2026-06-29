//! Queue‑related errors.
//!
//! These errors occur when interacting with the underlying message queue,
//! such as when sending, receiving, or claiming messages.

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    /// The queue is full and cannot accept more messages.
    #[error("Queue is full")]
    Full,

    /// The queue has been closed and no further operations are allowed.
    #[error("Queue is closed")]
    Closed,

    /// A send operation timed out.
    #[error("Send timeout")]
    Timeout,

    /// A send operation failed for a reason other than full or timeout.
    #[error("Send error: {0}")]
    Send(String),

    /// A receive operation failed.
    #[error("Receive error: {0}")]
    Receive(String),
}
