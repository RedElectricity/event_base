//! Dead letter messages and reasons.
//!
//! This module defines the structure of a dead letter message, which wraps an
//! original message along with the reason it was dead‑lettered and metadata
//! such as the timestamp and attempt count. The [`DeadReason`] enumeration
//! categorizes why a message could not be processed successfully.

use crate::message::EMessage;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use thiserror::Error;

/// A message that has been moved to the dead letter queue.
///
/// It contains the original message, the reason for failure, the time it was
/// dead‑lettered, and the number of attempts made.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct DeadLetterMessage {
    /// The original message that failed processing.
    pub original_message: EMessage,
    /// The reason the message was dead‑lettered.
    pub dead_reason: DeadReason,
    /// Timestamp when the message was moved to the dead letter queue.
    pub died_at: SystemTime,
    /// Number of processing attempts before it was dead‑lettered.
    pub attempts: u32,
}

/// The reason why a message was moved to the dead letter queue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode, Error)]
pub enum DeadReason {
    /// The message exceeded its maximum allowed retry count.
    #[error("Max Retries exceeded")]
    MaxRetriesExceeded,

    /// The handler explicitly returned a `Dead` acknowledgment.
    #[error("Handler Explicit")]
    Explicit,

    /// The handler timed out while processing the message.
    #[error("Handler Timeout")]
    Timeout,

    /// No handler was registered for the message's topic.
    #[error("NoHandler")]
    NoHandler,

    /// A generic error occurred in the handler, with a descriptive message.
    #[error("Handler Other Error: {0}")]
    Other(String),
}
