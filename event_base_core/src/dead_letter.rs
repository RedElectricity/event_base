use crate::message::EMessage;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct DeadLetterMessage {
    pub original_message: EMessage,
    pub dead_reason: DeadReason,
    pub died_at: SystemTime,
    pub attempts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, Error)]
pub enum DeadReason {
    #[error("Max Retries exceeded")]
    MaxRetriesExceeded,

    #[error("Handler Explicit")]
    Explicit,

    #[error("Handler Timeout")]
    Timeout,

    #[error("NoHandler")]
    NoHandler,

    #[error("Handler Other Error: {0}")]
    Other(String),
}
