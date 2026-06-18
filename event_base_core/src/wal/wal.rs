use std::time::SystemTime;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::dead_letter::DeadReason;
use crate::error::CoreError;
use crate::message::{EMessage, MessageMetadata, MessagePayload, MessageTopic};

#[async_trait]
pub trait Wal: Send + Sync {
    async fn append(&mut self, record: WalRecord) -> Result<(), CoreError>;
    async fn update_state(&mut self, message_id: &str, status: WalRecordState) -> Result<(), CoreError>;
    async fn replay_pending(&mut self) -> Result<Vec<WalRecord>, CoreError>;
    async fn flush(&mut self) -> Result<(), CoreError>;

    async fn schedule(&self, record: WalRecord) -> Result<(), CoreError>;
    async fn fetch_ready(&self, now: SystemTime) -> Result<Vec<WalRecord>, CoreError>;
    async fn remove_scheduled(&self, msg_id: &str) -> Result<(), CoreError>;
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct WalRecord {
    pub record_id: u64,
    pub message: EMessage,
    pub status: WalRecordState,
    pub last_attempt_at: Option<SystemTime>,
    pub is_dead_letter: bool,
    pub dead_reason: Option<DeadReason>,
}

impl WalRecord {
    pub fn from_msg(msg: EMessage) -> Self {
        Self {
            record_id: 0,
            message: msg,
            status: WalRecordState::Pending,
            last_attempt_at: None,
            is_dead_letter: false,
            dead_reason: None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Copy, Encode, Decode)]
pub enum WalRecordState {
    Pending = 0,
    Processing = 1,
    Complete = 2,
    Failed = 3,
}