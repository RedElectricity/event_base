use crate::constant::SYSTEM_TOPIC_WAL_SYNC;
use crate::error::CoreError;
use crate::message::DeliveryMode::Standard;
use crate::message::{EMessage, MessagePayload, MessageTopic};
use crate::topic::TopicRouter;
use crate::wal::wal::WalRecordState;
use crate::wal::wal::WalRecordState::{Complete, Failed, Pending, Processing};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSyncMessage {
    pub message_id: String,
    pub topic: String,
    pub worker_id: String,
    pub status: WalRecordState,
    pub attempts: u32,
    pub last_attempt_at: SystemTime,
    pub error: Option<String>,
    pub timestamp: SystemTime,
}

pub struct WalClient {
    worker_id: String,
}

impl WalClient {
    pub fn new(worker_id: String) -> Self {
        Self { worker_id }
    }

    async fn sync_status(
        &self,
        message_id: &str,
        topic: &str,
        status: WalRecordState,
        attempts: u32,
        error: Option<String>,
    ) -> Result<(), CoreError> {
        let sync_msg = WalSyncMessage {
            message_id: message_id.to_string(),
            topic: topic.to_string(),
            worker_id: self.worker_id.clone(),
            status,
            attempts,
            last_attempt_at: SystemTime::now(),
            error,
            timestamp: SystemTime::now(),
        };

        let payload = serde_json::to_vec(&sync_msg)?;

        let msg = EMessage::new(
            MessageTopic(SYSTEM_TOPIC_WAL_SYNC.to_string()),
            MessagePayload(payload),
            Standard,
            None,
        );
        TopicRouter::global()
            .send(SYSTEM_TOPIC_WAL_SYNC, msg, None, None)
            .await?;
        Ok(())
    }

    pub async fn mark_pending(&self, message_id: &str, topic: &str) -> Result<(), CoreError> {
        self.sync_status(message_id, topic, Pending, 0, None).await
    }

    pub async fn mark_processing(&self, message_id: &str, topic: &str) -> Result<(), CoreError> {
        self.sync_status(message_id, topic, Processing, 0, None)
            .await
    }

    pub async fn mark_complete(
        &self,
        message_id: &str,
        topic: &str,
        attempts: u32,
    ) -> Result<(), CoreError> {
        self.sync_status(message_id, topic, Complete, attempts, None)
            .await
    }

    pub async fn mark_dead_letter(
        &self,
        message_id: &str,
        topic: &str,
        attempts: u32,
        error: String,
    ) -> Result<(), CoreError> {
        self.sync_status(message_id, topic, Failed, attempts, Some(error))
            .await
    }
}
