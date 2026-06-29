//! Synchronization of WAL states between workers and the host.
//!
//! This module provides a client (`WalClient`) that workers use to send
//! status updates (pending, processing, complete, failed) for messages
//! to the host via the `SYSTEM_TOPIC_WAL_SYNC` topic.

use crate::constant::SYSTEM_TOPIC_WAL_SYNC;
use crate::error::CoreError;
use crate::message::DeliveryMode::Standard;
use crate::message::{EMessage, MessagePayload, MessageTopic};
use crate::topic::TopicRouter;
use crate::wal::wal::WalRecordState;
use crate::wal::wal::WalRecordState::{Complete, Failed, Pending, Processing};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// A message sent from a worker to update the status of a message in the WAL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSyncMessage {
    /// ID of the message whose status is being updated.
    pub message_id: String,
    /// Topic of the message.
    pub topic: String,
    /// ID of the worker sending the update.
    pub worker_id: String,
    /// New status of the message.
    pub status: WalRecordState,
    /// Current number of processing attempts.
    pub attempts: u32,
    /// Timestamp of the last attempt.
    pub last_attempt_at: SystemTime,
    /// Optional error message (if status is `Failed`).
    pub error: Option<String>,
    /// Timestamp when this sync message was created.
    pub timestamp: SystemTime,
}

/// A client used by workers to synchronize WAL state with the host.
///
/// It sends `WalSyncMessage` messages via the global `TopicRouter` for each
/// state transition of a message.
pub struct WalClient {
    worker_id: String,
}

impl WalClient {
    /// Creates a new `WalClient` with the given worker identifier.
    pub fn new(worker_id: String) -> Self {
        Self { worker_id }
    }

    /// Internal helper to send a sync message with the given status.
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

    /// Marks a message as pending (ready for processing).
    pub async fn mark_pending(&self, message_id: &str, topic: &str) -> Result<(), CoreError> {
        self.sync_status(message_id, topic, Pending, 0, None).await
    }

    /// Marks a message as being processed.
    pub async fn mark_processing(&self, message_id: &str, topic: &str) -> Result<(), CoreError> {
        self.sync_status(message_id, topic, Processing, 0, None)
            .await
    }

    /// Marks a message as successfully completed.
    ///
    /// # Arguments
    /// * `message_id` - The ID of the message.
    /// * `topic` - The topic of the message.
    /// * `attempts` - The number of attempts it took to complete.
    pub async fn mark_complete(
        &self,
        message_id: &str,
        topic: &str,
        attempts: u32,
    ) -> Result<(), CoreError> {
        self.sync_status(message_id, topic, Complete, attempts, None)
            .await
    }

    /// Marks a message as failed and moved to the dead letter queue.
    ///
    /// # Arguments
    /// * `message_id` - The ID of the message.
    /// * `topic` - The topic of the message.
    /// * `attempts` - The number of attempts made.
    /// * `error` - The error reason.
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
