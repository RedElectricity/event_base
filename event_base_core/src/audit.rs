use crate::error::CoreError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRecord {
    pub message_id: String,
    pub topic: String,
    pub event_type: AuditEventType,
    pub worker_id: Option<String>,
    pub timestamp: SystemTime,
    pub result: AuditResult,
    pub error: Option<String>,
    pub duration: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuditEventType {
    Enqueued,
    ProcessingStarted,
    ProcessingCompleted,
    ProcessingFailed,
    Retry,
    DeadLettered,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuditResult {
    Start,
    Success,
    Failure,
    Retry,
    Dead,
}

#[async_trait]
pub trait AuditWriter: Send + Sync {
    async fn write(&self, record: &AuditRecord) -> Result<(), CoreError>;
}
