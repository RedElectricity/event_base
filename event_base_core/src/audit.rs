use crate::error::CoreError;
use async_trait::async_trait;
use ringbuf::HeapRb;
use ringbuf::consumer::Consumer;
use ringbuf::traits::RingBuffer;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};
use futures::future::join_all;
use tokio::sync::RwLock;

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

static AUDIT_MANAGER: OnceLock<Arc<AuditManager>> = OnceLock::new();

pub struct AuditManager {
    buffer: Arc<RwLock<HeapRb<AuditRecord>>>,
    pub writers: Vec<Arc<dyn AuditWriter>>,
}

impl AuditManager {
    pub fn init(capacity: usize) -> Result<(), CoreError> {
        let audit = AuditManager {
            buffer: Arc::new(RwLock::new(HeapRb::new(capacity))),
            writers: Vec::new(),
        };

        AUDIT_MANAGER
            .set(Arc::from(audit))
            .map_err(|_| CoreError::AlreadyInitialized)?;
        Ok(())
    }

    pub fn with_writers(&mut self, writers: Vec<Arc<dyn AuditWriter>>) {
        self.writers = writers
    }

    pub fn global() -> Arc<AuditManager> {
        AUDIT_MANAGER
            .get()
            .expect("AuditManager not initialized")
            .clone()
    }

    pub async fn record(&self, record: AuditRecord) -> Result<(), CoreError> {
        let mut buffer = self.buffer.write().await;
        buffer.push_overwrite(record.clone());
        drop(buffer);

        let _ = join_all(self.writers.iter().map(|w| w.write(&record))).await;

        Ok(())
    }

    pub async fn get_recent(&self, count: usize) -> Vec<AuditRecord> {
        let buffer = self.buffer.read().await;
        buffer.iter().rev().take(count).cloned().collect()
    }
}