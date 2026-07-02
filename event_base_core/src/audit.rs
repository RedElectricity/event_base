//! Audit logging for message processing lifecycle.
//!
//! This module provides types and a global manager for recording audit events
//! (enqueue, processing start/end, retries, dead-lettering). Records are stored
//! in a ring buffer and can be written to one or more external writers.

use crate::error::CoreError;
use async_trait::async_trait;
use futures::future::join_all;
use ringbuf::HeapRb;
use ringbuf::consumer::Consumer;
use ringbuf::traits::RingBuffer;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

/// A single audit record describing an event in the message lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct AuditRecord {
    /// Unique identifier of the message being audited.
    pub message_id: String,
    /// Topic of the message.
    pub topic: String,
    /// Type of audit event (e.g., enqueued, started, completed).
    pub event_type: AuditEventType,
    /// Optional ID of the worker that processed the message.
    pub worker_id: Option<String>,
    /// Timestamp when this record was created.
    pub timestamp: SystemTime,
    /// Result of the operation (start, success, failure, etc.).
    pub result: AuditResult,
    /// Optional error message if the operation failed.
    pub error: Option<String>,
    /// Duration of processing (if applicable).
    pub duration: Option<Duration>,
}

/// Classification of audit events.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum AuditEventType {
    /// Message was enqueued.
    Enqueued,
    /// Processing of the message has started.
    ProcessingStarted,
    /// Processing completed successfully.
    ProcessingCompleted,
    /// Message is being retried.
    Retry,
    /// Message was moved to the dead letter queue.
    DeadLettered,
}

/// Outcome of a processing attempt.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum AuditResult {
    /// Processing has begun (no final outcome yet).
    Start,
    /// Processing succeeded.
    Success,
    /// Processing failed.
    Failure,
    /// Message will be retried.
    Retry,
    /// Message was dead-lettered.
    Dead,
}

/// Trait for writing audit records to an external sink.
#[async_trait]
pub trait AuditWriter: Send + Sync {
    /// Writes a single audit record.
    ///
    /// # Errors
    /// Returns `CoreError` if the write fails.
    async fn write(&self, record: &AuditRecord) -> Result<(), CoreError>;
}

static AUDIT_MANAGER: OnceLock<Arc<AuditManager>> = OnceLock::new();

/// Central audit manager that buffers records and forwards them to writers.
pub struct AuditManager {
    /// Ring buffer holding recent audit records.
    buffer: Arc<RwLock<HeapRb<AuditRecord>>>,
    /// List of registered audit writers.
    pub writers: Vec<Arc<dyn AuditWriter>>,
}

impl AuditManager {
    /// Initializes the global audit manager with a buffer of the given capacity.
    ///
    /// # Errors
    /// Returns `CoreError::AlreadyInitialized` if called more than once.
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

    /// Replaces the list of writers with the given set.
    pub fn with_writers(&mut self, writers: Vec<Arc<dyn AuditWriter>>) {
        self.writers = writers
    }

    /// Returns a reference to the global audit manager.
    ///
    /// # Panics
    /// Panics if the manager has not been initialized.
    pub fn global() -> Arc<AuditManager> {
        AUDIT_MANAGER
            .get()
            .expect("AuditManager not initialized")
            .clone()
    }

    /// Records an audit event.
    ///
    /// The record is pushed into the ring buffer (overwriting the oldest if full)
    /// and then asynchronously sent to all registered writers.
    ///
    /// # Errors
    /// Returns `CoreError` if any writer fails (though all writers are attempted).
    pub async fn record(&self, record: AuditRecord) -> Result<(), CoreError> {
        let mut buffer = self.buffer.write().await;
        buffer.push_overwrite(record.clone());
        drop(buffer);

        let _ = join_all(self.writers.iter().map(|w| w.write(&record))).await;

        Ok(())
    }

    /// Returns the most recent `count` audit records from the buffer.
    pub async fn get_recent(&self, count: usize) -> Vec<AuditRecord> {
        let buffer = self.buffer.read().await;
        buffer.iter().rev().take(count).cloned().collect()
    }
}
