//! Handler for trace records and the `TraceCollector` trait.
//!
//! The [`SystemTraceHandler`] deserializes incoming trace records and forwards
//! them to a list of configured [`TraceCollector`] implementations.

use crate::error::CoreError;
use crate::handler::{Ack, EHandler};
use crate::message::EMessage;
use crate::trace::TraceRecord;
use async_trait::async_trait;
use std::sync::Arc;

/// A handler that processes trace records.
///
/// It deserializes the payload as a [`TraceRecord`] and calls each registered
/// [`TraceCollector`] with the record. Errors from collectors are logged but
/// do not affect the acknowledgment (the message is always acked).
pub struct SystemTraceHandler {
    collectors: Vec<Arc<dyn TraceCollector>>,
}

impl SystemTraceHandler {
    /// Creates a new handler with the given collectors.
    pub fn new(collectors: Vec<Arc<dyn TraceCollector>>) -> Self {
        Self { collectors }
    }
}

/// A collector that can process a trace record.
///
/// This trait is typically implemented to forward traces to external systems
/// (e.g., Elasticsearch, Jaeger, or a file).
#[async_trait]
pub trait TraceCollector: Send + Sync {
    /// Processes a single trace record.
    ///
    /// # Errors
    /// Returns `CoreError` if the processing fails.
    async fn collect(&self, record: &TraceRecord) -> Result<(), CoreError>;
}

#[async_trait]
impl EHandler for SystemTraceHandler {
    async fn handler(&self, msg: &EMessage) -> Ack {
        let record: TraceRecord = match serde_json::from_slice(&msg.payload.0) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[TRACE_ERROR] Failed to deserialize trace record: {}", e);
                return Ack::Ack;
            }
        };

        for collector in &self.collectors {
            if let Err(e) = collector.collect(&record).await {
                eprintln!("[TRACE_ERROR] Trace collector failed: {}", e);
            }
        }

        Ack::Ack
    }
}
