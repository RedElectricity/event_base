use crate::audit::AuditWriter;
use crate::error::CoreError;
use crate::handler::{Ack, EHandler};
use crate::message::EMessage;
use crate::system_handlers::audit::AuditHandler;
use crate::trace::TraceRecord;
use async_trait::async_trait;
use std::sync::Arc;

pub struct SystemTraceHandler {
    collectors: Vec<Arc<dyn TraceCollector>>,
}

impl SystemTraceHandler {
    pub fn new(collectors: Vec<Arc<dyn TraceCollector>>) -> Self {
        Self { collectors }
    }
}

#[async_trait]
pub trait TraceCollector: Send + Sync {
    async fn collect(&self, record: &TraceRecord) -> Result<(), CoreError>;
}

#[async_trait]
impl EHandler for SystemTraceHandler {
    async fn handle(&self, msg: &EMessage) -> Ack {
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
