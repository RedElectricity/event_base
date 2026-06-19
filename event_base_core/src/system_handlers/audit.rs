use crate::audit::{AuditRecord, AuditWriter};
use crate::handler::{Ack, EHandler};
use crate::message::EMessage;
use async_trait::async_trait;
use std::sync::Arc;

pub struct SystemAuditHandler {
    writers: Vec<Arc<dyn AuditWriter>>,
}

impl SystemAuditHandler {
    pub fn new(writers: Vec<Arc<dyn AuditWriter>>) -> Self {
        Self { writers }
    }
}

#[async_trait]
impl EHandler for SystemAuditHandler {
    async fn handle(&self, msg: &EMessage) -> Ack {
        let record: AuditRecord = match serde_json::from_slice(&msg.payload.0) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed to deserialize audit record: {}", e);
                return Ack::Ack;
            }
        };

        for writer in &self.writers {
            if let Err(e) = writer.write(&record).await {
                eprintln!(
                    "[AUDIT_ERROR] Audit writer failed for msg {}: {}",
                    record.message_id, e
                );
            }
        }

        Ack::Ack
    }
}
