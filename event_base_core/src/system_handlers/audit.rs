use crate::audit::{AuditManager, AuditRecord};
use crate::handler::{Ack, EHandler};
use crate::message::EMessage;
use async_trait::async_trait;

pub struct AuditHandler {}
#[async_trait]
impl EHandler for AuditHandler {
    async fn handler(&self, msg: &EMessage) -> Ack {
        let record: AuditRecord = match serde_json::from_slice(&msg.payload.0) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed to deserialize audit record: {}", e);
                return Ack::Ack;
            }
        };

        let audit_mgr = AuditManager::global();

        if let Err(e) = audit_mgr.record(record.clone()).await {
            eprintln!(
                "[AUDIT_ERROR] Audit writer failed for msg {}: {}",
                record.message_id, e
            );
        }

        Ack::Ack
    }
}
