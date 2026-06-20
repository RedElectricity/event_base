use crate::handler::{Ack, EHandler};
use crate::message::EMessage;
use crate::wal::sync::WalSyncMessage;
use crate::wal::wal::Wal;
use async_trait::async_trait;
use std::sync::Arc;

pub struct WalSyncHandler {
    wal: Arc<tokio::sync::Mutex<dyn Wal>>,
}

impl WalSyncHandler {
    pub fn new(wal: Arc<tokio::sync::Mutex<dyn Wal>>) -> Self {
        Self { wal }
    }
}

#[async_trait]
impl EHandler for WalSyncHandler {
    async fn handle(&self, msg: &EMessage) -> Ack {
        let sync: WalSyncMessage = match serde_json::from_slice(&msg.payload.0) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[SYSTEM] Failed to deserialize WalSyncMessage: {}", e);
                return Ack::Ack;
            }
        };

        let mut wal = self.wal.lock().await;

        if let Err(e) = wal.update_state(&sync.message_id, sync.status).await {
            eprintln!(
                "[SYSTEM] Failed to update WAL status for {}: {}",
                sync.message_id, e
            );
        }

        Ack::Ack
    }
}
