use crate::handler::{Ack, EHandler};
use crate::message::EMessage;
use crate::wal::sync::WalSyncMessage;
use crate::wal::wal::Wal;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct WalSyncHandler {
    wal: Arc<RwLock<dyn Wal>>,
}

impl WalSyncHandler {
    pub fn new(wal: Arc<RwLock<dyn Wal>>) -> Self {
        Self { wal }
    }
}

#[async_trait]
impl EHandler for WalSyncHandler {
    async fn handler(&self, msg: &EMessage) -> Ack {
        let sync: WalSyncMessage = match serde_json::from_slice(&msg.payload.0) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[SYSTEM] Failed to deserialize Wal Sync Message: {}", e);
                return Ack::Ack;
            }
        };

        let mut wal = self.wal.write().await;

        if let Err(e) = wal.update_state(&sync.message_id, sync.status).await {
            eprintln!(
                "[SYSTEM] Failed to update WAL status for {}: {}",
                sync.message_id, e
            );
        }

        if let Err(_) = wal.flush().await {
            eprintln!("[SYSTEM] Failed to flush WAL");
        }

        Ack::Ack
    }
}
