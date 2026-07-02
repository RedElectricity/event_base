//! Handler for WAL synchronization messages.
//!
//! The [`WalSyncHandler`] processes messages that update the status of a message in the WAL.

use crate::handler::{Ack, EHandler};
use crate::message::EMessage;
use crate::wal::sync::WalSyncMessage;
use crate::wal::wal::Wal;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A handler that applies WAL status updates.
///
/// It deserializes a [`WalSyncMessage`] and updates the message state in the
/// WAL via [`Wal::update_state`], then flushes the WAL.
pub struct WalSyncHandler {
    wal: Arc<RwLock<dyn Wal>>,
}

impl WalSyncHandler {
    /// Creates a new handler with the given WAL instance.
    pub fn new(wal: Arc<RwLock<dyn Wal>>) -> Self {
        Self { wal }
    }
}

#[async_trait]
impl EHandler for WalSyncHandler {
    async fn handler(&self, msg: &EMessage) -> Ack {
        let sync: WalSyncMessage = match bincode::decode_from_slice(&msg.payload.0, bincode::config::standard()) {
            Ok((s, _)) => s,
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
