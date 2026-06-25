use crate::handler::{Ack, EHandler};
use crate::message::EMessage;
use crate::shutdown::messages::{ShutdownAck, ShutdownCommand, ShutdownStrategy};
use crate::shutdown::methods::{
    shutdown_all_workers_two_stage, shutdown_batched, shutdown_force, shutdown_idle_only,
    shutdown_timeout,
};
use crate::worker_registry::WorkerRegistry;
use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::broadcast;

pub struct ShutdownHandler {
    pub(crate) shutdown_tx: broadcast::Sender<()>,
}

#[async_trait]
impl EHandler for ShutdownHandler {
    async fn handle(&self, msg: &EMessage) -> Ack {
        let info: ShutdownCommand =
            match serde_json::from_slice::<ShutdownCommand>(&msg.payload.0.as_slice()) {
                Ok(msg) => msg,
                Err(e) => {
                    eprintln!("[SHUTDOWN]Failed to deserialize ShutdownCommand: {}", e);
                    return Ack::Ack;
                }
            };

        match info.strategy {
            ShutdownStrategy::TwoStage {
                poll_interval_ms,
                force_timeout_secs,
            } => {
                shutdown_all_workers_two_stage(
                    self.shutdown_tx.clone(),
                    Duration::from_millis(poll_interval_ms),
                    Duration::from_millis(force_timeout_secs),
                )
                .await
                .expect("[SHUTDOWN] Fail to shutdown all workers two stage");
            }
            ShutdownStrategy::Timeout { total_timeout_secs } => {
                shutdown_timeout(Duration::from_secs(total_timeout_secs)).await;
            }
            ShutdownStrategy::Force => shutdown_force().await,
            ShutdownStrategy::StateBasedIdle => shutdown_idle_only().await,
            ShutdownStrategy::Batched {
                batch_size,
                interval_ms,
            } => {
                shutdown_batched(batch_size, Duration::from_millis(interval_ms)).await;
            }
            _ => unreachable!(),
        }

        Ack::Ack
    }
}

pub struct ShutdownAckHandler;

#[async_trait]
impl EHandler for ShutdownAckHandler {
    async fn handle(&self, msg: &EMessage) -> Ack {
        let ack: ShutdownAck = serde_json::from_slice(&msg.payload.0)
            .map_err(|e| tracing::error!("[SHUTDOWN ACK]Failed to deserialize Shutdown Ack: {}", e))
            .unwrap();

        // 从 WR 删除 Worker
        WorkerRegistry::global()
            .unregister(&ack.worker_name)
            .await
            .expect(
                format!(
                    "[SHUTDOWN ACK]Failed to unregister worker: {}",
                    &ack.worker_name
                )
                .as_str(),
            );

        tracing::info!("Worker {} shutdown confirmed", ack.worker_name);
        Ack::Ack
    }
}
