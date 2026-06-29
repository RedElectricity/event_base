//! Handlers for shutdown commands and acknowledgments.
//!
//! The [`ShutdownHandler`] receives `ShutdownCommand` messages and invokes
//! the appropriate shutdown strategy. The [`ShutdownAckHandler`] processes
//! acknowledgment messages from workers that have completed shutdown.

use crate::handler::{Ack, EHandler};
use crate::message::EMessage;
use crate::shutdown::messages::{ShutdownAck, ShutdownCommand, ShutdownStrategy};
use crate::shutdown::methods::{
    graceful_shutdown, shutdown_all_workers_two_stage, shutdown_batched, shutdown_force,
    shutdown_idle_only, shutdown_timeout,
};
use crate::worker_registry::WorkerRegistry;
use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::error;

/// Handler for shutdown command messages.
///
/// It deserializes a [`ShutdownCommand`] and executes the specified strategy,
/// using the provided broadcast sender to signal workers when needed.
pub struct ShutdownHandler {
    /// Broadcast sender used to signal workers for two‑stage shutdown.
    pub(crate) shutdown_tx: broadcast::Sender<()>,
}

#[async_trait]
impl EHandler for ShutdownHandler {
    async fn handler(&self, msg: &EMessage) -> Ack {
        let info: ShutdownCommand =
            match serde_json::from_slice::<ShutdownCommand>(msg.payload.0.as_slice()) {
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
                if let Err(_) = shutdown_all_workers_two_stage(
                    self.shutdown_tx.clone(),
                    Duration::from_secs(force_timeout_secs),
                    Duration::from_millis(poll_interval_ms),
                )
                .await
                {
                    eprintln!("[SHUTDOWN] Fail to shutdown all workers two stage")
                }
            }
            ShutdownStrategy::Graceful {
                worker_name,
                poll_interval_ms,
            } => {
                let result =
                    graceful_shutdown(&*worker_name, Duration::from_millis(poll_interval_ms)).await;
                if let Err(e) = result {
                    eprintln!("[SHUTDOWN]Failed to grace ShutdownCommand: {}", e);
                    return Ack::Ack;
                }
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
        }
        Ack::Ack
    }
}

/// Handler for shutdown acknowledgment messages.
///
/// It deserializes [`ShutdownAck`] and unregisters the worker from the
/// [`WorkerRegistry`](WorkerRegistry).
pub struct ShutdownAckHandler;

#[async_trait]
impl EHandler for ShutdownAckHandler {
    async fn handler(&self, msg: &EMessage) -> Ack {
        let ack = serde_json::from_slice::<ShutdownAck>(&msg.payload.0);

        if let Ok(ack) = ack {
            WorkerRegistry::global()
                .unregister(&ack.worker_name)
                .await
                .unwrap_or_else(|_| {
                    error!(
                        "[SHUTDOWN ACK]Failed to unregister worker: {}",
                        ack.worker_name
                    )
                });

            tracing::info!("Worker {} shutdown confirmed", ack.worker_name);
        }

        Ack::Ack
    }
}
