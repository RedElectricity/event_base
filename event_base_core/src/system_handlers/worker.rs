//! Handlers for worker discovery and heartbeat messages.
//!
//! The [`WorkerDiscoveryHandler`] processes worker registration messages and
//! adds them to the [`WorkerRegistry`](WorkerRegistry).
//! The [`WorkerHeartbeatHandler`] updates the last heartbeat timestamp of a worker.

use crate::handler::{Ack, EHandler};
use crate::message::EMessage;
use crate::worker_registry::{
    WorkerDiscoveryMessage, WorkerHeartbeatMessage, WorkerInfo, WorkerRegistry,
};
use std::time::SystemTime;

/// Handler for worker discovery (registration) messages.
///
/// It deserializes a [`WorkerDiscoveryMessage`] and registers the worker in
/// the global [`WorkerRegistry`].
pub struct WorkerDiscoveryHandler {}

#[async_trait::async_trait]
impl EHandler for WorkerDiscoveryHandler {
    async fn handler(&self, msg: &EMessage) -> Ack {
        let info: WorkerDiscoveryMessage =
            match bincode::decode_from_slice::<WorkerDiscoveryMessage, _>(msg.payload.0.as_slice(), bincode::config::standard()) {
                Ok((msg, _)) => msg,
                Err(e) => {
                    eprintln!(
                        "[WORKER DISCOVERY]Failed to deserialize WorkerDiscoveryMessage: {}",
                        e
                    );
                    return Ack::Ack;
                }
            };
        let worker = WorkerInfo {
            worker_name: info.worker_name,
            topic: info.topic,
            last_heartbeat: SystemTime::now(),
        };

        if let Err(_) = WorkerRegistry::global().register(worker).await {
            eprintln!("[WORKER DISCOVERY]register worker failed")
        }
        Ack::Ack
    }
}

/// Handler for worker heartbeat messages.
///
/// It deserializes a [`WorkerHeartbeatMessage`] and updates the heartbeat
/// timestamp of the corresponding worker in the [`WorkerRegistry`].
pub struct WorkerHeartbeatHandler {}

#[async_trait::async_trait]
impl EHandler for WorkerHeartbeatHandler {
    async fn handler(&self, msg: &EMessage) -> Ack {
        let heartbeat: WorkerHeartbeatMessage =
            match bincode::decode_from_slice::<WorkerHeartbeatMessage, _>(msg.payload.0.as_slice(), bincode::config::standard()) {
                Ok((msg, _)) => msg,
                Err(e) => {
                    eprintln!(
                        "[WORKER HEARTBEAT]Failed to deserialize WorkerHeartbeatMessage: {}",
                        e
                    );
                    return Ack::Ack;
                }
            };

        if let Err(e) = WorkerRegistry::global()
            .heartbeat(&heartbeat.worker_name)
            .await
        {
            eprintln!(
                "[WORKER HEARTBEAT] Failed to update heartbeat for {}: {}",
                heartbeat.worker_name, e
            );
        }
        Ack::Ack
    }
}
