use crate::handler::{Ack, EHandler};
use crate::message::EMessage;
use crate::worker_registry::{
    WorkerDiscoveryMessage, WorkerHeartbeatMessage, WorkerInfo, WorkerRegistry,
};
use std::time::SystemTime;

pub struct WorkerDiscoveryHandler {}

#[async_trait::async_trait]
impl EHandler for WorkerDiscoveryHandler {
    async fn handler(&self, msg: &EMessage) -> Ack {
        let info: WorkerDiscoveryMessage =
            match serde_json::from_slice::<WorkerDiscoveryMessage>(msg.payload.0.as_slice()) {
                Ok(msg) => msg,
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

pub struct WorkerHeartbeatHandler {}

#[async_trait::async_trait]
impl EHandler for WorkerHeartbeatHandler {
    async fn handler(&self, msg: &EMessage) -> Ack {
        let heartbeat: WorkerHeartbeatMessage =
            match serde_json::from_slice::<WorkerHeartbeatMessage>(msg.payload.0.as_slice()) {
                Ok(msg) => msg,
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
