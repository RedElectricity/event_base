//! Handlers for worker discovery and heartbeat messages.
//!
//! The [`WorkerDiscoveryHandler`] processes worker registration messages and
//! adds them to the [`WorkerRegistry`](WorkerRegistry).
//! The [`WorkerHeartbeatHandler`] updates the last heartbeat timestamp of a worker.

use crate::constant::{SYSTEM_TOPIC_WORKER_DISCOVERY, SYSTEM_TOPIC_WORKER_HEARTBEAT};
use crate::handler::{Ack, EHandler};
use crate::message::DeliveryMode::Standard;
use crate::message::{EMessage, MessagePayload, MessageTopic};
use crate::queues::consumer_router::ConsumerRouter;
use crate::topic::TopicRouter;
use crate::worker_registry::{
    WorkerDiscoveryMessage, WorkerHeartbeatMessage, WorkerInfo, WorkerRegistry,
};
use std::time::{Duration, SystemTime};
use tokio::time::sleep;
use tracing::error;

/// Handler for worker discovery (registration) messages.
///
/// It deserializes a [`WorkerDiscoveryMessage`] and registers the worker in
/// the global [`WorkerRegistry`].
pub struct WorkerDiscoveryHandler {}

#[async_trait::async_trait]
impl EHandler for WorkerDiscoveryHandler {
    async fn handler(&self, msg: &EMessage) -> Ack {
        let info: WorkerDiscoveryMessage =
            match bincode::decode_from_slice::<WorkerDiscoveryMessage, _>(
                msg.payload.0.as_slice(),
                bincode::config::standard(),
            ) {
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

        if WorkerRegistry::global()
            .write()
            .await
            .register(worker)
            .await
            .is_err()
        {
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
            match bincode::decode_from_slice::<WorkerHeartbeatMessage, _>(
                msg.payload.0.as_slice(),
                bincode::config::standard(),
            ) {
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
            .write()
            .await
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

/// Cadence at which every node re‑announces heartbeats for its live workers
/// (consumed by the Host’s [`WorkerHeartbeatHandler`]).
pub const WORKER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// Cadence of the Host‑only stale sweep.
pub const WORKER_CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

/// A worker whose last heartbeat is older than this is considered dead and is
/// removed from the registry. Sized so that ~6 missed heartbeats tolerate a
/// busy runtime without evicting live workers.
pub const WORKER_STALE_TIMEOUT: Duration = Duration::from_secs(90);

/// Every Nth heartbeat cycle also re‑publishes the worker’s *discovery*
/// message, so a lost announcement (bounded queue full, node restarted)
/// self‑heals without waiting for the worker to be recreated.
pub const DISCOVERY_REANNOUNCE_EVERY: u64 = 10;

/// Periodic heartbeat loop — runs on **all** node roles (a single‑process
/// deployment is its own Host). For every live local worker it publishes a
/// `WorkerHeartbeatMessage` to `_system.worker_heartbeat`; the Host handler
/// refreshes that worker’s `last_heartbeat`, which keeps [`cleanup_stale`]
/// from evicting healthy workers. Every [`DISCOVERY_REANNOUNCE_EVERY`] cycles
/// it also re‑announces discovery for workers missing from the registry.
pub async fn run_worker_heartbeat_loop() {
    let mut cycle: u64 = 0;
    loop {
        sleep(WORKER_HEARTBEAT_INTERVAL).await;
        cycle += 1;
        heartbeat_once(cycle.is_multiple_of(DISCOVERY_REANNOUNCE_EVERY)).await;
    }
}

/// One pass of the heartbeat announcement (extracted so tests can drive it
/// without waiting out the interval). When `reannounce` is set, local workers
/// missing from the Host registry are re‑announced via discovery.
pub async fn heartbeat_once(reannounce: bool) {
    let Some(router) = TopicRouter::try_global() else {
        return;
    };
    // Snapshot local workers (names + topics) and drop the router guard
    // before sending: handlers and the recv loop acquire these globals
    // concurrently.
    let workers: Vec<(String, String)> = ConsumerRouter::global()
        .read()
        .await
        .get_all_workers()
        .await
        .iter()
        // Mirror announce_worker: `_system.*` workers are not in the
        // registry, so heartbeating them is pure chatter.
        .filter(|w| !w.topic.starts_with("_system."))
        .map(|w| (w.name.clone(), w.topic.clone()))
        .collect();
    // On re‑announce cycles, check which are missing from the Host
    // registry (a no‑op locally when this node IS the host — the
    // registry read is cheap and the insert idempotent).
    let missing: Vec<String> = if reannounce {
        let registered: Vec<String> = match WorkerRegistry::global()
            .read()
            .await
            .get_all_workers()
            .await
        {
            Ok(list) => list.into_iter().map(|i| i.worker_name).collect(),
            Err(_) => Vec::new(),
        };
        workers
            .iter()
            .filter(|(name, _)| !registered.contains(name))
            .map(|(name, _)| name.clone())
            .collect()
    } else {
        Vec::new()
    };
    let node = crate::try_get_node_name();
    for (worker_name, topic) in &workers {
        if reannounce && missing.contains(worker_name) {
            let discovery = WorkerDiscoveryMessage {
                worker_name: worker_name.clone(),
                topic: topic.clone(),
                started_at: SystemTime::now(),
            };
            let mut msg = EMessage::new(
                MessageTopic(SYSTEM_TOPIC_WORKER_DISCOVERY.to_string()),
                MessagePayload(
                    bincode::encode_to_vec(&discovery, bincode::config::standard())
                        .unwrap_or_default(),
                ),
                Standard,
                None,
            );
            msg.metadata.source = node.clone();
            if let Err(e) = router.read().await.send_system(msg, Some(true), None).await {
                error!("[HEARTBEAT] Failed to re-announce worker {worker_name}: {e}");
            }
        }
        let hb = WorkerHeartbeatMessage {
            worker_name: worker_name.clone(),
            timestamp: SystemTime::now(),
        };
        let mut msg = EMessage::new(
            MessageTopic(SYSTEM_TOPIC_WORKER_HEARTBEAT.to_string()),
            MessagePayload(
                bincode::encode_to_vec(&hb, bincode::config::standard()).unwrap_or_default(),
            ),
            Standard,
            None,
        );
        // `source` carries the emitting node id (wire shape unchanged).
        msg.metadata.source = node.clone();
        if let Err(e) = router.read().await.send_system(msg, Some(true), None).await {
            error!("[HEARTBEAT] Failed to send worker heartbeat: {e}");
        }
    }
}

/// Host‑only stale sweep: removes registry entries whose heartbeats lapsed
/// (dead nodes stop heartbeating) so broadcast fan‑out, `list_workers` and
/// shutdown accounting stop aiming at ghosts.
pub async fn run_registry_cleanup_loop() {
    loop {
        sleep(WORKER_CLEANUP_INTERVAL).await;
        match WorkerRegistry::global()
            .read()
            .await
            .cleanup_stale(WORKER_STALE_TIMEOUT)
            .await
        {
            Ok(removed) if !removed.is_empty() => {
                tracing::info!(
                    "[CLEANUP] Removed {} stale worker(s) from the registry",
                    removed.len()
                );
            }
            Ok(_) => {}
            Err(e) => error!("[CLEANUP] Stale sweep failed: {e}"),
        }
    }
}
