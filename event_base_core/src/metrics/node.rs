//! Node‑level metrics collection and reporting.
//!
//! This module defines the [`NodeMetrics`] structure and a [`NodeCollector`]
//! that periodically gathers system information and publishes it to the
//! system metrics topic.

use crate::constant::SYSTEM_TOPIC_METRICS;
use crate::error::CoreError;
use crate::message::DeliveryMode::Standard;
use crate::message::{EMessage, MessageTopic};
use crate::queues::consumer_router::ConsumerRouter;
use crate::topic::TopicRouter;
use crate::{NodeType, get_node_name, get_node_type, message};
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use sysinfo::System;

/// Metrics describing the current state of a node.
#[derive(Clone, Serialize, Deserialize, Debug, Encode, Decode)]
pub struct NodeMetrics {
    /// Unique name of the node.
    pub node_name: String,
    /// Type of the node (e.g., Host, Worker).
    pub node_type: NodeType,
    /// CPU usage percentage per core.
    pub cpu_percent: Vec<f32>,
    /// Memory usage percentage (used / total * 100).
    pub memory_percent: f32,
    /// Number of workers currently active on this node.
    pub node_worker_count: usize,
    /// Timestamp when these metrics were collected.
    pub update_time: SystemTime,
}

/// A collector that periodically samples system metrics and publishes them.
#[derive(Clone)]
pub struct NodeCollector;

impl NodeCollector {
    /// Starts the collector loop.
    ///
    /// It runs indefinitely, collecting metrics every 30 seconds and sending
    /// a message to the `SYSTEM_TOPIC_METRICS` topic.
    ///
    /// # Errors
    /// Returns `CoreError` if sending the message fails.
    pub async fn start(&self) -> Result<(), CoreError> {
        loop {
            let metrics = self.collect().await;
            let msg = EMessage::new(
                MessageTopic(SYSTEM_TOPIC_METRICS.to_string()),
                message::MessagePayload(bincode::encode_to_vec(&metrics, bincode::config::standard()).map_err(|e| CoreError::Serialize(crate::error::serialize::SerializeError::SerializeError(e.to_string())))?),
                Standard,
                None,
            );
            TopicRouter::global()
                .read().await
                .send(SYSTEM_TOPIC_METRICS, msg, None, None)
                .await?;
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    }

    /// Collects the current system metrics.
    ///
    /// This includes CPU usage per core, memory usage, and the number of workers
    /// registered on this node (via `ConsumerRouter`).
    async fn collect(&self) -> NodeMetrics {
        let mut sys = System::new_all();
        sys.refresh_all();

        let mut cpu_usage: Vec<f32> = vec![];
        for cpu in sys.cpus() {
            cpu_usage.push(cpu.cpu_usage())
        }

        let memory_used_percent = (sys.used_memory() as f32 / sys.total_memory() as f32) * 100.0;

        let node_worker_count = ConsumerRouter::global().read().await.get_all_workers().await.len();

        NodeMetrics {
            node_name: get_node_name(),
            node_type: *get_node_type(),
            cpu_percent: cpu_usage,
            memory_percent: memory_used_percent,
            node_worker_count,
            update_time: SystemTime::now(),
        }
    }
}
