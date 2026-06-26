use crate::constant::SYSTEM_TOPIC_METRICS;
use crate::error::CoreError;
use crate::message::DeliveryMode::Standard;
use crate::message::{EMessage, MessageTopic};
use crate::queues::consumer_router::ConsumerRouter;
use crate::topic::TopicRouter;
use crate::{NodeType, get_node_name, get_node_type, message};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use sysinfo::System;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct NodeMetrics {
    pub node_name: String,
    pub node_type: NodeType,
    pub cpu_percent: Vec<f32>,
    pub memory_percent: f32,
    pub node_worker_count: usize,
    pub update_time: SystemTime,
}

#[derive(Clone)]
pub struct NodeCollector;

impl NodeCollector {
    pub async fn start(&self) -> Result<(), CoreError> {
        loop {
            let metrics = self.collect().await;
            let msg = EMessage::new(
                MessageTopic(SYSTEM_TOPIC_METRICS.to_string()),
                message::MessagePayload(serde_json::to_vec(&metrics)?),
                Standard,
                None,
            );
            TopicRouter::global()
                .send(SYSTEM_TOPIC_METRICS, msg, None, None)
                .await?;
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    }

    async fn collect(&self) -> NodeMetrics {
        let mut sys = System::new_all();
        sys.refresh_all();

        let mut cpu_usage: Vec<f32> = vec![];
        for cpu in sys.cpus() {
            cpu_usage.push(cpu.cpu_usage())
        }

        let memory_used_percent = (sys.used_memory() as f32 / sys.total_memory() as f32) * 100.0;

        let node_worker_count = ConsumerRouter::global().get_all_workers().await.len();

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
