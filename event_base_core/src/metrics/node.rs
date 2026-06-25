use crate::constant::SYSTEM_TOPIC_METRICS;
use crate::message::DeliveryMode::Standard;
use crate::message::{EMessage, MessageTopic};
use crate::queues::consumer_router::ConsumerRouter;
use crate::topic::TopicRouter;
use crate::{get_node_name, message};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use sysinfo::System;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct NodeMetrics {
    pub node_name: String,
    pub cpu_percent: Vec<f32>,
    pub memory_percent: f32,
    pub node_worker_count: usize,
    pub update_time: SystemTime,
}

#[derive(Clone)]
pub struct NodeCollector;

impl NodeCollector {
    pub async fn start(&self) {
        loop {
            let metrics = self.collect().await;
            let msg = EMessage::new(
                MessageTopic(SYSTEM_TOPIC_METRICS.parse().unwrap()),
                message::MessagePayload(serde_json::to_vec(&metrics).unwrap()),
                Standard,
                None,
            );
            TopicRouter::global()
                .send(SYSTEM_TOPIC_METRICS, msg, None, None)
                .await
                .expect("[NODE METRICS COLLECTOR]Fail to send node message");
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

        let memory_used_percent = (sys.used_memory() / sys.total_memory()) as f32;

        let node_worker_count = ConsumerRouter::global().get_all_workers().await.len();

        NodeMetrics {
            node_name: get_node_name(),
            cpu_percent: cpu_usage,
            memory_percent: memory_used_percent,
            node_worker_count,
            update_time: SystemTime::now(),
        }
    }
}
