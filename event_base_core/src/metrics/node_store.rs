use crate::metrics::node::NodeMetrics;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;

static METRICS_STORE: OnceLock<Arc<MetricsStore>> = OnceLock::new();

pub struct MetricsStore {
    nodes: RwLock<HashMap<String, NodeMetrics>>,
}

impl MetricsStore {
    pub fn global() -> Arc<MetricsStore> {
        METRICS_STORE
            .get()
            .expect("MetricsStore not initialized")
            .clone()
    }

    pub async fn update(&self, metrics: NodeMetrics) {
        let mut nodes = self.nodes.write().await;
        if nodes.contains_key(&metrics.node_name.clone()) {
            let node_metrics = nodes.get_mut(&metrics.node_name.clone()).unwrap();
            *node_metrics = metrics.clone();
        }
        nodes.insert(metrics.node_name.clone(), metrics);
    }

    pub async fn get_all_nodes(&self) -> Vec<NodeMetrics> {
        let nodes = self.nodes.read().await;
        nodes.values().cloned().collect()
    }

    pub async fn get_node(&self, node_id: &str) -> Option<NodeMetrics> {
        let nodes = self.nodes.read().await;
        nodes.get(node_id).cloned()
    }
}
