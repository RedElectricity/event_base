use crate::error::CoreError;
use crate::metrics::node::NodeMetrics;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;

static METRICS_STORE: OnceLock<Arc<MetricsStore>> = OnceLock::new();

pub struct MetricsStore {
    nodes: RwLock<HashMap<String, NodeMetrics>>,
}

impl MetricsStore {
    pub fn init() -> Result<(), CoreError> {
        let store: Arc<MetricsStore> = Arc::new(MetricsStore {
            nodes: RwLock::new(HashMap::new()),
        });
        METRICS_STORE
            .set(store)
            .map_err(|_| CoreError::AlreadyInitialized)?;
        Ok(())
    }
    pub fn global() -> Arc<MetricsStore> {
        METRICS_STORE
            .get()
            .expect("MetricsStore not initialized")
            .clone()
    }

    pub async fn update(&self, metrics: NodeMetrics) {
        let mut nodes = self.nodes.write().await;
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
