//! A store for the latest node metrics, keyed by node name.
//!
//! The [`MetricsStore`] holds the most recent [`NodeMetrics`] for each node
//! and provides methods to update, retrieve, and list them.

use crate::error::CoreError;
use crate::metrics::node::NodeMetrics;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;

static METRICS_STORE: OnceLock<Arc<MetricsStore>> = OnceLock::new();

/// A thread‑safe store for node metrics.
pub struct MetricsStore {
    nodes: RwLock<HashMap<String, NodeMetrics>>,
}

impl MetricsStore {
    /// Initializes the global metrics store with an empty map.
    ///
    /// # Errors
    /// Returns `CoreError::AlreadyInitialized` if called more than once.
    pub fn init() -> Result<(), CoreError> {
        let store: Arc<MetricsStore> = Arc::new(MetricsStore {
            nodes: RwLock::new(HashMap::new()),
        });
        METRICS_STORE
            .set(store)
            .map_err(|_| CoreError::AlreadyInitialized)?;
        Ok(())
    }

    /// Returns a reference to the global metrics store.
    ///
    /// # Panics
    /// Panics if the store has not been initialized.
    pub fn global() -> Arc<MetricsStore> {
        METRICS_STORE
            .get()
            .expect("MetricsStore not initialized")
            .clone()
    }

    /// Updates the metrics for a specific node, overwriting any existing entry.
    pub async fn update(&self, metrics: NodeMetrics) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(metrics.node_name.clone(), metrics);
    }

    /// Returns a vector containing metrics for all known nodes.
    pub async fn get_all_nodes(&self) -> Vec<NodeMetrics> {
        let nodes = self.nodes.read().await;
        nodes.values().cloned().collect()
    }

    /// Returns the metrics for a given node, if present.
    pub async fn get_node(&self, node_id: &str) -> Option<NodeMetrics> {
        let nodes = self.nodes.read().await;
        nodes.get(node_id).cloned()
    }
}
