use crate::audit::AuditRecord;
use crate::metrics::aggregator::MetricsAggregator;
use crate::metrics::node::NodeMetrics;
use crate::metrics::node_store::MetricsStore;
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;
use tokio::sync::Mutex;

static METRICS_MANAGER: OnceLock<Arc<MetricsManager>> = OnceLock::new();

pub struct MetricsManager {
    aggregator: Arc<Mutex<MetricsAggregator>>,
}

impl MetricsManager {
    pub fn global() -> &'static MetricsManager {
        METRICS_MANAGER
            .get()
            .expect("MetricsManager not initialized")
    }

    pub async fn feed_audit(&self, record: &AuditRecord) {
        self.aggregator.lock().await.feed(record);
    }

    pub async fn snapshot(&self) -> MetricsSnapshot {
        let business = self.aggregator.lock().await.snapshot();
        let nodes = MetricsStore::global().get_all_nodes().await;
        MetricsSnapshot {
            business: business.clone(),
            nodes,
            timestamp: SystemTime::now(),
        }
    }
}

pub struct MetricsSnapshot {
    pub business: MetricsAggregator,
    pub nodes: Vec<NodeMetrics>,
    pub timestamp: SystemTime,
}
