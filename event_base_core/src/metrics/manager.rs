//! Global metrics manager that aggregates audit records and exposes snapshots.
//!
//! The [`MetricsManager`] receives audit records, feeds them into an aggregator,
//! and can produce a combined snapshot of business metrics and node metrics.

use crate::audit::AuditRecord;
use crate::error::CoreError;
use crate::metrics::aggregator::MetricsAggregator;
use crate::metrics::node::NodeMetrics;
use crate::metrics::node_store::MetricsStore;
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;
use tokio::sync::Mutex;

static METRICS_MANAGER: OnceLock<Arc<MetricsManager>> = OnceLock::new();

/// The central manager for all metrics collection and snapshots.
pub struct MetricsManager {
    aggregator: Arc<Mutex<MetricsAggregator>>,
}

impl MetricsManager {
    /// Initializes the global metrics manager with a default (empty) aggregator.
    ///
    /// # Errors
    /// Returns `CoreError::AlreadyInitialized` if called more than once.
    pub fn init() -> Result<(), CoreError> {
        let manager = Arc::new(MetricsManager {
            aggregator: Arc::new(Mutex::new(MetricsAggregator {
                enqueued: Default::default(),
                completed: Default::default(),
                failed: Default::default(),
                retried: Default::default(),
                latency_sum: Default::default(),
            })),
        });
        METRICS_MANAGER
            .set(manager)
            .map_err(|_| CoreError::AlreadyInitialized)?;
        Ok(())
    }

    /// Returns a reference to the global metrics manager.
    ///
    /// # Panics
    /// Panics if the manager has not been initialized.
    pub fn global() -> Arc<MetricsManager> {
        METRICS_MANAGER
            .get()
            .expect("MetricsManager not initialized")
            .clone()
    }

    /// Feeds an audit record into the aggregator.
    ///
    /// This updates the internal business metrics (counts and latencies).
    pub async fn feed_audit(&self, record: &AuditRecord) {
        self.aggregator.lock().await.feed(record);
    }

    /// Captures a snapshot of all current metrics.
    ///
    /// This includes the business metrics from the aggregator and the latest
    /// node metrics from the [`MetricsStore`].
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

/// A complete snapshot of both business and node metrics at a given time.
pub struct MetricsSnapshot {
    /// Aggregated business metrics (per‑topic counts and latencies).
    pub business: MetricsAggregator,
    /// Latest metrics for all known nodes.
    pub nodes: Vec<NodeMetrics>,
    /// Timestamp when the snapshot was taken.
    pub timestamp: SystemTime,
}
