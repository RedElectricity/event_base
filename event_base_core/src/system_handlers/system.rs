//! Builder for registering system‑level handlers.
//!
//! The [`SystemHandlerBuilder`] collects necessary dependencies (WAL, shutdown
//! sender, trace collectors) and registers all built‑in system handlers to
//! the [`ConsumerRouter`](ConsumerRouter).
//! Registration differs between Host and Worker node types.

use crate::audit::AuditManager;
use crate::constant::{
    SYSTEM_TOPIC_AUDIT, SYSTEM_TOPIC_METRICS, SYSTEM_TOPIC_SHUTDOWN, SYSTEM_TOPIC_SHUTDOWN_ACK,
    SYSTEM_TOPIC_TOPIC_DISCOVERY, SYSTEM_TOPIC_TOPIC_SYNC, SYSTEM_TOPIC_TRACE,
    SYSTEM_TOPIC_WAL_SYNC, SYSTEM_TOPIC_WORKER_DISCOVERY, SYSTEM_TOPIC_WORKER_HEARTBEAT,
};
use crate::error::CoreError;
use crate::metrics::manager::MetricsManager;
use crate::metrics::node::NodeCollector;
use crate::metrics::node_store::MetricsStore;
use crate::queues::consumer_router::ConsumerRouter;
use crate::shutdown::ShutdownSender;
use crate::system_handlers::audit::AuditHandler;
use crate::system_handlers::metrics::MetricsHandler;
use crate::system_handlers::shutdown::ShutdownAckHandler;
use crate::system_handlers::shutdown::ShutdownHandler;
use crate::system_handlers::topic::{TopicDiscovery, TopicSync};
use crate::system_handlers::trace::{SystemTraceHandler, TraceCollector};
use crate::system_handlers::wal::WalSyncHandler;
use crate::system_handlers::worker::{WorkerDiscoveryHandler, WorkerHeartbeatHandler};
use crate::wal::wal::Wal;
use crate::{NodeType, get_node_type};
use std::sync::Arc;
use tokio::sync::RwLock;

/// A builder that registers all system handlers based on node type and
/// provided dependencies.
///
/// It initializes global managers (AuditManager, MetricsManager, MetricsStore),
/// and conditionally registers handlers for audit, trace, worker discovery,
/// heartbeat, WAL sync, shutdown, topic discovery, and topic sync. It also
/// spawns a `NodeCollector` task to periodically publish node metrics.
pub struct SystemHandlerBuilder {
    trace_collectors: Vec<Arc<dyn TraceCollector>>,
    wal: Arc<RwLock<dyn Wal>>,
    shutdown_handler: ShutdownSender,
    audit_buf_capacity: usize,
}

impl SystemHandlerBuilder {
    /// Creates a new builder with required dependencies.
    ///
    /// # Arguments
    /// * `wal` - The WAL instance (wrapped in `Arc<RwLock<...>>`).
    /// * `shutdown_sender` - The broadcast sender for shutdown signals.
    /// * `audit_buf_capacity` - Capacity of the audit ring buffer.
    pub fn new(
        wal: Arc<RwLock<dyn Wal>>,
        shutdown_sender: ShutdownSender,
        audit_buf_capacity: usize,
    ) -> Self {
        Self {
            trace_collectors: Vec::new(),
            wal,
            shutdown_handler: shutdown_sender,
            audit_buf_capacity,
        }
    }

    /// Adds a trace collector to the builder.
    ///
    /// These collectors are passed to the `SystemTraceHandler` if any are
    /// provided.
    pub fn with_trace_collector(mut self, collector: Arc<dyn TraceCollector>) -> Self {
        self.trace_collectors.push(collector);
        self
    }

    /// Registers all system handlers to the global `ConsumerRouter`.
    ///
    /// This method initializes the global managers and then, depending on the
    /// node type:
    /// - **Worker**: registers only shutdown, topic sync, and metrics handlers,
    ///   and starts a `NodeCollector`.
    /// - **Host**: registers audit (if writers exist), trace (if collectors exist),
    ///   worker discovery, worker heartbeat, WAL sync, shutdown (both command and ack),
    ///   topic discovery, topic sync, and also starts a `NodeCollector`.
    ///
    /// # Errors
    /// Returns `CoreError` if any global manager initialization fails or
    /// if topic registration fails.
    pub async fn register_all(&self) -> Result<(), CoreError> {
        let router = ConsumerRouter::global();
        AuditManager::init(self.audit_buf_capacity)?;
        MetricsManager::init()?;
        MetricsStore::init()?;

        if get_node_type() == Arc::from(NodeType::Worker) {
            router
                .register(
                    SYSTEM_TOPIC_SHUTDOWN,
                    Arc::new(ShutdownHandler {
                        shutdown_tx: self.shutdown_handler.clone(),
                    }),
                )
                .await?;

            router
                .register(SYSTEM_TOPIC_TOPIC_SYNC, Arc::new(TopicSync {}))
                .await?;

            router
                .register(SYSTEM_TOPIC_METRICS, Arc::new(MetricsHandler {}))
                .await?;

            tokio::spawn(async move {
                let collector = NodeCollector;
                let _ = collector.start().await;
            });

            return Ok(());
        };

        if !AuditManager::global().writers.is_empty() {
            router
                .register(SYSTEM_TOPIC_AUDIT, Arc::new(AuditHandler {}))
                .await?;
        }

        if !self.trace_collectors.is_empty() {
            let handler = SystemTraceHandler::new(self.trace_collectors.clone());
            router
                .register(SYSTEM_TOPIC_TRACE, Arc::new(handler))
                .await?;
        }

        router
            .register(
                SYSTEM_TOPIC_WORKER_DISCOVERY,
                Arc::new(WorkerDiscoveryHandler {}),
            )
            .await?;

        router
            .register(
                SYSTEM_TOPIC_WORKER_HEARTBEAT,
                Arc::new(WorkerHeartbeatHandler {}),
            )
            .await?;

        router
            .register(
                SYSTEM_TOPIC_WAL_SYNC,
                Arc::new(WalSyncHandler::new(self.wal.clone())),
            )
            .await?;

        router
            .register(
                SYSTEM_TOPIC_SHUTDOWN,
                Arc::new(ShutdownHandler {
                    shutdown_tx: self.shutdown_handler.clone(),
                }),
            )
            .await?;

        router
            .register(SYSTEM_TOPIC_SHUTDOWN_ACK, Arc::new(ShutdownAckHandler {}))
            .await?;

        router
            .register(SYSTEM_TOPIC_TOPIC_DISCOVERY, Arc::new(TopicDiscovery {}))
            .await?;

        router
            .register(SYSTEM_TOPIC_TOPIC_SYNC, Arc::new(TopicSync {}))
            .await?;

        tokio::spawn(async move {
            let collector = NodeCollector;
            let _ = collector.start().await;
        });

        Ok(())
    }
}
