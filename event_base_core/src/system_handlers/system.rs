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

pub struct SystemHandlerBuilder {
    trace_collectors: Vec<Arc<dyn TraceCollector>>,
    wal: Arc<RwLock<dyn Wal>>,
    shutdown_handler: ShutdownSender,
    audit_buf_capacity: usize,
}

impl SystemHandlerBuilder {
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

    pub fn with_trace_collector(mut self, collector: Arc<dyn TraceCollector>) -> Self {
        self.trace_collectors.push(collector);
        self
    }

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
