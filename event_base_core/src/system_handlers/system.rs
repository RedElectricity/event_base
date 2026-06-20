use crate::audit::AuditWriter;
use crate::constant::{
    SYSTEM_TOPIC_AUDIT, SYSTEM_TOPIC_SHUTDOWN, SYSTEM_TOPIC_SHUTDOWN_ACK, SYSTEM_TOPIC_TRACE,
    SYSTEM_TOPIC_WAL_SYNC, SYSTEM_TOPIC_WORKER_DISCOVERY, SYSTEM_TOPIC_WORKER_HEARTBEAT,
};
use crate::error::CoreError;
use crate::shutdown::ShutdownSender;
use crate::system_handlers::audit::SystemAuditHandler;
use crate::system_handlers::shutdown::ShutdownAckHandler;
use crate::system_handlers::shutdown::ShutdownHandler;
use crate::system_handlers::trace::{SystemTraceHandler, TraceCollector};
use crate::system_handlers::wal::WalSyncHandler;
use crate::system_handlers::worker::{WorkerDiscoveryHandler, WorkerHeartbeatHandler};
use crate::topic::TopicRouter;
use crate::wal::wal::Wal;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SystemHandlerBuilder {
    audit_writers: Vec<Arc<dyn AuditWriter>>,
    trace_collectors: Vec<Arc<dyn TraceCollector>>,
    wal: Arc<Mutex<dyn Wal>>,
    shutdown_handler: ShutdownSender,
    is_host: bool,
}

impl SystemHandlerBuilder {
    pub fn new(wal: Arc<Mutex<dyn Wal>>, shutdown_sender: ShutdownSender, is_host: bool) -> Self {
        Self {
            audit_writers: Vec::new(),
            trace_collectors: Vec::new(),
            wal,
            shutdown_handler: shutdown_sender,
            is_host,
        }
    }

    pub fn with_audit_writer(mut self, writer: Arc<dyn AuditWriter>) -> Self {
        self.audit_writers.push(writer);
        self
    }

    pub fn with_trace_collector(mut self, collector: Arc<dyn TraceCollector>) -> Self {
        self.trace_collectors.push(collector);
        self
    }

    pub async fn register_all(&self) -> Result<(), CoreError> {
        let router = TopicRouter::global();

        if !self.is_host {
            router
                .register(
                    SYSTEM_TOPIC_SHUTDOWN,
                    Arc::new(ShutdownHandler {
                        shutdown_tx: self.shutdown_handler.clone(),
                    }),
                )
                .await?;
            return Ok(());
        };

        if !self.audit_writers.is_empty() {
            let handler = SystemAuditHandler::new(self.audit_writers.clone());
            router
                .register(SYSTEM_TOPIC_AUDIT, Arc::new(handler))
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

        Ok(())
    }
}
