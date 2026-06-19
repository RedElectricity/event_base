use crate::audit::AuditWriter;
use crate::constant::{SYSTEM_TOPIC_AUDIT, SYSTEM_TOPIC_TRACE};
use crate::error::CoreError;
use crate::system_handlers::audit::SystemAuditHandler;
use crate::system_handlers::trace::{SystemTraceHandler, TraceCollector};
use crate::topic::TopicRouter;
use std::sync::Arc;

pub struct SystemHandlerBuilder {
    audit_writers: Vec<Arc<dyn AuditWriter>>,
    trace_collectors: Vec<Arc<dyn TraceCollector>>,
}

impl SystemHandlerBuilder {
    pub fn new() -> Self {
        Self {
            audit_writers: Vec::new(),
            trace_collectors: Vec::new(),
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

        Ok(())
    }
}
