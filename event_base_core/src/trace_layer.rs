//! A tracing `Layer` that captures span and event data and sends them as messages.
//!
//! This layer converts tracing spans and events into `TraceRecord` structures and
//! forwards them via the system's message bus (using the `SYSTEM_TOPIC_TRACE` topic).

use crate::constant::SYSTEM_TOPIC_TRACE;
use crate::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use crate::queues::EProducer;
use crate::topic::TopicRouter;
use crate::trace::{TraceLevel, TraceRecord};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::span::{Attributes, Id};
use tracing_core::Subscriber;
use tracing_serde::AsSerde;
use tracing_subscriber::{Layer, registry::LookupSpan};

/// A tracing layer that emits trace records as messages.
pub struct TraceLayer {
    producer: Arc<dyn EProducer>,
}

impl TraceLayer {
    /// Creates a new `TraceLayer` with the given producer.
    pub fn new(producer: Arc<dyn EProducer>) -> Self {
        Self { producer }
    }
}

impl<S> Layer<S> for TraceLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    /// When a new span is created, it captures the span's fields and stores a `TraceRecord`
    /// in the span's extensions.
    fn on_new_span(
        &self,
        attrs: &Attributes<'_>,
        id: &Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx.span(id).unwrap();
        let serializable = attrs.as_serde();
        let mut extensions = span.extensions_mut();

        let json_value =
            serde_json::to_value(&serializable).unwrap_or_else(|_| serde_json::json!({}));

        let fields = if let Value::Object(obj) = json_value {
            obj.clone()
        } else {
            Map::new()
        };

        let trace_id: Option<String> = attrs.fields().iter().find_map(|x| {
            if x.name() == "trace_id" {
                Option::from(x.to_string())
            } else {
                None
            }
        });

        let fields = HashMap::from_iter(fields);

        let record = TraceRecord {
            trace_id,
            span_id: id.into_u64().to_string(),
            parent_span_id: span.parent().map(|p| p.id().into_u64().to_string()),
            name: span.name().to_string(),
            target: span.metadata().target().to_string(),
            level: TraceLevel::Info,
            fields,
            started_at: Some(SystemTime::now()),
            finished_at: None,
            duration: None,
            message_id: None,
            worker_id: None,
            topic: None,
        };

        extensions.insert(record);
    }

    /// When a tracing event occurs, it is serialized and sent as a trace message.
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let event_serializable = event.as_serde();

        let json_value =
            serde_json::to_value(&event_serializable).unwrap_or_else(|_| serde_json::json!({}));

        let fields = if let Value::Object(obj) = json_value {
            obj.clone()
        } else {
            Map::new()
        };

        let fields = HashMap::from_iter(fields);

        let record = TraceRecord {
            span_id: "".to_string(),
            trace_id: None,
            name: event.metadata().name().to_string(),
            target: event.metadata().target().to_string(),
            level: match *event.metadata().level() {
                tracing::Level::ERROR => TraceLevel::Error,
                tracing::Level::WARN => TraceLevel::Warn,
                tracing::Level::INFO => TraceLevel::Info,
                tracing::Level::DEBUG => TraceLevel::Debug,
                tracing::Level::TRACE => TraceLevel::Trace,
            },
            fields,
            started_at: Some(SystemTime::now()),
            finished_at: Some(SystemTime::now()),
            duration: None,
            message_id: None,
            worker_id: None,
            topic: None,
            parent_span_id: None,
        };

        let msg = EMessage::new(
            MessageTopic(SYSTEM_TOPIC_TRACE.to_string()),
            MessagePayload(serde_json::to_vec(&record).unwrap()),
            DeliveryMode::Standard,
            None,
        );
        let _ = self.producer.try_send(msg);
    }

    /// When a span is closed, the stored `TraceRecord` is completed with finish time
    /// and duration, then sent as a message.
    fn on_close(&self, id: Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let span = ctx.span(&id).unwrap();
        let mut extensions = span.extensions_mut();
        if let Some(mut record) = extensions.remove::<TraceRecord>() {
            record.finished_at = Some(SystemTime::now());
            if let Some(start) = record.started_at {
                record.duration = Some(start.elapsed().unwrap_or_default());
            }

            let msg = EMessage::new(
                MessageTopic(SYSTEM_TOPIC_TRACE.to_string()),
                MessagePayload(serde_json::to_vec(&record).unwrap()),
                DeliveryMode::Standard,
                None,
            );
            tokio::spawn(async move {
                let _ = TopicRouter::global()
                    .send(SYSTEM_TOPIC_TRACE, msg, None, None)
                    .await;
            });
        }
    }
}
