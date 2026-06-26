use crate::constant::SYSTEM_TOPIC_TRACE;
use crate::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use crate::queues::EProducer;
use crate::topic::TopicRouter;
use crate::trace::{TraceLevel, TraceRecord};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tracing::span::{Attributes, Id};
use tracing_core::Subscriber;
use tracing_serde::AsSerde;
use tracing_subscriber::{Layer, registry::LookupSpan};

pub struct TraceLayer {
    pending_spans: Arc<Mutex<HashMap<Id, TraceRecord>>>,
    producer: Arc<dyn EProducer>,
}

impl TraceLayer {
    pub fn new(producer: Arc<dyn EProducer>) -> Self {
        Self {
            pending_spans: Arc::new(Mutex::new(HashMap::new())),
            producer,
        }
    }
}

impl<S> Layer<S> for TraceLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &Attributes<'_>,
        id: &Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx.span(&id).unwrap();
        let serializable = attrs.as_serde();
        let mut extensions = span.extensions_mut();

        let json_value =
            serde_json::to_value(&serializable).unwrap_or_else(|_| serde_json::json!({}));

        let fields = if let Value::Object(obj) = json_value {
            obj.clone()
        } else {
            Map::new()
        };

        let fields = HashMap::from_iter(fields.into_iter());

        let record = TraceRecord {
            trace_id: attrs.metadata().name().to_string(),
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

    // fn on_event(
    //     &self,
    //     event: &tracing::Event<'_>,
    //     _ctx: tracing_subscriber::layer::Context<'_, S>,
    // ) {
    //
    // }

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
                TopicRouter::global()
                    .send(SYSTEM_TOPIC_TRACE, msg, None, None)
                    .await
                    .expect("Fail to send tracing msg"); // 失败静默忽略
            });
        }
    }
}
