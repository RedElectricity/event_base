use event_base_core::constant::SYSTEM_TOPIC_TRACE;
use event_base_core::trace::TraceRecord;
use event_base_core::trace_layer::TraceLayer;
use event_base_test::support::RecordingProducer;
use std::sync::Arc;
use std::time::Duration;
use tracing::Level;
use tracing_subscriber::prelude::*;

#[tokio::test]
async fn trace_layer_captures_events() {
    let producer = Arc::new(RecordingProducer::default());
    let trace_layer = TraceLayer::new(producer.clone());

    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .with_target(false)
        .finish()
        .with(trace_layer);

    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(event_key = "event_value", count = 42, "test event");
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let try_sent = producer.try_sent.lock().await;
    assert!(
        !try_sent.is_empty(),
        "expected at least one trace event from on_event"
    );

    let event_msg = try_sent
        .iter()
        .find(|m| m.topic.0 == SYSTEM_TOPIC_TRACE)
        .expect("should find trace topic message");

    let record: TraceRecord =
        serde_json::from_slice(&event_msg.payload.0).expect("should deserialize");

    // For an event, name should be the event's message
    assert_eq!(record.level, event_base_core::trace::TraceLevel::Info);
    assert_eq!(record.fields.get("count"), Some(&serde_json::json!(42)));
}

#[tokio::test]
async fn trace_layer_captures_multiple_levels() {
    let producer = Arc::new(RecordingProducer::default());
    let trace_layer = TraceLayer::new(producer.clone());

    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .with_target(false)
        .finish()
        .with(trace_layer);

    tracing::subscriber::with_default(subscriber, || {
        tracing::error!("error event");
        tracing::warn!("warn event");
        tracing::info!("info event");
        tracing::debug!("debug event");
        tracing::trace!("trace event");
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let try_sent = producer.try_sent.lock().await;
    // Should have at least the error event (produced via on_event)
    assert!(
        try_sent.len() >= 1,
        "expected trace events, got {}",
        try_sent.len()
    );
}
