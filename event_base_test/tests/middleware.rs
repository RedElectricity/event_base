use async_trait::async_trait;
use event_base_core::dead_letter::DeadReason;
use event_base_core::handler::{Ack, EHandler};
use event_base_core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use event_base_core::middleware::{Middleware, Next, Pipeline};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct RecordingHandler {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EHandler for RecordingHandler {
    async fn handler(&self, msg: &EMessage) -> Ack {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(msg.payload.0, b"abc12".to_vec());
        Ack::Ack
    }
}

struct AppendMiddleware {
    suffix: &'static [u8],
}

#[async_trait]
impl Middleware for AppendMiddleware {
    async fn handle(&self, msg: &mut EMessage, next: Next<'_>) -> Ack {
        msg.payload.0.extend_from_slice(self.suffix);
        next.run(msg).await
    }
}

struct ShortCircuitMiddleware;

#[async_trait]
impl Middleware for ShortCircuitMiddleware {
    async fn handle(&self, _msg: &mut EMessage, _next: Next<'_>) -> Ack {
        Ack::Dead {
            dead_reason: DeadReason::Explicit,
        }
    }
}

#[tokio::test]
async fn pipeline_runs_middlewares_in_order() {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = RecordingHandler {
        calls: calls.clone(),
    };
    let pipeline = Pipeline::new(Box::new(handler))
        .with(AppendMiddleware { suffix: b"1" })
        .with(AppendMiddleware { suffix: b"2" });

    let mut message = EMessage::new(
        MessageTopic("pipeline".to_string()),
        MessagePayload(b"abc".to_vec()),
        DeliveryMode::Standard,
        None,
    );

    let ack = pipeline.run(&mut message).await;
    assert!(matches!(ack, Ack::Ack));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(message.payload.0, b"abc12".to_vec());
}

#[tokio::test]
async fn pipeline_short_circuits_without_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = RecordingHandler { calls };
    let pipeline = Pipeline::new(Box::new(handler))
        .with(AppendMiddleware { suffix: b"1" })
        .with(ShortCircuitMiddleware)
        .with(AppendMiddleware { suffix: b"2" });

    let mut message = EMessage::new(
        MessageTopic("pipeline".to_string()),
        MessagePayload(b"abc".to_vec()),
        DeliveryMode::Standard,
        None,
    );

    let ack = pipeline.run(&mut message).await;
    assert!(matches!(ack, Ack::Dead { .. }));
    assert_eq!(message.payload.0, b"abc1".to_vec());
}
