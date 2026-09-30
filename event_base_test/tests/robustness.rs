#![allow(dead_code)]
//! Robustness guarantees added in the enterprise hardening pass:
//! * wire‑schema version guard rejects envelopes newer than this node speaks;
//! * `TopicRouter` applies a configurable default send timeout so a stuck
//!   (full) producer surfaces as an error instead of an eternal await;
//! * `WorkerRegistry::init` fails with an error (not a panic) when no WAL.

use async_trait::async_trait;
use event_base_core::error::CoreError;
use event_base_core::message::{
    CURRENT_SCHEMA_VERSION, DeliveryMode, EMessage, MessagePayload, MessageTopic,
};
use event_base_core::queues::factory::QueueFactory;
use event_base_core::queues::{EConsumer, EProducer};
use event_base_core::topic::TopicRouter;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// A producer whose `send` never resolves — models a permanently full queue so
/// the timeout path (not the happy path) is what returns.
#[derive(Clone)]
struct StuckProducer;

#[async_trait]
impl EProducer for StuckProducer {
    async fn send(&self, _msg: EMessage) -> Result<(), CoreError> {
        // Hang far longer than any test timeout; only send_timeout saves us.
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(())
    }
    async fn try_send(&self, msg: EMessage) -> Result<(), CoreError> {
        // try_send must still work (it's the non‑blocking escape hatch).
        let _ = msg;
        Ok(())
    }
    async fn send_timeout(&self, _msg: EMessage, timeout: Duration) -> Result<(), CoreError> {
        tokio::time::timeout(timeout, self.send(EMessage::default()))
            .await
            .unwrap_or(Err(CoreError::Unsupported("timeout".into())))
    }
}

struct StuckFactory;
#[async_trait]
impl QueueFactory for StuckFactory {
    fn create_queue(
        &self,
        _topic: &str,
    ) -> Result<
        (
            Arc<dyn EProducer>,
            Arc<dyn event_base_core::queues::consumer_factory::ConsumerFactory>,
        ),
        CoreError,
    > {
        Err(CoreError::Unsupported("unused".into()))
    }
    fn create_global_producer(&self) -> Result<Arc<dyn EProducer>, CoreError> {
        Ok(Arc::new(StuckProducer))
    }
    fn create_main_consumer(&self) -> Result<Arc<Mutex<dyn EConsumer>>, CoreError> {
        Err(CoreError::Unsupported("unused".into()))
    }
    fn name(&self) -> &'static str {
        "stuck"
    }
    async fn health_check(&self) -> Result<(), CoreError> {
        Ok(())
    }
}

#[test]
fn wire_version_guard_accepts_current_rejects_newer() {
    let mut m = EMessage::new(
        MessageTopic("t".into()),
        MessagePayload(b"x".to_vec()),
        DeliveryMode::Standard,
        None,
    );
    assert_eq!(m.version, CURRENT_SCHEMA_VERSION);
    m.check_wire_version().expect("current version is accepted");

    m.version = CURRENT_SCHEMA_VERSION + 1;
    let err = m
        .check_wire_version()
        .expect_err("newer version must be rejected");
    assert!(matches!(err, CoreError::Unsupported(_)), "{err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_send_timeout_bounds_a_stuck_producer() {
    // Independent process‑wide singleton: this file owns TopicRouter.
    let _ = TopicRouter::init(StuckFactory.create_global_producer().unwrap());
    let router = TopicRouter::global().read().await;

    // Without a default timeout the send would await forever → the outer test
    // timeout (cargo test) is the backstop; here we prove the bounded path.
    router
        .set_default_send_timeout(Some(Duration::from_millis(80)))
        .await;

    let msg = EMessage::new(
        MessageTopic("orders".into()),
        MessagePayload(b"hi".to_vec()),
        DeliveryMode::Standard,
        None,
    );
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        router.send("orders", msg, None, None),
    )
    .await
    .expect("default send timeout must bound the stuck producer (no infinite hang)");
    assert!(result.is_err(), "stuck send should surface as an error");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "bounded quickly"
    );

    // try_send bypasses the timeout entirely (non‑blocking path).
    let msg2 = EMessage::new(
        MessageTopic("orders".into()),
        MessagePayload(b"try".to_vec()),
        DeliveryMode::Standard,
        None,
    );
    router
        .send("orders", msg2, Some(true), None)
        .await
        .expect("try_send on stuck producer returns Ok immediately");
}

#[tokio::test]
async fn worker_registry_init_without_wal_errors_not_panics() {
    // A fresh process (integration test binary) → WorkerRegistry is unset; the
    // None case must return Err, not hit the old `.unwrap()` panic.
    let result = event_base_core::worker_registry::WorkerRegistry::init(None).await;
    assert!(
        matches!(result, Err(CoreError::Unsupported(_))),
        "expected Unsupported error, got {result:?}"
    );
}
