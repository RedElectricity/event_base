//! Comprehensive tests for Worker message processing (process_msg) and
//! WalClient synchronization.
//!
//! All tests in ONE function — OnceLock singletons are process-global.

use async_trait::async_trait;
use event_base_core::audit::{AuditEventType, AuditManager, AuditRecord, AuditResult};
use event_base_core::constant::{
    SYSTEM_TOPIC_AUDIT, SYSTEM_TOPIC_SHUTDOWN_ACK, SYSTEM_TOPIC_WAL_SYNC,
};
use event_base_core::dead_letter::DeadReason;
use event_base_core::error::CoreError;
use event_base_core::handler::{Ack, EHandler};
use event_base_core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use event_base_core::middleware::Pipeline;
use event_base_core::queues::consumer_factory::ConsumerFactory;
use event_base_core::queues::consumer_router::ConsumerRouter;
use event_base_core::queues::factory::QueueFactory;
use event_base_core::queues::{ClaimedMessage, EConsumer, EProducer};
use event_base_core::shutdown::messages::ShutdownAck;
use event_base_core::topic::TopicRouter;
use event_base_core::wal::sync::{WalClient, WalSyncMessage};
use event_base_core::wal::wal::{Wal, WalRecordState};
use event_base_core::worker::Worker;
use event_base_core::worker_registry::WorkerRegistry;
use event_base_core::{NodeType, set_node_name, set_node_type};
use event_base_test::support::{RecordingProducer, RecordingWal};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, RwLock};

// ── Minimal noop consumer (unused — test_process_msg bypasses start()) ──

struct NoopConsumer;
#[async_trait]
impl EConsumer for NoopConsumer {
    async fn receive(&mut self) -> Option<EMessage> {
        None
    }
    async fn claim(&mut self) -> Result<Option<ClaimedMessage>, CoreError> {
        Ok(None)
    }
    async fn ack(&mut self, _: &str) -> Result<(), CoreError> {
        Ok(())
    }
    async fn nack(&mut self, _: &str) -> Result<(), CoreError> {
        Ok(())
    }
}

struct NoopConsumerFactory;
impl ConsumerFactory for NoopConsumerFactory {
    fn create_consumer(&self) -> Box<dyn EConsumer> {
        Box::new(NoopConsumer)
    }
    fn clone_factory(&self) -> Arc<dyn ConsumerFactory> {
        Arc::new(NoopConsumerFactory)
    }
}

struct NoopQueueFactory {
    producer: Arc<dyn EProducer>,
}
#[async_trait]
impl QueueFactory for NoopQueueFactory {
    fn create_queue(
        &self,
        _: &str,
    ) -> Result<(Arc<dyn EProducer>, Arc<dyn ConsumerFactory>), CoreError> {
        Ok((self.producer.clone(), Arc::new(NoopConsumerFactory)))
    }
    fn create_global_producer(&self) -> Result<Arc<dyn EProducer>, CoreError> {
        Ok(self.producer.clone())
    }
    fn create_main_consumer(&self) -> Result<Arc<Mutex<dyn EConsumer>>, CoreError> {
        Ok(Arc::new(Mutex::new(NoopConsumer)))
    }
    fn name(&self) -> &'static str {
        "noop"
    }
}

// ── Static-response handler ──

struct StaticHandler {
    response: Ack,
}
#[async_trait]
impl EHandler for StaticHandler {
    async fn handler(&self, _msg: &EMessage) -> Ack {
        match &self.response {
            Ack::Ack => Ack::Ack,
            Ack::Dead { dead_reason } => Ack::Dead {
                dead_reason: dead_reason.clone(),
            },
            Ack::NoAck {
                retry_after,
                max_retries,
            } => Ack::NoAck {
                retry_after: *retry_after,
                max_retries: *max_retries,
            },
        }
    }
}

// ── Helpers ──

fn msg(topic: &str, payload: &[u8], mode: DeliveryMode) -> EMessage {
    EMessage::new(
        MessageTopic(topic.to_string()),
        MessagePayload(payload.to_vec()),
        mode,
        None,
    )
}

fn make_worker(
    topic: &str,
    pipeline: Arc<Pipeline>,
    producer: Arc<dyn EProducer>,
    timeout: Option<Duration>,
) -> Worker {
    Worker::new(
        topic.to_string(),
        Box::new(NoopConsumer),
        pipeline,
        producer,
        timeout,
        Duration::from_millis(50),
        Some(Duration::from_millis(500)),
    )
}

async fn g_topic(p: &RecordingProducer, t: &str) -> Vec<EMessage> {
    p.sent
        .lock()
        .await
        .iter()
        .filter(|m| m.topic.0 == t)
        .cloned()
        .collect()
}
async fn g_count(p: &RecordingProducer, t: &str) -> usize {
    g_topic(p, t).await.len()
}

// ═══════════════════════════════════════════════════════════════
// THE COMBINED TEST
// ═══════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_process_msg_and_wal_sync_coverage() {
    let _ = set_node_name("cov-node".to_string());
    let _ = set_node_type(NodeType::Host);
    let fake_wal = RecordingWal::new();
    let wal_handle: Arc<RwLock<Box<dyn Wal>>> = Arc::new(RwLock::new(Box::new(fake_wal.clone())));
    let _ = WorkerRegistry::init(Some(wal_handle.clone())).await;
    let gp = Arc::new(RecordingProducer::default());
    let _ = TopicRouter::init(gp.clone());
    let _ = AuditManager::init(32);
    let _ = event_base_core::metrics::manager::MetricsManager::init();
    let _ = event_base_core::metrics::node_store::MetricsStore::init();
    let f = Arc::new(NoopQueueFactory {
        producer: gp.clone(),
    });
    let mc = f.create_main_consumer().unwrap();
    let _ = ConsumerRouter::init(mc, f, None);
    let t = "cov";
    let h = Arc::new(StaticHandler { response: Ack::Ack });
    let wp = Arc::new(RecordingProducer::default());

    // ── 1: Ack Standard → 2 audits + WAL complete ──
    ConsumerRouter::global().write().await.register(t, h.clone()).await.expect("r");
    let pl = Arc::new(Pipeline::new(Box::new(StaticHandler {
        response: Ack::Ack,
    })));
    let w = make_worker(t, pl, wp.clone(), None);
    let m = msg(t, b"1", DeliveryMode::Standard);
    let mid = m.id.clone();
    let ba = g_count(&gp, SYSTEM_TOPIC_AUDIT).await;
    let bw = g_count(&gp, SYSTEM_TOPIC_WAL_SYNC).await;
    w.test_process_msg(m).await.expect("1");
    assert_eq!(g_count(&gp, SYSTEM_TOPIC_AUDIT).await - ba, 2);
    let wms = g_topic(&gp, SYSTEM_TOPIC_WAL_SYNC).await;
    let ours: Vec<_> = wms
        .iter()
        .skip(bw)
        .filter(|x| {
            bincode::decode_from_slice::<WalSyncMessage, _>(&x.payload.0, bincode::config::standard())
                .map_or(false, |(s, _)| s.message_id == mid)
        })
        .collect();
    assert_eq!(ours.len(), 2);
    assert!(ours.iter().any(|x| {
        bincode::decode_from_slice::<WalSyncMessage, _>(&x.payload.0, bincode::config::standard())
            .unwrap()
            .0
            .status
            == WalRecordState::Processing
    }));
    assert!(ours.iter().any(|x| {
        bincode::decode_from_slice::<WalSyncMessage, _>(&x.payload.0, bincode::config::standard())
            .unwrap()
            .0
            .status
            == WalRecordState::Complete
    }));
    ConsumerRouter::global().write().await.del_workers(t).await.ok();

    // ── 2: Dead → dead_letter topic + WAL Failed ──
    ConsumerRouter::global().write().await.register(t, h.clone()).await.expect("r");
    let pl = Arc::new(Pipeline::new(Box::new(StaticHandler {
        response: Ack::Dead {
            dead_reason: DeadReason::Explicit,
        },
    })));
    let w = make_worker(t, pl, wp.clone(), None);
    let m = msg(t, b"2", DeliveryMode::Standard);
    let mid = m.id.clone();
    let bb = gp.sent.lock().await.len();
    w.test_process_msg(m).await.expect("2");
    let ms = gp.sent.lock().await;
    let n: Vec<_> = ms.iter().skip(bb).collect();
    assert!(n.iter().any(|x| x.topic.0.starts_with("dead_letter.")));
    assert!(
        n.iter()
            .filter(|x| x.topic.0 == SYSTEM_TOPIC_WAL_SYNC)
            .any(|x| {
                bincode::decode_from_slice::<WalSyncMessage, _>(&x.payload.0, bincode::config::standard()).map_or(false, |(s, _)| {
                    s.status == WalRecordState::Failed && s.message_id == mid
                })
            })
    );
    drop(ms);
    ConsumerRouter::global().write().await.del_workers(t).await.ok();

    // ── 3: NoAck no retry_after → worker producer ──
    ConsumerRouter::global().write().await.register(t, h.clone()).await.expect("r");
    let pl = Arc::new(Pipeline::new(Box::new(StaticHandler {
        response: Ack::NoAck {
            retry_after: None,
            max_retries: 5,
        },
    })));
    let w = make_worker(t, pl, wp.clone(), None);
    let m = msg(t, b"3", DeliveryMode::Standard);
    let bw = wp.sent.lock().await.len();
    w.test_process_msg(m).await.expect("3");
    assert_eq!(wp.sent.lock().await.len(), bw + 1);
    assert_eq!(wp.sent.lock().await[bw].attempts, 1);
    ConsumerRouter::global().write().await.del_workers(t).await.ok();

    // ── 4: NoAck with retry_after → scheduled in WAL (not sent immediately) ──
    ConsumerRouter::global().write().await.register(t, h.clone()).await.expect("r");
    let pl = Arc::new(Pipeline::new(Box::new(StaticHandler {
        response: Ack::NoAck {
            retry_after: Some(Duration::from_secs(10)),
            max_retries: 5,
        },
    })));
    let w = make_worker(t, pl, wp.clone(), None);
    let m = msg(t, b"4", DeliveryMode::Standard);
    let mid = m.id.clone();
    let wp_before = wp.sent.lock().await.len();
    w.test_process_msg(m).await.expect("4");
    // Message with deliver_at goes to WAL schedule, not producer
    // Worker producer delta should be 0 (retry_after route uses TopicRouter)
    assert_eq!(wp.sent.lock().await.len(), wp_before);
    // TopicRouter schedules the message; verify it appears in scheduled records
    let scheduled = fake_wal.scheduled_records().await;
    assert!(
        scheduled.iter().any(|r| r.message.id == mid),
        "message should be scheduled in WAL"
    );
    ConsumerRouter::global().write().await.del_workers(t).await.ok();

    // ── 5: NoAck max_retries exceeded → dead letter ──
    ConsumerRouter::global().write().await.register(t, h.clone()).await.expect("r");
    let pl = Arc::new(Pipeline::new(Box::new(StaticHandler {
        response: Ack::NoAck {
            retry_after: None,
            max_retries: 1,
        },
    })));
    let w = make_worker(t, pl, wp.clone(), None);
    let mut m = msg(t, b"5", DeliveryMode::Standard);
    m.attempts = 1;
    let bg = gp.sent.lock().await.len();
    w.test_process_msg(m).await.expect("5");
    let ms = gp.sent.lock().await;
    assert!(
        ms.iter()
            .skip(bg)
            .any(|x| x.topic.0.starts_with("dead_letter."))
    );
    drop(ms);
    ConsumerRouter::global().write().await.del_workers(t).await.ok();

    // ── 6: Repeated(3) fully consumed → complete ──
    ConsumerRouter::global().write().await.register(t, h.clone()).await.expect("r");
    let pl = Arc::new(Pipeline::new(Box::new(StaticHandler {
        response: Ack::Ack,
    })));
    let w = make_worker(t, pl, wp.clone(), None);
    let mut m = msg(t, b"6", DeliveryMode::Repeated(3));
    m.consumed_count = 2;
    let mid = m.id.clone();
    let bw = g_count(&gp, SYSTEM_TOPIC_WAL_SYNC).await;
    w.test_process_msg(m).await.expect("6");
    let wms = g_topic(&gp, SYSTEM_TOPIC_WAL_SYNC).await;
    assert!(wms.iter().skip(bw).any(|x| {
        bincode::decode_from_slice::<WalSyncMessage, _>(&x.payload.0, bincode::config::standard()).map_or(false, |(s, _)| {
            s.status == WalRecordState::Complete && s.message_id == mid
        })
    }));
    ConsumerRouter::global().write().await.del_workers(t).await.ok();

    // ── 7: Repeated(5) requeue ──
    ConsumerRouter::global().write().await.register(t, h.clone()).await.expect("r");
    let pl = Arc::new(Pipeline::new(Box::new(StaticHandler {
        response: Ack::Ack,
    })));
    let w = make_worker(t, pl, wp.clone(), None);
    let m = msg(t, b"7", DeliveryMode::Repeated(5));
    let mid = m.id.clone();
    let bw = wp.sent.lock().await.len();
    let bwl = g_count(&gp, SYSTEM_TOPIC_WAL_SYNC).await;
    w.test_process_msg(m).await.expect("7");
    assert_eq!(wp.sent.lock().await.len(), bw + 1);
    assert_eq!(wp.sent.lock().await[bw].consumed_count, 1);
    let wms = g_topic(&gp, SYSTEM_TOPIC_WAL_SYNC).await;
    assert!(wms.iter().skip(bwl).any(|x| {
        bincode::decode_from_slice::<WalSyncMessage, _>(&x.payload.0, bincode::config::standard()).map_or(false, |(s, _)| {
            s.status == WalRecordState::Pending && s.message_id == mid
        })
    }));
    ConsumerRouter::global().write().await.del_workers(t).await.ok();

    // ── 8: Timeout → Dead(Timeout) ──
    ConsumerRouter::global().write().await.register(t, h.clone()).await.expect("r");
    struct Slow;
    #[async_trait]
    impl EHandler for Slow {
        async fn handler(&self, _: &EMessage) -> Ack {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ack::Ack
        }
    }
    let pl = Arc::new(Pipeline::new(Box::new(Slow)));
    let w = make_worker(t, pl, wp.clone(), Some(Duration::from_millis(10)));
    w.test_process_msg(msg(t, b"8", DeliveryMode::Standard))
        .await
        .expect("8");
    let ms = gp.sent.lock().await;
    assert!(ms.iter().any(|x| x.topic.0.starts_with("dead_letter.")));
    drop(ms);
    ConsumerRouter::global().write().await.del_workers(t).await.ok();

    // ── 9: WalClient all methods ──
    let client = WalClient::new("w".to_string());
    let tt = "wc";
    let bb = gp.sent.lock().await.len();
    client.mark_pending("mp", tt).await.expect("mp");
    client.mark_processing("mpr", tt).await.expect("mpr");
    client.mark_complete("mc", tt, 3).await.expect("mc");
    client
        .mark_dead_letter("md", tt, 5, "err".to_string())
        .await
        .expect("md");
    let ms = gp.sent.lock().await;
    let n: Vec<_> = ms.iter().skip(bb).collect();
    assert_eq!(n.len(), 4);
    assert!(n.iter().all(|x| x.topic.0 == SYSTEM_TOPIC_WAL_SYNC));
    let d: Vec<WalSyncMessage> = n
        .iter()
        .map(|x| bincode::decode_from_slice(&x.payload.0, bincode::config::standard()).unwrap().0)
        .collect();
    assert!(
        d.iter()
            .any(|s| s.message_id == "mp" && s.status == WalRecordState::Pending)
    );
    assert!(
        d.iter()
            .any(|s| s.message_id == "mpr" && s.status == WalRecordState::Processing)
    );
    assert!(
        d.iter().any(|s| s.message_id == "mc"
            && s.status == WalRecordState::Complete
            && s.attempts == 3)
    );
    assert!(d.iter().any(|s| s.message_id == "md"
        && s.status == WalRecordState::Failed
        && s.error.as_deref() == Some("err")
        && s.attempts == 5));
    drop(ms);

    // ── 10: Broadcast Ack ──
    ConsumerRouter::global().write().await.register(t, h.clone()).await.expect("r");
    let pl = Arc::new(Pipeline::new(Box::new(StaticHandler {
        response: Ack::Ack,
    })));
    let w = make_worker(t, pl, wp.clone(), None);
    w.test_process_msg(msg(t, b"10", DeliveryMode::Broadcast))
        .await
        .expect("10");
    assert!(g_topic(&gp, SYSTEM_TOPIC_WAL_SYNC).await.iter().any(|x| {
        bincode::decode_from_slice::<WalSyncMessage, _>(&x.payload.0, bincode::config::standard())
            .map_or(false, |(s, _)| s.status == WalRecordState::Complete)
    }));
    ConsumerRouter::global().write().await.del_workers(t).await.ok();

    // ── 11: generate_audit_msg fields ──
    let pl = Arc::new(Pipeline::new(Box::new(StaticHandler {
        response: Ack::Ack,
    })));
    let w = make_worker(t, pl, wp.clone(), None);
    let m = msg(t, b"11", DeliveryMode::Standard);
    let d = Duration::from_millis(42);
    let r = w.test_generate_audit_msg(
        m.clone(),
        AuditResult::Success,
        AuditEventType::ProcessingCompleted,
        Some("e".into()),
        Some(d),
    );
    assert_eq!(r.message_id, m.id);
    assert_eq!(r.topic, t);
    assert!(matches!(r.event_type, AuditEventType::ProcessingCompleted));
    assert!(matches!(r.result, AuditResult::Success));
    assert_eq!(r.error.as_deref(), Some("e"));
    assert_eq!(r.duration, Some(d));

    // ── 12: send_to_dead_letter full path ──
    ConsumerRouter::global().write().await.register(t, h.clone()).await.expect("r");
    let pl = Arc::new(Pipeline::new(Box::new(StaticHandler {
        response: Ack::Ack,
    })));
    let w = make_worker(t, pl, wp.clone(), None);
    let m = msg(t, b"12", DeliveryMode::Standard);
    let b = gp.sent.lock().await.len();
    w.test_send_to_dead_letter(m, DeadReason::Timeout, Duration::from_millis(50))
        .await
        .expect("dl");
    let ms = gp.sent.lock().await;
    let n: Vec<_> = ms.iter().skip(b).collect();
    assert!(n.iter().any(|x| x.topic.0 == format!("dead_letter.{}", t)));
    assert!(n.iter().any(|x| x.topic.0 == SYSTEM_TOPIC_AUDIT));
    drop(ms);
    ConsumerRouter::global().write().await.del_workers(t).await.ok();

    // ── 13: shutdown → ShutdownAck ──
    ConsumerRouter::global().write().await.register(t, h.clone()).await.expect("r");
    let pl = Arc::new(Pipeline::new(Box::new(StaticHandler {
        response: Ack::Ack,
    })));
    let w = make_worker(t, pl, wp.clone(), None);
    let wname = w.name.clone();
    let b = gp.sent.lock().await.len();
    w.shutdown(Duration::from_millis(10), Some(Duration::from_millis(100)))
        .await
        .expect("sd");
    let ms = gp.sent.lock().await;
    let acks: Vec<_> = ms
        .iter()
        .skip(b)
        .filter(|x| x.topic.0 == SYSTEM_TOPIC_SHUTDOWN_ACK)
        .collect();
    assert!(!acks.is_empty());
    if let Ok((a, _)) = bincode::decode_from_slice::<ShutdownAck, _>(&acks[0].payload.0, bincode::config::standard()) {
        assert_eq!(a.worker_name, wname);
    }
    drop(ms);
    ConsumerRouter::global().write().await.del_workers(t).await.ok();

    // ── 14: AuditManager get_recent ──
    for i in 0..5 {
        AuditManager::global()
            .write().await
            .record(AuditRecord {
                message_id: format!("a-{}", i),
                topic: "ac".into(),
                event_type: AuditEventType::Enqueued,
                worker_id: None,
                timestamp: SystemTime::now(),
                result: AuditResult::Start,
                error: None,
                duration: None,
            })
            .await
            .expect("rec");
    }
    let recent = AuditManager::global().read().await.get_recent(3).await;
    assert_eq!(recent.len(), 3);
    assert_eq!(recent[0].message_id, "a-4");

    // ── 15: requeue_message ──
    ConsumerRouter::global().write().await.register(t, h.clone()).await.expect("r");
    let pl = Arc::new(Pipeline::new(Box::new(StaticHandler {
        response: Ack::Ack,
    })));
    let w = make_worker(t, pl, wp.clone(), None);
    let m = msg(t, b"15", DeliveryMode::Standard);
    let mid = m.id.clone();
    let bw = wp.sent.lock().await.len();
    let bwl = g_count(&gp, SYSTEM_TOPIC_WAL_SYNC).await;
    w.test_requeue_message(m).await.expect("rq");
    assert_eq!(wp.sent.lock().await.len(), bw + 1);
    assert!(
        g_topic(&gp, SYSTEM_TOPIC_WAL_SYNC)
            .await
            .iter()
            .skip(bwl)
            .any(|x| {
                bincode::decode_from_slice::<WalSyncMessage, _>(&x.payload.0, bincode::config::standard()).map_or(false, |(s, _)| {
                    s.status == WalRecordState::Pending && s.message_id == mid
                })
            })
    );
    ConsumerRouter::global().write().await.del_workers(t).await.ok();

    // ── 16: Repeated(1) complete ──
    ConsumerRouter::global().write().await.register(t, h.clone()).await.expect("r");
    let pl = Arc::new(Pipeline::new(Box::new(StaticHandler {
        response: Ack::Ack,
    })));
    let w = make_worker(t, pl, wp.clone(), None);
    let m = msg(t, b"16", DeliveryMode::Repeated(1));
    let mid = m.id.clone();
    let bw = g_count(&gp, SYSTEM_TOPIC_WAL_SYNC).await;
    w.test_process_msg(m).await.expect("16");
    assert!(
        g_topic(&gp, SYSTEM_TOPIC_WAL_SYNC)
            .await
            .iter()
            .skip(bw)
            .any(|x| {
                bincode::decode_from_slice::<WalSyncMessage, _>(&x.payload.0, bincode::config::standard()).map_or(false, |(s, _)| {
                    s.status == WalRecordState::Complete && s.message_id == mid
                })
            })
    );
    ConsumerRouter::global().write().await.del_workers(t).await.ok();

    // ── 17: is_shutdown_complete + get_status ──
    ConsumerRouter::global().write().await.register(t, h.clone()).await.expect("r");
    let pl = Arc::new(Pipeline::new(Box::new(StaticHandler {
        response: Ack::Ack,
    })));
    let w = make_worker(t, pl, wp.clone(), None);
    assert!(!w.is_shutdown_complete());
    assert!(matches!(
        w.get_status().await,
        event_base_core::worker::WorkerStatus::Idle
    ));
    ConsumerRouter::global().write().await.del_workers(t).await.ok();
}
