use event_base_core::audit::{AuditEventType, AuditManager, AuditRecord, AuditResult};
use event_base_core::constant::SYSTEM_TOPIC_TRACE;
use event_base_core::handler::EHandler;
use event_base_core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use event_base_core::metrics::manager::MetricsManager;
use event_base_core::metrics::node::NodeMetrics;
use event_base_core::metrics::node_store::MetricsStore;
use event_base_core::shutdown::messages::{ShutdownAck, ShutdownStatus};
use event_base_core::system_handlers::audit::AuditHandler;
use event_base_core::system_handlers::metrics::MetricsHandler;
use event_base_core::system_handlers::shutdown::ShutdownAckHandler;
use event_base_core::topic::TopicRouter;
use event_base_core::trace::TraceRecord;
use event_base_core::trace_layer::TraceLayer;
use event_base_core::wal::wal::{Wal, WalRecord, WalRecordState};
use event_base_core::worker_registry::{WorkerInfo, WorkerRegistry};
use event_base_core::{NodeType, get_node_name, get_node_type, set_node_name, set_node_type};
use event_base_test::support::{RecordingProducer, RecordingWal};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tracing::Level;
use tracing_subscriber::prelude::*;

fn message(topic: &str, payload: &[u8], delivery_mode: DeliveryMode) -> EMessage {
    EMessage::new(
        MessageTopic(topic.to_string()),
        MessagePayload(payload.to_vec()),
        delivery_mode,
        None,
    )
}

fn worker_info(worker_name: &str, topic: &str, last_heartbeat: SystemTime) -> WorkerInfo {
    WorkerInfo {
        worker_name: worker_name.to_string(),
        topic: topic.to_string(),
        last_heartbeat,
    }
}

fn audit_record(message_id: &str, topic: &str, event_type: AuditEventType) -> AuditRecord {
    AuditRecord {
        message_id: message_id.to_string(),
        topic: topic.to_string(),
        event_type,
        worker_id: Some("worker-a".to_string()),
        timestamp: SystemTime::now(),
        result: AuditResult::Success,
        error: None,
        duration: Some(Duration::from_millis(15)),
    }
}

fn node_metrics(node_name: &str) -> NodeMetrics {
    NodeMetrics {
        node_name: node_name.to_string(),
        node_type: NodeType::Host,
        cpu_percent: vec![12.5, 45.0],
        memory_percent: 33.0,
        node_worker_count: 2,
        update_time: SystemTime::now(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn core_workflow_covers_global_paths() {
    eprintln!("stage: setup");
    set_node_name("node-a".to_string());
    set_node_type(NodeType::Host);
    assert_eq!(get_node_name(), "node-a");
    assert_eq!(*get_node_type(), NodeType::Host);

    let fake_wal = RecordingWal::new();
    let now = SystemTime::now();
    fake_wal
        .seed_worker_registry(HashMap::from([
            (
                "worker-a".to_string(),
                worker_info("worker-a", "orders", now),
            ),
            (
                "worker-b".to_string(),
                worker_info("worker-b", "orders", now),
            ),
        ]))
        .await;

    let wal_handle: Arc<RwLock<Box<dyn Wal>>> = Arc::new(RwLock::new(Box::new(fake_wal.clone())));
    let producer = Arc::new(RecordingProducer::default());
    let mut wal_state = fake_wal.clone();

    AuditManager::init(16).expect("audit manager should initialize");
    MetricsStore::init().expect("metrics store should initialize");
    MetricsManager::init().expect("metrics manager should initialize");
    WorkerRegistry::init(Some(wal_handle.clone()))
        .await
        .expect("worker registry should initialize");
    TopicRouter::init(producer.clone())
        .expect("topic router should initialize");

    eprintln!("stage: router setup");

    let registry = WorkerRegistry::global().write().await;
    registry
        .register(worker_info(
            "retire-me",
            "orders",
            now - Duration::from_secs(60),
        ))
        .await
        .expect("worker register should succeed");
    registry
        .register(worker_info(
            "stale-worker",
            "orders",
            now - Duration::from_secs(3_600),
        ))
        .await
        .expect("worker register should succeed");

    TopicRouter::global().write().await.register_topic("orders").await;
    TopicRouter::global().write().await.register_topic("orders").await;
    let topics = TopicRouter::global().read().await.list_topics().await;
    assert_eq!(topics, vec!["orders".to_string()]);

    eprintln!("stage: routing");

    let standard_msg = message("ignored", b"standard", DeliveryMode::Standard);
    let standard_id = standard_msg.id.clone();
    TopicRouter::global().read().await
        .send("orders", standard_msg, None, None)
        .await
        .expect("standard send should succeed");
    assert_eq!(producer.sent.lock().await.len(), 1);
    assert_eq!(producer.sent.lock().await[0].topic.0, "orders");
    // TopicRouter::send does NOT write to the WAL, see topic.rs doc comment.
    // Seed the WAL manually so update_state can find the record.
    fake_wal
        .seed_pending(WalRecord::from_msg(producer.sent.lock().await[0].clone()))
        .await;
    wal_state
        .update_state(&standard_id, WalRecordState::Complete)
        .await
        .expect("state update should succeed");

    let try_msg = message("ignored", b"try", DeliveryMode::Standard);
    let try_id = try_msg.id.clone();
    TopicRouter::global().read().await
        .send("orders", try_msg, Some(true), None)
        .await
        .expect("try send should succeed");
    assert_eq!(producer.try_sent.lock().await.len(), 1);
    fake_wal
        .seed_pending(WalRecord::from_msg(producer.try_sent.lock().await[0].clone()))
        .await;
    wal_state
        .update_state(&try_id, WalRecordState::Complete)
        .await
        .expect("state update should succeed");

    let timeout_msg = message("ignored", b"timeout", DeliveryMode::Standard);
    let timeout_id = timeout_msg.id.clone();
    TopicRouter::global().read().await
        .send("orders", timeout_msg, None, Some(Duration::from_millis(2)))
        .await
        .expect("timeout send should succeed");
    assert_eq!(producer.timeout_sent.lock().await.len(), 1);
    fake_wal
        .seed_pending(WalRecord::from_msg(producer.timeout_sent.lock().await[0].0.clone()))
        .await;
    wal_state
        .update_state(&timeout_id, WalRecordState::Complete)
        .await
        .expect("state update should succeed");

    let broadcast_msg = message("ignored", b"broadcast", DeliveryMode::Broadcast);
    let broadcast_id = broadcast_msg.id.clone();
    let try_count_before = producer.try_sent.lock().await.len();
    let expected_broadcast_count = WorkerRegistry::global()
        .read().await
        .get_workers("orders")
        .await
        .expect("topic workers should exist")
        .len();
    TopicRouter::global().read().await
        .send("orders", broadcast_msg, Some(true), None)
        .await
        .expect("broadcast send should succeed");
    let try_sent = producer.try_sent.lock().await;
    assert_eq!(try_sent.len(), try_count_before + expected_broadcast_count);
    let broadcast_copies = &try_sent[try_count_before..];
    assert!(
        broadcast_copies
            .iter()
            .any(|msg| msg.to_worker.as_deref() == Some("worker-a"))
    );
    assert!(
        broadcast_copies
            .iter()
            .any(|msg| msg.to_worker.as_deref() == Some("worker-b"))
    );
    assert!(
        broadcast_copies
            .iter()
            .all(|msg| msg.id.starts_with(&broadcast_id))
    );
    drop(try_sent);
    fake_wal
        .seed_pending(WalRecord::from_msg({
            let mut m = message("ignored", b"", DeliveryMode::Standard);
            m.id = broadcast_id.clone();
            m
        }))
        .await;
    wal_state
        .update_state(&broadcast_id, WalRecordState::Complete)
        .await
        .expect("state update should succeed");

    let past_msg = message("orders", b"past", DeliveryMode::Standard);
    let mut past_msg = past_msg;
    let past_msg_id = past_msg.id.clone();
    past_msg.deliver_at = Some(SystemTime::now() - Duration::from_secs(1));
    // TopicRouter schedules past-deliver_at messages in the WAL (no ErrorTime check)
    TopicRouter::global().read().await
        .send("orders", past_msg, None, None)
        .await
        .expect("past deliver_at should schedule in WAL, not fail");
    fake_wal
        .seed_pending(WalRecord::from_msg({
            let mut m = message("ignored", b"", DeliveryMode::Standard);
            m.id = past_msg_id.clone();
            m
        }))
        .await;
    wal_state
        .update_state(&past_msg_id, WalRecordState::Complete)
        .await
        .expect("state update should succeed");

    let replay_now = message("orders", b"replay-now", DeliveryMode::Standard);
    let replay_later = {
        let mut msg = message("orders", b"replay-later", DeliveryMode::Standard);
        msg.deliver_at = Some(SystemTime::now() + Duration::from_secs(30));
        msg
    };
    let ignored = message("ignored-topic", b"ignore", DeliveryMode::Standard);
    fake_wal
        .seed_pending(WalRecord::from_msg(replay_now.clone()))
        .await;
    fake_wal
        .seed_pending(WalRecord::from_msg(replay_later.clone()))
        .await;
    fake_wal
        .seed_pending(WalRecord::from_msg(ignored.clone()))
        .await;

    let replay_summary = TopicRouter::global().read().await
        .replay(Some(&["orders"]))
        .await
        .expect("replay should succeed");
    assert_eq!(replay_summary.recovered, 1);
    assert_eq!(replay_summary.delayed, 1);
    assert!(replay_summary.errors.is_empty());
    assert!(
        producer
            .sent
            .lock()
            .await
            .iter()
            .any(|msg| msg.id == replay_now.id)
    );

    eprintln!("stage: replay complete");

    let stale = registry
        .cleanup_stale(Duration::from_secs(300))
        .await
        .expect("cleanup should succeed");
    assert!(stale.iter().any(|worker| worker == "stale-worker"));

    let audit_handler = AuditHandler {};
    let audit_event = audit_record("audit-1", "orders", AuditEventType::Enqueued);
    let audit_message = message(
        "_system.audit",
        &bincode::encode_to_vec(&audit_event, bincode::config::standard()).expect("audit event should serialize"),
        DeliveryMode::Standard,
    );
    assert!(matches!(
        audit_handler.handler(&audit_message).await,
        event_base_core::handler::Ack::Ack
    ));
    let recent_audits = AuditManager::global().read().await.get_recent(1).await;
    assert_eq!(recent_audits.len(), 1);
    assert_eq!(recent_audits[0].message_id, "audit-1");

    let metrics_handler = MetricsHandler {};
    let metrics = node_metrics(&get_node_name());
    let metrics_message = message(
        "_system.metrics",
        &bincode::encode_to_vec(&metrics, bincode::config::standard()).expect("metrics should serialize"),
        DeliveryMode::Standard,
    );
    assert!(matches!(
        metrics_handler.handler(&metrics_message).await,
        event_base_core::handler::Ack::Ack
    ));
    let stored_metrics = MetricsStore::global()
        .read().await
        .get_node(&get_node_name())
        .await
        .expect("node metrics should be present");
    assert_eq!(stored_metrics.node_worker_count, 2);

    MetricsManager::global()
        .write().await
        .feed_audit(&audit_record("audit-2", "orders", AuditEventType::Retry))
        .await;
    let snapshot = MetricsManager::global().read().await.snapshot().await;
    assert_eq!(snapshot.business.retried.get("orders").copied(), Some(1));
    assert!(
        snapshot
            .nodes
            .iter()
            .any(|node| node.node_name == get_node_name())
    );

    let shutdown_handler = ShutdownAckHandler;
    let shutdown_message = message(
        "_system.shutdown_ack",
        &bincode::encode_to_vec(&ShutdownAck {
            worker_name: "retire-me".to_string(),
            status: ShutdownStatus::Completed,
            timestamp: SystemTime::now(),
            error: None,
        }, bincode::config::standard())
        .expect("shutdown ack should serialize"),
        DeliveryMode::Standard,
    );
    assert!(matches!(
        shutdown_handler.handler(&shutdown_message).await,
        event_base_core::handler::Ack::Ack
    ));
    let remaining_workers = WorkerRegistry::global().read().await.get_all_workers().await;
    assert!(!format!("{:?}", remaining_workers).contains("retire-me"));

    eprintln!("stage: shutdown handlers");

    let trace_producer = Arc::new(RecordingProducer::default());
    let trace_layer = TraceLayer::new(trace_producer.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish()
        .with(trace_layer);
    tracing::subscriber::with_default(subscriber, || {
        let span = tracing::span!(
            Level::INFO,
            "workflow",
            trace_id = "trace-123",
            job = "integration"
        );
        let _guard = span.enter();
        tracing::info!(event = "trace emitted", value = 7, "trace event");
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(trace_producer.try_sent.lock().await.len(), 1);
    let trace_event = trace_producer.try_sent.lock().await[0].clone();
    assert_eq!(trace_event.topic.0, SYSTEM_TOPIC_TRACE);
    let trace_record: TraceRecord =
        serde_json::from_slice(&trace_event.payload.0).expect("trace record should deserialize");
    assert!(trace_record.name.starts_with("event "));
    assert_eq!(
        trace_record.fields.get("value"),
        Some(&serde_json::json!(7))
    );

    let router_trace_messages = producer.sent.lock().await;
    assert!(
        router_trace_messages
            .iter()
            .any(|msg| msg.topic.0 == SYSTEM_TOPIC_TRACE)
    );

    eprintln!("stage: trace complete");
}
