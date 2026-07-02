use async_trait::async_trait;
use event_base_core::constant::{
    SYSTEM_TOPIC_AUDIT, SYSTEM_TOPIC_METRICS, SYSTEM_TOPIC_SHUTDOWN, SYSTEM_TOPIC_SHUTDOWN_ACK,
    SYSTEM_TOPIC_TOPIC_DISCOVERY, SYSTEM_TOPIC_TOPIC_SYNC, SYSTEM_TOPIC_TRACE,
    SYSTEM_TOPIC_WAL_SYNC, SYSTEM_TOPIC_WORKER_DISCOVERY, SYSTEM_TOPIC_WORKER_HEARTBEAT,
};
use event_base_core::handler::{Ack, EHandler};
use event_base_core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use event_base_core::system_handlers::audit::AuditHandler;
use event_base_core::system_handlers::metrics::MetricsHandler;
use event_base_core::system_handlers::shutdown::ShutdownAckHandler;
use event_base_core::system_handlers::topic::{
    TopicDiscovery, TopicDiscoveryMessage, TopicSyncMessage,
};
use event_base_core::system_handlers::trace::{SystemTraceHandler, TraceCollector};
use event_base_core::system_handlers::worker::{WorkerDiscoveryHandler, WorkerHeartbeatHandler};
use event_base_core::trace::TraceRecord;
use event_base_core::wal::sync::WalSyncMessage;
use event_base_core::wal::wal::WalRecordState;
use event_base_core::worker_registry::{WorkerDiscoveryMessage, WorkerHeartbeatMessage};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

fn message(topic: &str, payload: &[u8]) -> EMessage {
    EMessage::new(
        MessageTopic(topic.to_string()),
        MessagePayload(payload.to_vec()),
        DeliveryMode::Standard,
        None,
    )
}

fn trace_record_bytes() -> Vec<u8> {
    let record = TraceRecord {
        trace_id: Some("trace-1".to_string()),
        span_id: "span-1".to_string(),
        parent_span_id: None,
        name: "test-event".to_string(),
        target: "test".to_string(),
        level: event_base_core::trace::TraceLevel::Info,
        fields: HashMap::new(),
        started_at: Some(SystemTime::now()),
        finished_at: Some(SystemTime::now()),
        duration: Some(Duration::from_millis(5)),
        message_id: Some("msg-1".to_string()),
        worker_id: Some("worker-a".to_string()),
        topic: Some("orders".to_string()),
    };
    serde_json::to_vec(&record).unwrap()
}

// ──────────────────────────────────────────────
// Individual handler tests (no singletons needed)
// ──────────────────────────────────────────────

#[tokio::test]
async fn audit_handler_acks_invalid_message() {
    let handler = AuditHandler {};
    let msg = message(SYSTEM_TOPIC_AUDIT, b"not-json");
    let ack = handler.handler(&msg).await;
    assert!(matches!(ack, Ack::Ack));
}

#[tokio::test]
async fn metrics_handler_acks_invalid_message() {
    let handler = MetricsHandler {};
    let msg = message(SYSTEM_TOPIC_METRICS, b"bad-data");
    let ack = handler.handler(&msg).await;
    assert!(matches!(ack, Ack::Ack));
}

#[tokio::test]
async fn shutdown_ack_handler_handles_invalid_message() {
    let handler = ShutdownAckHandler {};
    let msg = message(SYSTEM_TOPIC_SHUTDOWN_ACK, b"invalid");
    let result = handler.handler(&msg).await;
    assert!(matches!(result, Ack::Ack));
}

#[tokio::test]
async fn topic_discovery_handler_handles_invalid_message() {
    let handler = TopicDiscovery {};
    let msg = message("_system.topic_discovery", b"invalid");
    let result = handler.handler(&msg).await;
    assert!(matches!(result, Ack::Ack));
}

#[tokio::test]
async fn trace_handler_acks_valid_message() {
    let collector = Arc::new(RecordingTraceCollector::new());
    let handler = SystemTraceHandler::new(vec![collector.clone()]);
    let msg = message(SYSTEM_TOPIC_TRACE, &trace_record_bytes());
    let result = handler.handler(&msg).await;
    assert!(matches!(result, Ack::Ack));
    assert_eq!(collector.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn trace_handler_acks_invalid_message() {
    let collector = Arc::new(RecordingTraceCollector::new());
    let handler = SystemTraceHandler::new(vec![collector]);
    let msg = message(SYSTEM_TOPIC_TRACE, b"invalid");
    let result = handler.handler(&msg).await;
    assert!(matches!(result, Ack::Ack));
}

#[tokio::test]
async fn trace_handler_calls_all_collectors() {
    let c1 = Arc::new(RecordingTraceCollector::new());
    let c2 = Arc::new(RecordingTraceCollector::new());
    let handler = SystemTraceHandler::new(vec![c1.clone(), c2.clone()]);
    let msg = message(SYSTEM_TOPIC_TRACE, &trace_record_bytes());
    let result = handler.handler(&msg).await;
    assert!(matches!(result, Ack::Ack));
    assert_eq!(c1.count.load(Ordering::SeqCst), 1);
    assert_eq!(c2.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn trace_handler_continues_on_collector_error() {
    let failing = Arc::new(FailingTraceCollector);
    let c2 = Arc::new(RecordingTraceCollector::new());
    let handler = SystemTraceHandler::new(vec![failing, c2.clone()]);
    let msg = message(SYSTEM_TOPIC_TRACE, &trace_record_bytes());
    let result = handler.handler(&msg).await;
    assert!(matches!(result, Ack::Ack));
    assert_eq!(c2.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn worker_discovery_handler_handles_invalid_message() {
    let handler = WorkerDiscoveryHandler {};
    let msg = message(SYSTEM_TOPIC_WORKER_DISCOVERY, b"invalid");
    let result = handler.handler(&msg).await;
    assert!(matches!(result, Ack::Ack));
}

#[tokio::test]
async fn worker_heartbeat_handler_handles_invalid_message() {
    let handler = WorkerHeartbeatHandler {};
    let msg = message(SYSTEM_TOPIC_WORKER_HEARTBEAT, b"invalid");
    let result = handler.handler(&msg).await;
    assert!(matches!(result, Ack::Ack));
}

// ──────────────────────────────────────────────
// Test helpers
// ──────────────────────────────────────────────

struct RecordingTraceCollector {
    count: AtomicUsize,
}

impl RecordingTraceCollector {
    fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl TraceCollector for RecordingTraceCollector {
    async fn collect(
        &self,
        _record: &TraceRecord,
    ) -> Result<(), event_base_core::error::CoreError> {
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct FailingTraceCollector;

#[async_trait]
impl TraceCollector for FailingTraceCollector {
    async fn collect(
        &self,
        _record: &TraceRecord,
    ) -> Result<(), event_base_core::error::CoreError> {
        Err(event_base_core::error::CoreError::Other(
            "failed".to_string(),
        ))
    }
}

// ──────────────────────────────────────────────
// Serialization tests for system message types
// ──────────────────────────────────────────────

#[test]
fn topic_discovery_message_serialization_roundtrip() {
    let msg = TopicDiscoveryMessage {
        has_topics: vec!["orders".to_string(), "payments".to_string()],
    };
    let bytes = bincode::encode_to_vec(&msg, bincode::config::standard()).expect("serialize");
    let decoded: TopicDiscoveryMessage = bincode::decode_from_slice(&bytes, bincode::config::standard()).expect("deserialize").0;
    assert_eq!(decoded.has_topics.len(), 2);
    assert!(decoded.has_topics.contains(&"orders".to_string()));
}

#[test]
fn topic_discovery_message_empty_topics() {
    let msg = TopicDiscoveryMessage { has_topics: vec![] };
    let bytes = bincode::encode_to_vec(&msg, bincode::config::standard()).expect("serialize");
    let decoded: TopicDiscoveryMessage = bincode::decode_from_slice(&bytes, bincode::config::standard()).expect("deserialize").0;
    assert!(decoded.has_topics.is_empty());
}

#[test]
fn topic_sync_message_serialization_roundtrip() {
    let msg = TopicSyncMessage {
        topics: vec!["t1".to_string(), "t2".to_string(), "t3".to_string()],
    };
    let bytes = bincode::encode_to_vec(&msg, bincode::config::standard()).expect("serialize");
    let decoded: TopicSyncMessage = bincode::decode_from_slice(&bytes, bincode::config::standard()).expect("deserialize").0;
    assert_eq!(decoded.topics.len(), 3);
}

#[test]
fn topic_sync_message_empty_topics() {
    let msg = TopicSyncMessage { topics: vec![] };
    let bytes = bincode::encode_to_vec(&msg, bincode::config::standard()).expect("serialize");
    let decoded: TopicSyncMessage = bincode::decode_from_slice(&bytes, bincode::config::standard()).expect("deserialize").0;
    assert!(decoded.topics.is_empty());
}

#[test]
fn wal_sync_message_serialization_roundtrip() {
    let msg = WalSyncMessage {
        message_id: "msg-1".to_string(),
        topic: "orders".to_string(),
        worker_id: "worker-x".to_string(),
        status: WalRecordState::Processing,
        attempts: 1,
        last_attempt_at: SystemTime::now(),
        error: None,
        timestamp: SystemTime::now(),
    };
    let bytes = bincode::encode_to_vec(&msg, bincode::config::standard()).expect("serialize");
    let decoded: WalSyncMessage = bincode::decode_from_slice(&bytes, bincode::config::standard()).expect("deserialize").0;
    assert_eq!(decoded.message_id, "msg-1");
    assert_eq!(decoded.attempts, 1);
    assert!(decoded.error.is_none());
}

#[test]
fn wal_sync_message_with_error_field() {
    let msg = WalSyncMessage {
        message_id: "msg-err".to_string(),
        topic: "orders".to_string(),
        worker_id: "worker-y".to_string(),
        status: WalRecordState::Failed,
        attempts: 3,
        last_attempt_at: SystemTime::now(),
        error: Some("handler timeout".to_string()),
        timestamp: SystemTime::now(),
    };
    let bytes = bincode::encode_to_vec(&msg, bincode::config::standard()).expect("serialize");
    let decoded: WalSyncMessage = bincode::decode_from_slice(&bytes, bincode::config::standard()).expect("deserialize").0;
    assert_eq!(decoded.error.as_deref(), Some("handler timeout"));
    assert_eq!(decoded.status, WalRecordState::Failed);
}

#[test]
fn wal_sync_message_all_states() {
    for state in &[
        WalRecordState::Pending,
        WalRecordState::Processing,
        WalRecordState::Complete,
        WalRecordState::Failed,
    ] {
        let msg = WalSyncMessage {
            message_id: "msg".to_string(),
            topic: "t".to_string(),
            worker_id: "w".to_string(),
            status: *state,
            attempts: 0,
            last_attempt_at: SystemTime::now(),
            error: None,
            timestamp: SystemTime::now(),
        };
        let bytes = bincode::encode_to_vec(&msg, bincode::config::standard()).expect("serialize");
        let decoded: WalSyncMessage = bincode::decode_from_slice(&bytes, bincode::config::standard()).expect("deserialize").0;
        assert_eq!(decoded.status, *state);
    }
}

#[test]
fn worker_discovery_message_serialization_roundtrip() {
    let msg = WorkerDiscoveryMessage {
        worker_name: "worker-a".to_string(),
        topic: "orders".to_string(),
        started_at: SystemTime::now(),
    };
    let bytes = bincode::encode_to_vec(&msg, bincode::config::standard()).expect("serialize");
    let decoded: WorkerDiscoveryMessage = bincode::decode_from_slice(&bytes, bincode::config::standard()).expect("deserialize").0;
    assert_eq!(decoded.worker_name, "worker-a");
    assert_eq!(decoded.topic, "orders");
}

#[test]
fn worker_heartbeat_message_serialization_roundtrip() {
    let msg = WorkerHeartbeatMessage {
        worker_name: "worker-b".to_string(),
        timestamp: SystemTime::now(),
    };
    let bytes = bincode::encode_to_vec(&msg, bincode::config::standard()).expect("serialize");
    let decoded: WorkerHeartbeatMessage = bincode::decode_from_slice(&bytes, bincode::config::standard()).expect("deserialize").0;
    assert_eq!(decoded.worker_name, "worker-b");
}

#[test]
fn system_topic_constants_are_distinct() {
    let topics = vec![
        SYSTEM_TOPIC_AUDIT,
        SYSTEM_TOPIC_TRACE,
        SYSTEM_TOPIC_SHUTDOWN,
        SYSTEM_TOPIC_SHUTDOWN_ACK,
        SYSTEM_TOPIC_WAL_SYNC,
        SYSTEM_TOPIC_WORKER_DISCOVERY,
        SYSTEM_TOPIC_WORKER_HEARTBEAT,
        SYSTEM_TOPIC_METRICS,
        SYSTEM_TOPIC_TOPIC_DISCOVERY,
        SYSTEM_TOPIC_TOPIC_SYNC,
    ];
    // All must be distinct
    let mut seen = std::collections::HashSet::new();
    for t in &topics {
        assert!(seen.insert(*t), "duplicate topic: {}", t);
    }
    // All must start with _system
    for t in &topics {
        assert!(t.starts_with("_system."), "not a system topic: {}", t);
    }
}

#[test]
fn trace_level_debug_format() {
    use event_base_core::trace::TraceLevel;
    assert_eq!(format!("{:?}", TraceLevel::Trace), "Trace");
    assert_eq!(format!("{:?}", TraceLevel::Debug), "Debug");
    assert_eq!(format!("{:?}", TraceLevel::Info), "Info");
    assert_eq!(format!("{:?}", TraceLevel::Warn), "Warn");
    assert_eq!(format!("{:?}", TraceLevel::Error), "Error");
}

#[test]
fn trace_level_clone_and_eq() {
    use event_base_core::trace::TraceLevel;
    assert_eq!(TraceLevel::Info, TraceLevel::Info);
    assert_eq!(TraceLevel::Info.clone(), TraceLevel::Info);
    assert_ne!(TraceLevel::Info, TraceLevel::Error);
    assert_ne!(TraceLevel::Trace, TraceLevel::Warn);
}
