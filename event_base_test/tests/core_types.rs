use async_trait::async_trait;
use event_base_core::dead_letter::{DeadLetterMessage, DeadReason};
use event_base_core::error::CoreError;
use event_base_core::error::handler::HandlerError;
use event_base_core::error::middleware::MiddlewareError;
use event_base_core::error::queue::QueueError;
use event_base_core::error::serialize::SerializeError;
use event_base_core::error::shutdown::ShutdownError;
use event_base_core::error::topic::TopicError;
use event_base_core::error::wal::WalError;
use event_base_core::handler::{Ack, EHandler};
use event_base_core::message::{
    DeliveryMode, EMessage, MessageMetadata, MessagePayload, MessageTopic,
};
use event_base_core::shutdown::shutdown_channel;
use event_base_core::trace::{TraceLevel, TraceRecord};
use event_base_core::traits::codec::Codec;
use event_base_core::wal::wal::{WalRecord, WalRecordState};
use event_base_core::{NodeType, get_node_name, get_node_type, set_node_name, set_node_type};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

// ──────────────────────────────────────────────
// NodeType and globals
// ──────────────────────────────────────────────

#[test]
fn node_type_has_correct_discriminants() {
    assert_eq!(NodeType::Host as u8, 0);
    assert_eq!(NodeType::Worker as u8, 1);
}

#[test]
fn node_type_debug_and_clone() {
    let host = NodeType::Host;
    let worker = NodeType::Worker;
    assert_eq!(format!("{:?}", host), "Host");
    assert_eq!(format!("{:?}", worker), "Worker");
    assert_eq!(host.clone(), host);
    assert_ne!(host, worker);
}

#[test]
fn node_name_and_type_roundtrip() {
    set_node_name("my-node".to_string());
    set_node_type(NodeType::Worker);
    assert_eq!(get_node_name(), "my-node");
    assert_eq!(*get_node_type(), NodeType::Worker);
}

// ──────────────────────────────────────────────
// EMessage tests
// ──────────────────────────────────────────────

#[test]
fn message_new_sets_fields_correctly() {
    let topic = MessageTopic("test.topic".to_string());
    let payload = MessagePayload(vec![10, 20, 30]);
    let msg = EMessage::new(
        topic.clone(),
        payload.clone(),
        DeliveryMode::Broadcast,
        Some("target-worker".to_string()),
    );
    assert!(!msg.id.is_empty());
    assert_eq!(msg.topic, topic);
    assert_eq!(msg.payload, payload);
    assert_eq!(msg.delivery_mode, DeliveryMode::Broadcast);
    assert_eq!(msg.to_worker, Some("target-worker".to_string()));
    assert_eq!(msg.attempts, 0);
    assert_eq!(msg.consumed_count, 0);
    assert_eq!(msg.version, 0);
    assert!(msg.deliver_at.is_none());
}

#[test]
fn message_default_has_empty_id() {
    let msg = EMessage::default();
    assert_eq!(msg.id, "");
    assert_eq!(msg.topic.0, "");
    assert!(msg.payload.0.is_empty());
    assert_eq!(msg.delivery_mode, DeliveryMode::Standard);
    assert!(msg.to_worker.is_none());
}

#[test]
fn message_with_repeated_delivery() {
    let msg = EMessage::new(
        MessageTopic("r".to_string()),
        MessagePayload(vec![]),
        DeliveryMode::Repeated(5),
        None,
    );
    assert_eq!(msg.delivery_mode, DeliveryMode::Repeated(5));
}

#[test]
fn message_metadata_defaults() {
    let meta = MessageMetadata {
        created_at: SystemTime::now(),
        trace_id: None,
        correlation_id: None,
        causation_id: None,
        source: None,
    };
    assert!(meta.trace_id.is_none());
    assert!(meta.correlation_id.is_none());
    assert!(meta.causation_id.is_none());
    assert!(meta.source.is_none());
}

#[test]
fn message_metadata_with_all_fields() {
    let meta = MessageMetadata {
        created_at: SystemTime::now(),
        trace_id: Some("trace-abc".to_string()),
        correlation_id: Some("corr-123".to_string()),
        causation_id: Some("cause-456".to_string()),
        source: Some("my-service".to_string()),
    };
    assert_eq!(meta.trace_id.as_deref(), Some("trace-abc"));
    assert_eq!(meta.correlation_id.as_deref(), Some("corr-123"));
    assert_eq!(meta.causation_id.as_deref(), Some("cause-456"));
    assert_eq!(meta.source.as_deref(), Some("my-service"));
}

#[test]
fn message_clone_preserves_all_fields() {
    let msg = EMessage::new(
        MessageTopic("orders".to_string()),
        MessagePayload(vec![1, 2, 3]),
        DeliveryMode::Repeated(3),
        Some("worker-a".to_string()),
    );
    let cloned = msg.clone();
    assert_eq!(cloned.id, msg.id);
    assert_eq!(cloned.topic, msg.topic);
    assert_eq!(cloned.payload, msg.payload);
    assert_eq!(cloned.delivery_mode, msg.delivery_mode);
    assert_eq!(cloned.to_worker, msg.to_worker);
}

// ──────────────────────────────────────────────
// DeliveryMode tests
// ──────────────────────────────────────────────

#[test]
fn delivery_mode_partial_eq() {
    assert_eq!(DeliveryMode::Standard, DeliveryMode::Standard);
    assert_eq!(DeliveryMode::Repeated(3), DeliveryMode::Repeated(3));
    assert_ne!(DeliveryMode::Repeated(3), DeliveryMode::Repeated(5));
    assert_eq!(DeliveryMode::Broadcast, DeliveryMode::Broadcast);
}

// ──────────────────────────────────────────────
// DeadLetterMessage tests
// ──────────────────────────────────────────────

#[test]
fn dead_letter_message_creation() {
    let original = EMessage::new(
        MessageTopic("orders".to_string()),
        MessagePayload(vec![9, 8, 7]),
        DeliveryMode::Standard,
        None,
    );
    let dl = DeadLetterMessage {
        original_message: original.clone(),
        dead_reason: DeadReason::MaxRetriesExceeded,
        died_at: SystemTime::now(),
        attempts: 5,
    };
    assert_eq!(dl.original_message.id, original.id);
    assert!(matches!(dl.dead_reason, DeadReason::MaxRetriesExceeded));
    assert_eq!(dl.attempts, 5);
}

#[test]
fn dead_reason_display() {
    assert_eq!(
        DeadReason::MaxRetriesExceeded.to_string(),
        "Max Retries exceeded"
    );
    assert_eq!(DeadReason::Explicit.to_string(), "Handler Explicit");
    assert_eq!(DeadReason::Timeout.to_string(), "Handler Timeout");
    assert_eq!(DeadReason::NoHandler.to_string(), "NoHandler");
    assert_eq!(
        DeadReason::Other("custom".to_string()).to_string(),
        "Handler Other Error: custom"
    );
}

#[test]
fn dead_letter_message_serialization_roundtrip() {
    let original = EMessage::new(
        MessageTopic("dl".to_string()),
        MessagePayload(vec![1, 2, 3]),
        DeliveryMode::Standard,
        None,
    );
    let dl = DeadLetterMessage {
        original_message: original,
        dead_reason: DeadReason::Timeout,
        died_at: SystemTime::now(),
        attempts: 3,
    };
    let json = serde_json::to_vec(&dl).expect("serialize");
    let decoded: DeadLetterMessage = serde_json::from_slice(&json).expect("deserialize");
    assert_eq!(decoded.attempts, 3);
    assert!(matches!(decoded.dead_reason, DeadReason::Timeout));
}

// ──────────────────────────────────────────────
// WalRecord tests
// ──────────────────────────────────────────────

#[test]
fn wal_record_from_msg_defaults() {
    let msg = EMessage::default();
    let record = WalRecord::from_msg(msg.clone());
    assert_eq!(record.record_id, 0);
    assert_eq!(record.message.id, msg.id);
    assert_eq!(record.status, WalRecordState::Pending);
    assert!(!record.is_dead_letter);
    assert!(record.dead_reason.is_none());
    assert!(record.last_attempt_at.is_none());
}

#[test]
fn wal_record_state_discriminants() {
    assert_eq!(WalRecordState::Pending as u8, 0);
    assert_eq!(WalRecordState::Processing as u8, 1);
    assert_eq!(WalRecordState::Complete as u8, 2);
    assert_eq!(WalRecordState::Failed as u8, 3);
}

#[test]
fn wal_record_state_clone_and_eq() {
    assert_eq!(WalRecordState::Pending, WalRecordState::Pending);
    assert_ne!(WalRecordState::Pending, WalRecordState::Complete);
    assert_eq!(
        WalRecordState::Processing.clone(),
        WalRecordState::Processing
    );
}

// ──────────────────────────────────────────────
// Ack handler tests
// ──────────────────────────────────────────────

#[tokio::test]
async fn boxed_handler_delegates_to_inner() {
    struct TestHandler;
    #[async_trait]
    impl EHandler for TestHandler {
        async fn handler(&self, _msg: &EMessage) -> Ack {
            Ack::Ack
        }
    }

    let boxed: Box<dyn EHandler> = Box::new(TestHandler);
    let msg = EMessage::default();
    let result = boxed.handler(&msg).await;
    assert!(matches!(result, Ack::Ack));
}

#[tokio::test]
async fn boxed_handler_can_return_dead() {
    struct DeadHandler;
    #[async_trait]
    impl EHandler for DeadHandler {
        async fn handler(&self, _msg: &EMessage) -> Ack {
            Ack::Dead {
                dead_reason: DeadReason::Explicit,
            }
        }
    }

    let boxed: Box<dyn EHandler> = Box::new(DeadHandler);
    let msg = EMessage::default();
    let result = boxed.handler(&msg).await;
    match result {
        Ack::Dead { dead_reason } => {
            assert!(matches!(dead_reason, DeadReason::Explicit));
        }
        _ => panic!("expected Dead"),
    }
}

#[tokio::test]
async fn boxed_handler_can_return_noack() {
    struct NoAckHandler;
    #[async_trait]
    impl EHandler for NoAckHandler {
        async fn handler(&self, _msg: &EMessage) -> Ack {
            Ack::NoAck {
                retry_after: Some(Duration::from_secs(5)),
                max_retries: 3,
            }
        }
    }

    let boxed: Box<dyn EHandler> = Box::new(NoAckHandler);
    let msg = EMessage::default();
    let result = boxed.handler(&msg).await;
    match result {
        Ack::NoAck {
            retry_after,
            max_retries,
        } => {
            assert_eq!(retry_after, Some(Duration::from_secs(5)));
            assert_eq!(max_retries, 3);
        }
        _ => panic!("expected NoAck"),
    }
}

// ──────────────────────────────────────────────
// TraceRecord tests
// ──────────────────────────────────────────────

#[test]
fn trace_record_creation() {
    let record = TraceRecord {
        trace_id: Some("trace-1".to_string()),
        span_id: "span-1".to_string(),
        parent_span_id: Some("parent-1".to_string()),
        name: "operation".to_string(),
        target: "my_module".to_string(),
        level: TraceLevel::Info,
        fields: HashMap::from([("key".to_string(), serde_json::json!("value"))]),
        started_at: Some(SystemTime::now()),
        finished_at: None,
        duration: None,
        message_id: Some("msg-1".to_string()),
        worker_id: Some("worker-a".to_string()),
        topic: Some("orders".to_string()),
    };
    assert_eq!(record.span_id, "span-1");
    assert_eq!(record.name, "operation");
    assert_eq!(record.level, TraceLevel::Info);
    assert_eq!(record.fields.get("key"), Some(&serde_json::json!("value")));
}

#[test]
fn trace_record_serialization_roundtrip() {
    let record = TraceRecord {
        trace_id: None,
        span_id: "span-1".to_string(),
        parent_span_id: None,
        name: "test".to_string(),
        target: "target".to_string(),
        level: TraceLevel::Error,
        fields: HashMap::new(),
        started_at: None,
        finished_at: None,
        duration: Some(Duration::from_millis(100)),
        message_id: None,
        worker_id: None,
        topic: None,
    };
    let json = serde_json::to_vec(&record).expect("serialize");
    let decoded: TraceRecord = serde_json::from_slice(&json).expect("deserialize");
    assert_eq!(decoded.name, "test");
    assert_eq!(decoded.level, TraceLevel::Error);
    assert_eq!(decoded.duration, Some(Duration::from_millis(100)));
}

#[test]
fn trace_level_variants() {
    assert_ne!(TraceLevel::Trace, TraceLevel::Debug);
    assert_ne!(TraceLevel::Debug, TraceLevel::Info);
    assert_ne!(TraceLevel::Info, TraceLevel::Warn);
    assert_ne!(TraceLevel::Warn, TraceLevel::Error);
    assert_eq!(TraceLevel::Info, TraceLevel::Info);
}

// ──────────────────────────────────────────────
// Error display tests
// ──────────────────────────────────────────────

#[test]
fn error_queue_display() {
    assert_eq!(QueueError::Full.to_string(), "Queue is full");
    assert_eq!(QueueError::Closed.to_string(), "Queue is closed");
    assert_eq!(QueueError::Timeout.to_string(), "Send timeout");
    assert_eq!(
        QueueError::Send("oops".to_string()).to_string(),
        "Send error: oops"
    );
    assert_eq!(
        QueueError::InvalidClaimId("c1".to_string()).to_string(),
        "Invalid Claim Id: c1"
    );
    assert_eq!(
        QueueError::Receive("fail".to_string()).to_string(),
        "Receive error: fail"
    );
}

#[test]
fn error_handler_display() {
    assert_eq!(
        HandlerError::NotFound("topic".to_string()).to_string(),
        "Msg Handler Not Found: topic"
    );
    assert_eq!(
        HandlerError::Error("boom".to_string()).to_string(),
        "Msg Handler Error: boom"
    );
}

#[test]
fn error_middleware_display() {
    assert_eq!(
        MiddlewareError::Execution("fail".to_string()).to_string(),
        "Execution failed: fail"
    );
    assert_eq!(
        MiddlewareError::Interrupted.to_string(),
        "Middleware chain interrupted"
    );
}

#[test]
fn error_serialize_display() {
    assert_eq!(
        SerializeError::SerializeError("enc".to_string()).to_string(),
        "Serialize error: enc"
    );
    assert_eq!(
        SerializeError::DeserializeError("dec".to_string()).to_string(),
        "Deserialize error: dec"
    );
}

#[test]
fn error_shutdown_display() {
    assert_eq!(
        ShutdownError::Timeout(Duration::from_secs(5)).to_string(),
        "Timeout: 5s"
    );
    assert_eq!(
        ShutdownError::ComponentNotFound("db".to_string()).to_string(),
        "Component 'db' not found"
    );
    assert_eq!(
        ShutdownError::ComponentFailed("svc".to_string(), "err".to_string()).to_string(),
        "Component 'svc' shutdown failed: err"
    );
}

#[test]
fn error_topic_display() {
    assert_eq!(
        TopicError::AlreadyExists("orders".to_string()).to_string(),
        "Topic already exists: orders"
    );
    assert_eq!(
        TopicError::NotFound("missing".to_string()).to_string(),
        "Topic Not Found: missing"
    );
}

#[test]
fn error_wal_display() {
    assert_eq!(
        WalError::RecordNotFound("r1".to_string()).to_string(),
        "Record not found: r1"
    );
    assert_eq!(
        WalError::Corrupted("bad data".to_string()).to_string(),
        "WAL corrupted: bad data"
    );
    assert_eq!(
        WalError::Backend("disk full".to_string()).to_string(),
        "Backend error: disk full"
    );
    assert_eq!(
        WalError::Write("io error".to_string()).to_string(),
        "Write error: io error"
    );
}

#[test]
fn error_core_error_display() {
    let err = CoreError::AlreadyInitialized;
    assert_eq!(err.to_string(), "Object already exists");

    let err = CoreError::ErrorTime;
    assert_eq!(err.to_string(), "Error Time");

    let err = CoreError::ShuttingDown;
    assert_eq!(err.to_string(), "Shutting down");

    let err = CoreError::Timeout(Duration::from_secs(10));
    assert_eq!(err.to_string(), "Timeout: 10s");

    let err = CoreError::InvalidParameter("bad".to_string());
    assert_eq!(err.to_string(), "Invalid Parameter: bad");

    let err = CoreError::InvalidData("bad".to_string());
    assert_eq!(err.to_string(), "Invalid Type: bad");

    let err = CoreError::WorkerNotFound("w1".to_string());
    assert_eq!(err.to_string(), "Worker Not Found: w1");

    let err = CoreError::Unsupported("feature".to_string());
    assert_eq!(err.to_string(), "Unsupported: feature");

    let err = CoreError::QueueSendError("full".to_string());
    assert_eq!(err.to_string(), "Queue Send Error: full");

    let err = CoreError::TaskJoinError("panic".to_string());
    assert_eq!(err.to_string(), "Task Join Error: panic");

    let err = CoreError::Other("misc".to_string());
    assert_eq!(err.to_string(), "Other: misc");
}

#[test]
fn error_core_error_from_queue() {
    let err = CoreError::from(QueueError::Full);
    assert!(err.to_string().contains("Queue error"));
}

#[test]
fn error_core_error_from_wal() {
    let err = CoreError::from(WalError::RecordNotFound("r1".to_string()));
    assert!(err.to_string().contains("WAL error"));
}

#[test]
fn error_core_error_from_topic() {
    let err = CoreError::from(TopicError::NotFound("x".to_string()));
    assert!(err.to_string().contains("Topic error"));
}

#[test]
fn error_core_error_from_handler() {
    let err = CoreError::from(HandlerError::Error("e".to_string()));
    assert!(err.to_string().contains("Handler error"));
}

#[test]
fn error_core_error_from_middleware() {
    let err = CoreError::from(MiddlewareError::Interrupted);
    assert!(err.to_string().contains("Middleware error"));
}

#[test]
fn error_core_error_from_serialize() {
    let err = CoreError::from(SerializeError::SerializeError("e".to_string()));
    assert!(err.to_string().contains("Serialization error"));
}

#[test]
fn error_core_error_from_shutdown() {
    let err = CoreError::from(ShutdownError::Timeout(Duration::from_secs(1)));
    assert!(err.to_string().contains("Shutdown error"));
}

// ──────────────────────────────────────────────
// Codec trait
// ──────────────────────────────────────────────

struct IdentityCodec;

impl Codec for IdentityCodec {
    fn encode(&self, msg: &EMessage) -> Result<Vec<u8>, CoreError> {
        serde_json::to_vec(msg).map_err(|e| CoreError::Other(e.to_string()))
    }

    fn decode(&self, data: &[u8]) -> Result<EMessage, CoreError> {
        serde_json::from_slice(data).map_err(|e| CoreError::Other(e.to_string()))
    }
}

#[test]
fn custom_codec_roundtrip() {
    let codec = IdentityCodec;
    let msg = EMessage::new(
        MessageTopic("codec-test".to_string()),
        MessagePayload(vec![5, 6, 7]),
        DeliveryMode::Standard,
        None,
    );
    let encoded = codec.encode(&msg).expect("encode");
    let decoded = codec.decode(&encoded).expect("decode");
    assert_eq!(decoded.id, msg.id);
    assert_eq!(decoded.topic, msg.topic);
    assert_eq!(decoded.payload, msg.payload);
}

#[test]
fn codec_decode_invalid_data_returns_error() {
    let codec = IdentityCodec;
    let result = codec.decode(b"not-json");
    assert!(result.is_err());
}

// ──────────────────────────────────────────────
// Constant values
// ──────────────────────────────────────────────

#[test]
fn system_topic_constants_are_correct() {
    assert_eq!(
        event_base_core::constant::SYSTEM_TOPIC_AUDIT,
        "_system.audit"
    );
    assert_eq!(
        event_base_core::constant::SYSTEM_TOPIC_TRACE,
        "_system.trace"
    );
    assert_eq!(
        event_base_core::constant::SYSTEM_TOPIC_SHUTDOWN,
        "_system.shutdown"
    );
    assert_eq!(
        event_base_core::constant::SYSTEM_TOPIC_SHUTDOWN_ACK,
        "_system.shutdown_ack"
    );
    assert_eq!(
        event_base_core::constant::SYSTEM_TOPIC_WAL_SYNC,
        "_system.wal_sync"
    );
    assert_eq!(
        event_base_core::constant::SYSTEM_TOPIC_WORKER_DISCOVERY,
        "_system.worker_discovery"
    );
    assert_eq!(
        event_base_core::constant::SYSTEM_TOPIC_WORKER_HEARTBEAT,
        "_system.worker_heartbeat"
    );
    assert_eq!(
        event_base_core::constant::SYSTEM_TOPIC_METRICS,
        "_system.metrics"
    );
    assert_eq!(
        event_base_core::constant::SYSTEM_TOPIC_TOPIC_DISCOVERY,
        "_system.topic_discovery"
    );
    assert_eq!(
        event_base_core::constant::SYSTEM_TOPIC_TOPIC_SYNC,
        "_system.topic_sync"
    );
}

// ──────────────────────────────────────────────
// Shutdown channel
// ──────────────────────────────────────────────

#[tokio::test]
async fn shutdown_channel_multiple_receivers() {
    let (tx, rx1) = shutdown_channel();
    let mut rx2 = rx1;
    tx.send(()).expect("send");
    assert!(rx2.try_recv().is_ok());
}

#[test]
fn message_topic_default_and_construction() {
    let topic = MessageTopic("test".to_string());
    assert_eq!(topic.0, "test");
    let default = MessageTopic::default();
    assert_eq!(default.0, "");
}

#[test]
fn message_payload_default_and_construction() {
    let payload = MessagePayload(vec![1, 2, 3]);
    assert_eq!(payload.0, vec![1, 2, 3]);
    let default = MessagePayload::default();
    assert!(default.0.is_empty());
}

#[test]
fn delivery_mode_debug() {
    let d = format!("{:?}", DeliveryMode::Standard);
    assert_eq!(d, "Standard");
    let d = format!("{:?}", DeliveryMode::Repeated(3));
    assert_eq!(d, "Repeated(3)");
    let d = format!("{:?}", DeliveryMode::Broadcast);
    assert_eq!(d, "Broadcast");
}
