use event_base_core::dead_letter::{DeadLetterMessage, DeadReason};
use event_base_core::error::handler::HandlerError;
use event_base_core::error::queue::QueueError;
use event_base_core::error::serialize::SerializeError;
use event_base_core::error::shutdown::ShutdownError;
use event_base_core::error::topic::TopicError;
use event_base_core::error::wal::WalError;
use event_base_core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use event_base_core::shutdown::messages::{
    ShutdownAck, ShutdownCommand, ShutdownStatus, ShutdownStrategy,
};
use event_base_core::shutdown::shutdown_channel;
use event_base_core::wal::codec::{BincodeCodec, WalRecordCodec};
use event_base_core::wal::wal::{WalRecord, WalRecordState};
use std::time::{Duration, SystemTime};

#[test]
fn message_and_wal_codec_roundtrip() {
    let mut message = EMessage::new(
        MessageTopic("orders.created".to_string()),
        MessagePayload(vec![1, 2, 3, 4]),
        DeliveryMode::Repeated(3),
        Some("worker-a".to_string()),
    );
    message.deliver_at = Some(SystemTime::now() + Duration::from_secs(30));

    let default_message = EMessage::default();
    assert!(default_message.id.is_empty());
    assert_eq!(default_message.delivery_mode, DeliveryMode::Standard);

    let mut record = WalRecord::from_msg(message.clone());
    record.record_id = 42;
    record.status = WalRecordState::Processing;
    record.last_attempt_at = Some(SystemTime::now());
    record.is_dead_letter = true;
    record.dead_reason = Some(DeadReason::Other("boom".to_string()));

    let codec = BincodeCodec;
    let encoded = codec.encode(&record).expect("record should encode");
    let decoded = codec.decode(&encoded).expect("record should decode");

    assert_eq!(decoded.record_id, 42);
    assert_eq!(decoded.message.id, message.id);
    assert_eq!(decoded.message.topic, message.topic);
    assert_eq!(decoded.message.payload, message.payload);
    assert_eq!(decoded.message.delivery_mode, message.delivery_mode);
    assert_eq!(decoded.message.to_worker, message.to_worker);
    assert_eq!(decoded.status, WalRecordState::Processing);
    assert!(decoded.is_dead_letter);
    assert!(matches!(
        decoded.dead_reason,
        Some(DeadReason::Other(ref message)) if message == "boom"
    ));
}

#[test]
fn error_display_strings_cover_variants() {
    let queue_errors = [
        QueueError::Full.to_string(),
        QueueError::Closed.to_string(),
        QueueError::Timeout.to_string(),
        QueueError::Send("send-failed".to_string()).to_string(),
        QueueError::InvalidClaimId("claim-1".to_string()).to_string(),
        QueueError::Receive("recv-failed".to_string()).to_string(),
    ];
    assert!(
        queue_errors
            .iter()
            .any(|text| text.contains("Queue is full"))
    );
    assert!(
        queue_errors
            .iter()
            .any(|text| text.contains("Invalid Claim Id"))
    );

    let topic_errors = [
        TopicError::AlreadyExists("orders".to_string()).to_string(),
        TopicError::NotFound("missing".to_string()).to_string(),
    ];
    assert!(
        topic_errors
            .iter()
            .any(|text| text.contains("Topic already exists"))
    );
    assert!(
        topic_errors
            .iter()
            .any(|text| text.contains("Topic Not Found"))
    );

    let handler_errors = [
        HandlerError::NotFound("topic".to_string()).to_string(),
        HandlerError::Error("boom".to_string()).to_string(),
    ];
    assert!(
        handler_errors
            .iter()
            .any(|text| text.contains("Msg Handler Not Found"))
    );
    assert!(
        handler_errors
            .iter()
            .any(|text| text.contains("Msg Handler Error"))
    );

    let wal_errors = [
        WalError::RecordNotFound("record-1".to_string()).to_string(),
        WalError::Corrupted("bad".to_string()).to_string(),
        WalError::Backend("backend".to_string()).to_string(),
        WalError::Write("write".to_string()).to_string(),
    ];
    assert!(
        wal_errors
            .iter()
            .any(|text| text.contains("Record not found"))
    );
    assert!(wal_errors.iter().any(|text| text.contains("WAL corrupted")));

    let serialize_errors = [
        SerializeError::SerializeError("serialize".to_string()).to_string(),
        SerializeError::DeserializeError("deserialize".to_string()).to_string(),
    ];
    assert!(
        serialize_errors
            .iter()
            .any(|text| text.contains("Serialize error"))
    );
    assert!(
        serialize_errors
            .iter()
            .any(|text| text.contains("Deserialize error"))
    );

    let shutdown_errors = [
        ShutdownError::Timeout(Duration::from_secs(3)).to_string(),
        ShutdownError::ComponentNotFound("component-a".to_string()).to_string(),
        ShutdownError::ComponentFailed("component-b".to_string(), "boom".to_string()).to_string(),
    ];
    assert!(shutdown_errors.iter().any(|text| text.contains("Timeout")));
    assert!(
        shutdown_errors
            .iter()
            .any(|text| text.contains("Component 'component-a' not found"))
    );
    assert!(
        shutdown_errors
            .iter()
            .any(|text| text.contains("shutdown failed"))
    );
}

#[test]
fn dead_letter_and_shutdown_roundtrip() {
    let message = EMessage::new(
        MessageTopic("dead.letter".to_string()),
        MessagePayload(vec![9, 8, 7]),
        DeliveryMode::Standard,
        None,
    );
    let dead_letter = DeadLetterMessage {
        original_message: message,
        dead_reason: DeadReason::Timeout,
        died_at: SystemTime::now(),
        attempts: 5,
    };

    let json = serde_json::to_vec(&dead_letter).expect("dead letter should serialize");
    let decoded: DeadLetterMessage =
        serde_json::from_slice(&json).expect("dead letter should deserialize");
    assert_eq!(decoded.attempts, 5);
    assert!(matches!(decoded.dead_reason, DeadReason::Timeout));
    assert_eq!(decoded.original_message.topic.0, "dead.letter");

    let shutdown = ShutdownCommand {
        strategy: ShutdownStrategy::Batched {
            batch_size: 4,
            interval_ms: 250,
        },
    };
    let shutdown_json =
        serde_json::to_string(&shutdown).expect("shutdown command should serialize");
    let decoded_shutdown: ShutdownCommand =
        serde_json::from_str(&shutdown_json).expect("shutdown command should deserialize");
    match decoded_shutdown.strategy {
        ShutdownStrategy::Batched {
            batch_size,
            interval_ms,
        } => {
            assert_eq!(batch_size, 4);
            assert_eq!(interval_ms, 250);
        }
        other => panic!("unexpected shutdown strategy: {other:?}"),
    }

    let ack = ShutdownAck {
        worker_name: "worker-a".to_string(),
        status: ShutdownStatus::Completed,
        timestamp: SystemTime::now(),
        error: None,
    };
    let ack_json = serde_json::to_vec(&ack).expect("shutdown ack should serialize");
    let decoded_ack: ShutdownAck =
        serde_json::from_slice(&ack_json).expect("shutdown ack should deserialize");
    assert_eq!(decoded_ack.worker_name, "worker-a");
}

#[test]
fn shutdown_channel_delivers_signal() {
    let (sender, mut receiver) = shutdown_channel();
    sender.send(()).expect("shutdown signal should send");

    let received = receiver.try_recv();
    assert!(received.is_ok(), "shutdown signal should be observable");
}
