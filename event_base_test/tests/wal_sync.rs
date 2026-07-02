use event_base_core::error::serialize::SerializeError;
use event_base_core::wal::codec::{BincodeCodec, WalRecordCodec};
use event_base_core::wal::sync::WalSyncMessage;
use event_base_core::wal::wal::{WalRecord, WalRecordState};
use std::time::SystemTime;

// ──────────────────────────────────────────────
// BincodeCodec tests (no singletons)
// ──────────────────────────────────────────────

#[test]
fn bincode_codec_roundtrip_complete_record() {
    let codec = BincodeCodec;
    let record = WalRecord {
        record_id: 42,
        message: event_base_core::message::EMessage::default(),
        status: WalRecordState::Processing,
        last_attempt_at: Some(SystemTime::now()),
        is_dead_letter: true,
        dead_reason: Some(event_base_core::dead_letter::DeadReason::Timeout),
    };

    let encoded = codec.encode(&record).expect("encode should succeed");
    let decoded = codec.decode(&encoded).expect("decode should succeed");

    assert_eq!(decoded.record_id, 42);
    assert_eq!(decoded.status, WalRecordState::Processing);
    assert!(decoded.is_dead_letter);
}

#[test]
fn bincode_codec_roundtrip_minimal_record() {
    let codec = BincodeCodec;
    let msg = event_base_core::message::EMessage::new(
        event_base_core::message::MessageTopic("test".to_string()),
        event_base_core::message::MessagePayload(vec![1, 2, 3]),
        event_base_core::message::DeliveryMode::Standard,
        None,
    );
    let record = WalRecord::from_msg(msg);

    let encoded = codec.encode(&record).expect("encode should succeed");
    let decoded = codec.decode(&encoded).expect("decode should succeed");

    assert_eq!(decoded.message.topic.0, "test");
    assert_eq!(decoded.message.payload.0, vec![1, 2, 3]);
    assert_eq!(decoded.status, WalRecordState::Pending);
    assert!(!decoded.is_dead_letter);
}

#[test]
fn bincode_codec_decode_invalid_data_returns_error() {
    let codec = BincodeCodec;
    let bad_data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let result = codec.decode(&bad_data);
    assert!(result.is_err());
    match result {
        Err(SerializeError::DeserializeError(_)) => {} // expected
        _ => panic!("Expected DeserializeError"),
    }
}

#[test]
fn wal_sync_message_serialization() {
    let msg = WalSyncMessage {
        message_id: "msg-1".to_string(),
        topic: "orders".to_string(),
        worker_id: "worker-a".to_string(),
        status: WalRecordState::Complete,
        attempts: 3,
        last_attempt_at: SystemTime::now(),
        error: None,
        timestamp: SystemTime::now(),
    };

    let bytes = bincode::encode_to_vec(&msg, bincode::config::standard()).expect("serialize should succeed");
    let decoded: WalSyncMessage =
        bincode::decode_from_slice(&bytes, bincode::config::standard()).expect("deserialize should succeed").0;

    assert_eq!(decoded.message_id, "msg-1");
    assert_eq!(decoded.topic, "orders");
    assert_eq!(decoded.worker_id, "worker-a");
    assert_eq!(decoded.status, WalRecordState::Complete);
    assert_eq!(decoded.attempts, 3);
    assert!(decoded.error.is_none());
}

#[test]
fn wal_sync_message_with_error() {
    let msg = WalSyncMessage {
        message_id: "msg-2".to_string(),
        topic: "payments".to_string(),
        worker_id: "worker-b".to_string(),
        status: WalRecordState::Failed,
        attempts: 5,
        last_attempt_at: SystemTime::now(),
        error: Some("max retries exceeded".to_string()),
        timestamp: SystemTime::now(),
    };

    let bytes = bincode::encode_to_vec(&msg, bincode::config::standard()).expect("serialize should succeed");
    let decoded: WalSyncMessage =
        bincode::decode_from_slice(&bytes, bincode::config::standard()).expect("deserialize should succeed").0;

    assert_eq!(decoded.status, WalRecordState::Failed);
    assert_eq!(decoded.error.as_deref(), Some("max retries exceeded"));
}
