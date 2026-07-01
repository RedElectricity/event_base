//! Tests for WAL codec (BincodeCodec) and WalRecord types.

use event_base_core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use event_base_core::wal::codec::{BincodeCodec, WalRecordCodec};
use event_base_core::wal::wal::{WalRecord, WalRecordState};
use std::time::SystemTime;

fn message(topic: &str, payload: &[u8]) -> EMessage {
    EMessage::new(
        MessageTopic(topic.to_string()),
        MessagePayload(payload.to_vec()),
        DeliveryMode::Standard,
        None,
    )
}

// ──────────────────────────────────────────────
// BincodeCodec roundtrip tests
// ──────────────────────────────────────────────

#[test]
fn bincode_codec_roundtrip_pending() {
    let codec = BincodeCodec;
    let msg = message("orders", b"test-payload");
    let record = WalRecord {
        record_id: 42,
        message: msg.clone(),
        status: WalRecordState::Pending,
        is_dead_letter: false,
        dead_reason: None,
        last_attempt_at: None,
    };

    let encoded = codec.encode(&record).expect("encode should succeed");
    let decoded = codec.decode(&encoded).expect("decode should succeed");

    assert_eq!(decoded.record_id, 42);
    assert_eq!(decoded.message.id, msg.id);
    assert_eq!(decoded.message.topic.0, "orders");
    assert_eq!(decoded.message.payload.0, b"test-payload");
    assert_eq!(decoded.status, WalRecordState::Pending);
    assert!(!decoded.is_dead_letter);
    assert!(decoded.dead_reason.is_none());
}

#[test]
fn bincode_codec_roundtrip_complete() {
    let codec = BincodeCodec;
    let now = SystemTime::now();
    let record = WalRecord {
        record_id: 100,
        message: message("payments", b"done"),
        status: WalRecordState::Complete,
        is_dead_letter: false,
        dead_reason: None,
        last_attempt_at: Some(now),
    };

    let encoded = codec.encode(&record).expect("encode should succeed");
    let decoded = codec.decode(&encoded).expect("decode should succeed");

    assert_eq!(decoded.status, WalRecordState::Complete);
    assert_eq!(decoded.record_id, 100);
    assert!(decoded.last_attempt_at.is_some());
}

#[test]
fn bincode_codec_roundtrip_processing() {
    let codec = BincodeCodec;
    let record = WalRecord {
        record_id: 7,
        message: message("events", b"processing"),
        status: WalRecordState::Processing,
        is_dead_letter: false,
        dead_reason: None,
        last_attempt_at: Some(SystemTime::now()),
    };

    let encoded = codec.encode(&record).expect("encode should succeed");
    let decoded = codec.decode(&encoded).expect("decode should succeed");

    assert_eq!(decoded.status, WalRecordState::Processing);
}

#[test]
fn bincode_codec_roundtrip_failed_with_reason() {
    use event_base_core::dead_letter::DeadReason;
    let codec = BincodeCodec;
    let record = WalRecord {
        record_id: 99,
        message: message("dlq", b"dead"),
        status: WalRecordState::Failed,
        is_dead_letter: true,
        dead_reason: Some(DeadReason::MaxRetriesExceeded),
        last_attempt_at: Some(SystemTime::now()),
    };

    let encoded = codec.encode(&record).expect("encode should succeed");
    let decoded = codec.decode(&encoded).expect("decode should succeed");

    assert_eq!(decoded.status, WalRecordState::Failed);
    assert!(decoded.is_dead_letter);
    assert!(matches!(
        decoded.dead_reason,
        Some(DeadReason::MaxRetriesExceeded)
    ));
}

#[test]
fn bincode_codec_roundtrip_all_dead_reasons() {
    use event_base_core::dead_letter::DeadReason;
    let codec = BincodeCodec;

    let reasons = vec![
        DeadReason::MaxRetriesExceeded,
        DeadReason::Explicit,
        DeadReason::Timeout,
        DeadReason::NoHandler,
        DeadReason::Other("custom error".to_string()),
    ];

    for reason in reasons {
        let record = WalRecord {
            record_id: 1,
            message: message("test", b"data"),
            status: WalRecordState::Failed,
            is_dead_letter: true,
            dead_reason: Some(reason.clone()),
            last_attempt_at: None,
        };

        let encoded = codec.encode(&record).expect("encode should succeed");
        let decoded = codec.decode(&encoded).expect("decode should succeed");

        assert_eq!(decoded.dead_reason, Some(reason));
    }
}

#[test]
fn bincode_codec_decode_invalid_data_returns_error() {
    let codec = BincodeCodec;
    let invalid_bytes = vec![0xFF, 0x00, 0xAB, 0xCD]; // garbage data
    let result = codec.decode(&invalid_bytes);
    assert!(result.is_err());
}

#[test]
fn bincode_codec_decode_empty_slice_returns_error() {
    let codec = BincodeCodec;
    let result = codec.decode(&[]);
    assert!(result.is_err());
}

// ──────────────────────────────────────────────
// WalRecord::from_msg tests
// ──────────────────────────────────────────────

#[test]
fn wal_record_from_msg_all_delivery_modes() {
    for mode in &[
        DeliveryMode::Standard,
        DeliveryMode::Repeated(3),
        DeliveryMode::Broadcast,
    ] {
        let msg = EMessage::new(
            MessageTopic("t".to_string()),
            MessagePayload(vec![]),
            mode.clone(),
            None,
        );
        let record = WalRecord::from_msg(msg.clone());
        assert_eq!(record.message.delivery_mode, mode.clone());
        assert_eq!(record.status, WalRecordState::Pending);
        assert!(!record.is_dead_letter);
    }
}

#[test]
fn wal_record_from_msg_preserves_metadata() {
    let mut msg = message("orders", b"test");
    msg.metadata.trace_id = Some("trace-abc".to_string());
    msg.metadata.correlation_id = Some("corr-123".to_string());
    msg.metadata.source = Some("test-svc".to_string());
    msg.to_worker = Some("worker-x".to_string());
    msg.deliver_at = Some(SystemTime::now());

    let record = WalRecord::from_msg(msg.clone());
    assert_eq!(
        record.message.metadata.trace_id.as_deref(),
        Some("trace-abc")
    );
    assert_eq!(
        record.message.metadata.correlation_id.as_deref(),
        Some("corr-123")
    );
    assert_eq!(record.message.metadata.source.as_deref(), Some("test-svc"));
    assert_eq!(record.message.to_worker.as_deref(), Some("worker-x"));
    assert!(record.message.deliver_at.is_some());
}

// ──────────────────────────────────────────────
// WalRecordState copy/clone/debug
// ──────────────────────────────────────────────

#[test]
fn wal_record_state_debug_format() {
    assert_eq!(format!("{:?}", WalRecordState::Pending), "Pending");
    assert_eq!(format!("{:?}", WalRecordState::Processing), "Processing");
    assert_eq!(format!("{:?}", WalRecordState::Complete), "Complete");
    assert_eq!(format!("{:?}", WalRecordState::Failed), "Failed");
}

#[test]
fn wal_record_state_copy_clone() {
    let s = WalRecordState::Processing;
    let copied = s;
    assert_eq!(s, copied);
    assert_eq!(s.clone(), WalRecordState::Processing);
}

// ──────────────────────────────────────────────
// WalRecord clone preserves all fields
// ──────────────────────────────────────────────

#[test]
fn wal_record_clone_is_deep() {
    let record = WalRecord {
        record_id: 77,
        message: message("topic-a", b"payload-a"),
        status: WalRecordState::Complete,
        is_dead_letter: false,
        dead_reason: None,
        last_attempt_at: Some(SystemTime::now()),
    };

    let cloned = record.clone();
    assert_eq!(cloned.record_id, record.record_id);
    assert_eq!(cloned.message.id, record.message.id);
    assert_eq!(cloned.status, record.status);
    assert_eq!(cloned.is_dead_letter, record.is_dead_letter);

    // Verify deep copy: modifying original payload doesn't affect clone
    let mut original = record;
    original.message.payload.0.push(99);
    assert_ne!(original.message.payload.0, cloned.message.payload.0);
}
