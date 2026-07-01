use event_base_core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use event_base_core::wal::wal::{Wal, WalRecord, WalRecordState};
use event_base_core::worker_registry::WorkerInfo;
use event_base_wal::memory::MemoryWal;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

fn message(topic: &str, payload: &[u8]) -> EMessage {
    EMessage::new(
        MessageTopic(topic.to_string()),
        MessagePayload(payload.to_vec()),
        DeliveryMode::Standard,
        None,
    )
}

#[tokio::test]
async fn memory_wal_tracks_pending_and_delayed_records() {
    let mut wal = MemoryWal::new();

    let record_message = message("wal", b"payload");
    let record = WalRecord::from_msg(record_message.clone());
    wal.append(record).await.expect("append should succeed");

    let pending = wal.replay_pending().await.expect("replay should succeed");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].message.id, record_message.id);

    wal.update_state(&record_message.id, WalRecordState::Complete)
        .await
        .expect("update_state should succeed");
    assert!(
        wal.replay_pending()
            .await
            .expect("replay should succeed")
            .is_empty()
    );

    let delayed_message = {
        let mut msg = self::message("wal.delayed", b"later");
        msg.deliver_at = Some(SystemTime::now() - Duration::from_secs(1));
        msg
    };
    wal.schedule(WalRecord::from_msg(delayed_message.clone()))
        .await
        .expect("schedule should succeed");

    let ready = wal.fetch_ready().await.expect("fetch_ready should succeed");
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].message.id, delayed_message.id);
    assert!(
        wal.fetch_ready()
            .await
            .expect("fetch_ready should succeed")
            .is_empty()
    );

    wal.remove_scheduled(&delayed_message.id)
        .await
        .expect("remove_scheduled should succeed");
}

#[tokio::test]
async fn memory_wal_persists_worker_registry() {
    let wal = MemoryWal::new();
    let mut registry = HashMap::new();
    registry.insert(
        "worker-a".to_string(),
        WorkerInfo {
            worker_name: "worker-a".to_string(),
            topic: "orders".to_string(),
            last_heartbeat: SystemTime::now(),
        },
    );

    wal.save_worker_registry(registry.clone())
        .await
        .expect("save_worker_registry should succeed");
    let loaded = wal
        .load_worker_registry()
        .await
        .expect("load_worker_registry should succeed");
    assert_eq!(loaded.len(), 1);
    let stored = loaded.get("worker-a").expect("worker should be stored");
    assert_eq!(stored.worker_name, "worker-a");
    assert_eq!(stored.topic, "orders");

    let missing = wal
        .remove_scheduled("missing-id")
        .await
        .expect("remove_scheduled should be tolerant for missing ids");
    assert_eq!(missing, ());
}
