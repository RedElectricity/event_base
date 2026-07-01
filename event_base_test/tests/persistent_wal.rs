use event_base_core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use event_base_core::wal::wal::{Wal, WalRecord, WalRecordState};
use event_base_core::worker_registry::WorkerInfo;
use event_base_wal::persistent::PersistentWal;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

fn msg(topic: &str, payload: &[u8]) -> EMessage {
    EMessage::new(
        MessageTopic(topic.to_string()),
        MessagePayload(payload.to_vec()),
        DeliveryMode::Standard,
        None,
    )
}

#[tokio::test]
async fn persistent_wal_new_creates_empty() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_empty.wal");
    let wal = PersistentWal::new(path.to_str().unwrap().to_string())
        .await
        .expect("new should succeed");

    let pending = wal
        .clone()
        .replay_pending()
        .await
        .expect("replay should succeed");
    assert!(pending.is_empty());
}

#[tokio::test]
async fn persistent_wal_append_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_append.wal");
    let mut wal = PersistentWal::new(path.to_str().unwrap().to_string())
        .await
        .expect("new should succeed");

    let message = msg("orders", b"test payload");
    let record = WalRecord::from_msg(message.clone());
    wal.append(record).await.expect("append should succeed");

    let pending = wal.replay_pending().await.expect("replay should succeed");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].message.id, message.id);
    assert_eq!(pending[0].status, WalRecordState::Pending);
}

#[tokio::test]
async fn persistent_wal_update_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_state.wal");
    let mut wal = PersistentWal::new(path.to_str().unwrap().to_string())
        .await
        .expect("new should succeed");

    let message = msg("orders", b"state test");
    let record = WalRecord::from_msg(message.clone());
    wal.append(record).await.expect("append should succeed");

    wal.update_state(&message.id, WalRecordState::Complete)
        .await
        .expect("update_state should succeed");

    let pending = wal.replay_pending().await.expect("replay should succeed");
    assert!(pending.is_empty());
}

#[tokio::test]
async fn persistent_wal_update_state_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_notfound.wal");
    let mut wal = PersistentWal::new(path.to_str().unwrap().to_string())
        .await
        .expect("new should succeed");

    let result = wal
        .update_state("nonexistent", WalRecordState::Complete)
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Record not found"));
}

#[tokio::test]
async fn persistent_wal_schedule_and_fetch_ready() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_sched.wal");
    let wal = PersistentWal::new(path.to_str().unwrap().to_string())
        .await
        .expect("new should succeed");

    let mut future_msg = msg("delayed", b"future");
    future_msg.deliver_at = Some(SystemTime::now() + Duration::from_secs(60));
    let future_record = WalRecord::from_msg(future_msg.clone());
    wal.schedule(future_record)
        .await
        .expect("schedule should succeed");

    let ready = wal.fetch_ready().await.expect("fetch_ready should succeed");
    assert!(ready.is_empty());

    let mut ready_msg = msg("delayed", b"ready now");
    ready_msg.deliver_at = Some(SystemTime::now() - Duration::from_secs(1));
    let ready_record = WalRecord::from_msg(ready_msg.clone());
    wal.schedule(ready_record)
        .await
        .expect("schedule should succeed");

    let ready = wal.fetch_ready().await.expect("fetch_ready should succeed");
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].message.id, ready_msg.id);
}

#[tokio::test]
async fn persistent_wal_remove_scheduled() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_remove_sched.wal");
    let wal = PersistentWal::new(path.to_str().unwrap().to_string())
        .await
        .expect("new should succeed");

    let mut msg = msg("delayed", b"to remove");
    msg.deliver_at = Some(SystemTime::now() - Duration::from_secs(1));
    let record = WalRecord::from_msg(msg.clone());
    wal.schedule(record).await.expect("schedule should succeed");

    wal.remove_scheduled(&msg.id)
        .await
        .expect("remove_scheduled should succeed");

    let ready = wal.fetch_ready().await.expect("fetch_ready should succeed");
    assert!(ready.is_empty());
}

#[tokio::test]
async fn persistent_wal_worker_registry_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_registry.wal");
    let wal = PersistentWal::new(path.to_str().unwrap().to_string())
        .await
        .expect("new should succeed");

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
        .expect("save should succeed");

    let loaded = wal
        .load_worker_registry()
        .await
        .expect("load should succeed");
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded.get("worker-a").unwrap().worker_name, "worker-a");
}

#[tokio::test]
async fn persistent_wal_flush_and_recover() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_flush.wal");
    let path_str = path.to_str().unwrap().to_string();

    // Create and write
    {
        let mut wal = PersistentWal::new(path_str.clone())
            .await
            .expect("new should succeed");

        let message = msg("orders", b"persist me");
        let record = WalRecord::from_msg(message.clone());
        wal.append(record).await.expect("append should succeed");
        wal.flush().await.expect("flush should succeed");
    }

    // Recover from disk
    {
        let mut wal = PersistentWal::new(path_str)
            .await
            .expect("recover should succeed");

        let pending = wal.replay_pending().await.expect("replay should succeed");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].message.payload.0, b"persist me");
    }
}
