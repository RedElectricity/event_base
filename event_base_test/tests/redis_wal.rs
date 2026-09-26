#![cfg(feature = "redis")]

use event_base_core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use event_base_core::wal::wal::{Wal, WalRecord, WalRecordState};
use event_base_core::worker_registry::WorkerInfo;
use event_base_wal::redis::RedisWal;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Resolves the test Redis URL, or `None` (skip) when it is not configured.
fn redis_url() -> Option<String> {
    match std::env::var("EVENT_BASE_TEST_REDIS_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            eprintln!(
                "SKIP redis_wal: set EVENT_BASE_TEST_REDIS_URL (e.g. redis://127.0.0.1:6379) \
                 to run the Redis WAL backend tests"
            );
            None
        }
    }
}

/// A unique key prefix per test so concurrent tests and shared instances do
/// not interfere. Keys are namespaced under `ebtest:*` and only leak on a
/// persistent server — point `EVENT_BASE_TEST_REDIS_URL` at a disposable
/// instance (CI uses an ephemeral container).
fn test_prefix(tag: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("ebtest:{}:{tag}:{}", std::process::id(), nanos)
}

async fn wal(tag: &str) -> Option<RedisWal> {
    let url = redis_url()?;
    Some(
        RedisWal::with_prefix(url, test_prefix(tag))
            .await
            .expect("connect test redis"),
    )
}

fn message(topic: &str, payload: &[u8]) -> EMessage {
    EMessage::new(
        MessageTopic(topic.to_string()),
        MessagePayload(payload.to_vec()),
        DeliveryMode::Standard,
        None,
    )
}

#[tokio::test]
async fn redis_wal_tracks_pending_and_states() {
    let Some(mut wal) = wal("states").await else {
        return;
    };

    let msg = message("wal", b"payload");
    wal.append(WalRecord::from_msg(msg.clone()))
        .await
        .expect("append should succeed");

    let pending = wal.replay_pending().await.expect("replay should succeed");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].message.id, msg.id);
    assert_eq!(pending[0].record_id, 1, "record ids come from INCR");

    wal.update_state(&msg.id, WalRecordState::Complete)
        .await
        .expect("update_state should succeed");
    assert!(
        wal.replay_pending()
            .await
            .expect("replay should succeed")
            .is_empty()
    );

    // A second append gets a fresh (monotonic) record id.
    let msg2 = message("wal", b"payload2");
    wal.append(WalRecord::from_msg(msg2.clone()))
        .await
        .expect("second append should succeed");
    let pending = wal.replay_pending().await.expect("replay should succeed");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].message.id, msg2.id);
    assert!(pending[0].record_id > 1);

    // Unknown message ids must error, mirroring PersistentWal.
    wal.update_state("no-such-id", WalRecordState::Failed)
        .await
        .expect_err("update_state on a missing record must fail");

    wal.flush().await.expect("flush is a no-op");
}

#[tokio::test]
async fn redis_wal_schedules_and_delivers() {
    let Some(wal) = wal("delays").await else {
        return;
    };

    let future_msg = {
        let mut msg = message("wal.later", b"not yet");
        msg.deliver_at = Some(SystemTime::now() + Duration::from_secs(60));
        msg
    };
    wal.schedule(WalRecord::from_msg(future_msg.clone()))
        .await
        .expect("schedule should succeed");

    let past_msg = {
        let mut msg = message("wal.now", b"ready");
        msg.deliver_at = Some(SystemTime::now() - Duration::from_secs(1));
        msg
    };
    wal.schedule(WalRecord::from_msg(past_msg.clone()))
        .await
        .expect("schedule should succeed");

    let ready = wal.fetch_ready().await.expect("fetch_ready should succeed");
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].message.id, past_msg.id);

    // Ready entries are consumed once — a second fetch must be empty.
    let again = wal.fetch_ready().await.expect("fetch_ready should succeed");
    assert!(again.is_empty());

    // remove_scheduled drops a pending future delivery.
    wal.remove_scheduled(&future_msg.id)
        .await
        .expect("remove_scheduled should succeed");
    wal.remove_scheduled("absent-id")
        .await
        .expect("remove_scheduled is idempotent");
}

#[tokio::test]
async fn redis_wal_persists_worker_registry() {
    let Some(wal) = wal("registry").await else {
        return;
    };

    let mut registry = HashMap::new();
    registry.insert(
        "w1".to_string(),
        WorkerInfo {
            worker_name: "w1".to_string(),
            topic: "t".to_string(),
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
    let w = loaded.get("w1").expect("w1 present");
    assert_eq!(w.topic, "t");

    // Replacement semantics: saving an empty map clears the registry.
    wal.save_worker_registry(HashMap::new())
        .await
        .expect("save empty should succeed");
    assert!(
        wal.load_worker_registry()
            .await
            .expect("load should succeed")
            .is_empty()
    );
}

#[tokio::test]
async fn redis_wal_state_survives_handle_restart() {
    let Some(url) = redis_url() else {
        return;
    };
    let prefix = test_prefix("restart");
    let mut wal = RedisWal::with_prefix(url.clone(), prefix.clone())
        .await
        .expect("connect test redis");

    let msg = message("wal.restart", b"durable");
    wal.append(WalRecord::from_msg(msg.clone()))
        .await
        .expect("append should succeed");

    // A brand-new handle on the same prefix sees the same pending set —
    // the core promise of a Redis WAL over the memory one.
    let mut reopened = RedisWal::with_prefix(url, prefix)
        .await
        .expect("reconnect test redis");
    let pending = reopened
        .replay_pending()
        .await
        .expect("replay should succeed");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].message.id, msg.id);
    assert_eq!(pending[0].record_id, 1);
}
