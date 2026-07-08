use event_base_core::error::CoreError;
use event_base_core::wal::wal::Wal;
use event_base_core::worker_registry::{WorkerInfo, WorkerRegistry};
use event_base_test::support::RecordingWal;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

fn worker_info(worker_name: &str, topic: &str) -> WorkerInfo {
    WorkerInfo {
        worker_name: worker_name.to_string(),
        topic: topic.to_string(),
        last_heartbeat: SystemTime::now(),
    }
}

/// Combined test because WorkerRegistry uses OnceLock (singleton).
#[tokio::test]
async fn worker_registry_full_lifecycle() {
    // ---- init ----
    let fake_wal = RecordingWal::new();
    let now = SystemTime::now();
    fake_wal
        .seed_worker_registry(HashMap::from([
            (
                "worker-a".to_string(),
                WorkerInfo {
                    worker_name: "worker-a".to_string(),
                    topic: "orders".to_string(),
                    last_heartbeat: now,
                },
            ),
            (
                "worker-b".to_string(),
                WorkerInfo {
                    worker_name: "worker-b".to_string(),
                    topic: "payments".to_string(),
                    last_heartbeat: now,
                },
            ),
        ]))
        .await;

    let wal_handle: Arc<RwLock<Box<dyn Wal>>> = Arc::new(RwLock::new(Box::new(fake_wal.clone())));

    WorkerRegistry::init(Some(wal_handle.clone()))
        .await
        .expect("init should succeed");

    // ---- init failure on double init ----
    let result = WorkerRegistry::init(Some(wal_handle)).await;
    assert!(matches!(result, Err(CoreError::AlreadyInitialized)));

    // ---- get_all_workers after init ----
    let all = WorkerRegistry::global()
        .read().await
        .get_all_workers()
        .await
        .expect("get_all should succeed");
    assert_eq!(all.len(), 2);
    let names: Vec<String> = all.iter().map(|w| w.worker_name.clone()).collect();
    assert!(names.contains(&"worker-a".to_string()));
    assert!(names.contains(&"worker-b".to_string()));

    // ---- register new worker ----
    let registry = WorkerRegistry::global().write().await;
    registry
        .register(worker_info("worker-c", "analytics"))
        .await
        .expect("register should succeed");
    assert_eq!(
        registry
            .get_all_workers()
            .await
            .expect("get_all should succeed")
            .len(),
        3
    );

    // ---- register updates existing worker ----
    let old_heartbeat = SystemTime::now() - Duration::from_secs(60);
    let mut updated = worker_info("worker-a", "new-topic");
    updated.last_heartbeat = old_heartbeat;
    registry
        .register(updated)
        .await
        .expect("register update should succeed");
    let all = registry
        .get_all_workers()
        .await
        .expect("get_all should succeed");
    assert_eq!(all.len(), 3);
    let worker_a = all.iter().find(|w| w.worker_name == "worker-a").unwrap();
    assert_eq!(worker_a.topic, "new-topic");

    // ---- unregister ----
    registry
        .unregister("worker-c")
        .await
        .expect("unregister should succeed");
    let all = registry
        .get_all_workers()
        .await
        .expect("get_all should succeed");
    assert_eq!(all.len(), 2);
    assert!(!all.iter().any(|w| w.worker_name == "worker-c"));

    // ---- heartbeat ----
    tokio::time::sleep(Duration::from_millis(5)).await;
    registry
        .heartbeat("worker-a")
        .await
        .expect("heartbeat should succeed");
    let all = registry
        .get_all_workers()
        .await
        .expect("get_all should succeed");
    let worker = all.iter().find(|w| w.worker_name == "worker-a").unwrap();
    let elapsed = SystemTime::now()
        .duration_since(worker.last_heartbeat)
        .unwrap();
    assert!(elapsed < Duration::from_secs(1));

    // ---- get_workers filters by topic ----
    let orders = registry
        .get_workers("orders")
        .await
        .expect("get_workers should succeed");
    assert!(orders.is_empty()); // worker-a changed to "new-topic"

    let new_topic_workers = registry
        .get_workers("new-topic")
        .await
        .expect("get_workers should succeed");
    assert_eq!(new_topic_workers.len(), 1);
    assert_eq!(new_topic_workers[0].worker_name, "worker-a");

    let unknown = registry
        .get_workers("unknown")
        .await
        .expect("get_workers should succeed");
    assert!(unknown.is_empty());

    // ---- cleanup_stale ----
    registry
        .register(WorkerInfo {
            worker_name: "stale-worker".to_string(),
            topic: "old".to_string(),
            last_heartbeat: now - Duration::from_secs(3600),
        })
        .await
        .expect("register stale should succeed");

    registry
        .register(WorkerInfo {
            worker_name: "fresh-worker".to_string(),
            topic: "new".to_string(),
            last_heartbeat: SystemTime::now(),
        })
        .await
        .expect("register fresh should succeed");

    let stale = registry
        .cleanup_stale(Duration::from_secs(300))
        .await
        .expect("cleanup should succeed");
    assert!(stale.contains(&"stale-worker".to_string()));
    assert!(!stale.contains(&"fresh-worker".to_string()));

    let remaining = registry
        .get_all_workers()
        .await
        .expect("get_all should succeed");
    assert!(!remaining.iter().any(|w| w.worker_name == "stale-worker"));
    assert!(remaining.iter().any(|w| w.worker_name == "fresh-worker"));

    // ---- cleanup_stale with zero timeout ----
    registry
        .register(WorkerInfo {
            worker_name: "stale-worker".to_string(),
            topic: "old".to_string(),
            last_heartbeat: now - Duration::from_secs(3600),
        })
        .await
        .expect("register stale should succeed");

    let stale = registry
        .cleanup_stale(Duration::from_secs(0))
        .await
        .expect("cleanup should succeed");
    assert!(stale.contains(&"stale-worker".to_string()));
}
