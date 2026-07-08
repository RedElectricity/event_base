use event_base_core::audit::{AuditEventType, AuditManager, AuditRecord, AuditResult};
use event_base_core::error::CoreError;
use event_base_core::metrics::aggregator::MetricsAggregator;
use event_base_core::metrics::manager::MetricsManager;
use event_base_core::metrics::node::NodeMetrics;
use event_base_core::metrics::node_store::MetricsStore;
use std::time::{Duration, SystemTime};

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

// ──────────────────────────────────────────────
// MetricsAggregator tests (no singletons – can be parallel)
// ──────────────────────────────────────────────

#[test]
fn metrics_aggregator_tracks_enqueued() {
    let mut agg = MetricsAggregator {
        enqueued: Default::default(),
        completed: Default::default(),
        failed: Default::default(),
        retried: Default::default(),
        latency_sum: Default::default(),
    };

    agg.feed(&audit_record("m1", "orders", AuditEventType::Enqueued));
    agg.feed(&audit_record("m2", "orders", AuditEventType::Enqueued));
    agg.feed(&audit_record("m3", "payments", AuditEventType::Enqueued));

    assert_eq!(agg.enqueued.get("orders"), Some(&2));
    assert_eq!(agg.enqueued.get("payments"), Some(&1));
    assert_eq!(agg.completed.len(), 0);
}

#[test]
fn metrics_aggregator_tracks_completed_and_latency() {
    let mut agg = MetricsAggregator {
        enqueued: Default::default(),
        completed: Default::default(),
        failed: Default::default(),
        retried: Default::default(),
        latency_sum: Default::default(),
    };

    agg.feed(&audit_record(
        "m1",
        "orders",
        AuditEventType::ProcessingCompleted,
    ));
    agg.feed(&audit_record(
        "m2",
        "orders",
        AuditEventType::ProcessingCompleted,
    ));

    assert_eq!(agg.completed.get("orders"), Some(&2));
    let (count, total) = agg.latency_sum.get("orders").unwrap();
    assert_eq!(*count, 2);
    assert!(total.as_millis() >= 30);
}

#[test]
fn metrics_aggregator_tracks_failed_and_retried() {
    let mut agg = MetricsAggregator {
        enqueued: Default::default(),
        completed: Default::default(),
        failed: Default::default(),
        retried: Default::default(),
        latency_sum: Default::default(),
    };

    agg.feed(&audit_record("m1", "orders", AuditEventType::DeadLettered));
    agg.feed(&audit_record("m2", "orders", AuditEventType::Retry));
    agg.feed(&audit_record("m3", "orders", AuditEventType::Retry));

    assert_eq!(agg.failed.get("orders"), Some(&1));
    assert_eq!(agg.retried.get("orders"), Some(&2));
}

#[test]
fn metrics_aggregator_ignores_processing_started() {
    let mut agg = MetricsAggregator {
        enqueued: Default::default(),
        completed: Default::default(),
        failed: Default::default(),
        retried: Default::default(),
        latency_sum: Default::default(),
    };

    agg.feed(&audit_record(
        "m1",
        "orders",
        AuditEventType::ProcessingStarted,
    ));
    assert_eq!(agg.enqueued.len(), 0);
    assert_eq!(agg.completed.len(), 0);
    assert_eq!(agg.failed.len(), 0);
    assert_eq!(agg.retried.len(), 0);
}

#[test]
fn metrics_aggregator_snapshot_is_independent() {
    let mut agg = MetricsAggregator {
        enqueued: Default::default(),
        completed: Default::default(),
        failed: Default::default(),
        retried: Default::default(),
        latency_sum: Default::default(),
    };

    agg.feed(&audit_record("m1", "orders", AuditEventType::Enqueued));
    let snap = agg.snapshot();
    agg.feed(&audit_record("m2", "orders", AuditEventType::Enqueued));

    assert_eq!(snap.enqueued.get("orders"), Some(&1));
    assert_eq!(agg.enqueued.get("orders"), Some(&2));
}

// ──────────────────────────────────────────────
// Combined singleton-dependent tests
// MetricsStore, MetricsManager, AuditManager all use OnceLock.
// They must run in a single test function.
// ──────────────────────────────────────────────

#[tokio::test]
async fn metrics_and_audit_singletons_lifecycle() {
    // ── MetricsStore ──
    MetricsStore::init().expect("MetricsStore init should succeed");
    assert!(matches!(
        MetricsStore::init(),
        Err(CoreError::AlreadyInitialized)
    ));

    assert!(MetricsStore::global().read().await.get_all_nodes().await.is_empty());

    let metrics = NodeMetrics {
        node_name: "node-a".to_string(),
        node_type: event_base_core::NodeType::Host,
        cpu_percent: vec![12.5, 45.0],
        memory_percent: 33.0,
        node_worker_count: 2,
        update_time: SystemTime::now(),
    };
    MetricsStore::global().write().await.update(metrics.clone()).await;
    assert_eq!(MetricsStore::global().read().await.get_all_nodes().await.len(), 1);
    assert_eq!(MetricsStore::global().read().await.get_node("node-a").await.unwrap().node_worker_count, 2);
    assert!(MetricsStore::global().read().await.get_node("unknown").await.is_none());

    // Overwrite
    MetricsStore::global()
        .write().await
        .update(NodeMetrics {
            node_name: "node-a".to_string(),
            node_type: event_base_core::NodeType::Host,
            cpu_percent: vec![99.0],
            memory_percent: 50.0,
            node_worker_count: 5,
            update_time: SystemTime::now(),
        })
        .await;
    assert_eq!(MetricsStore::global().read().await.get_node("node-a").await.unwrap().node_worker_count, 5);

    // ── MetricsManager ──
    MetricsManager::init().expect("MetricsManager init should succeed");
    assert!(matches!(
        MetricsManager::init(),
        Err(CoreError::AlreadyInitialized)
    ));

    let mgr = MetricsManager::global().write().await;
    mgr.feed_audit(&audit_record("a1", "orders", AuditEventType::Enqueued))
        .await;
    mgr.feed_audit(&audit_record("a2", "orders", AuditEventType::Enqueued))
        .await;
    mgr.feed_audit(&audit_record(
        "a3",
        "orders",
        AuditEventType::ProcessingCompleted,
    ))
    .await;
    mgr.feed_audit(&audit_record("a4", "orders", AuditEventType::DeadLettered))
        .await;
    mgr.feed_audit(&audit_record("a5", "orders", AuditEventType::Retry))
        .await;
    drop(mgr);

    let snap = mgr.snapshot().await;
    assert_eq!(snap.business.enqueued.get("orders"), Some(&2));
    assert_eq!(snap.business.completed.get("orders"), Some(&1));
    assert_eq!(snap.business.failed.get("orders"), Some(&1));
    assert_eq!(snap.business.retried.get("orders"), Some(&1));

    // ── AuditManager ──
    AuditManager::init(16).expect("AuditManager init should succeed");
    assert!(matches!(
        AuditManager::init(16),
        Err(CoreError::AlreadyInitialized)
    ));

    let audit = AuditManager::global();

    // Record entries
    for i in 0..8 {
        audit
            .record(audit_record(
                &format!("audit-{}", i),
                "test",
                AuditEventType::Enqueued,
            ))
            .await
            .expect("record should succeed");
    }

    // get_recent returns newest first
    let recent = audit.get_recent(3).await;
    assert_eq!(recent.len(), 3);
    assert_eq!(recent[0].message_id, "audit-7");
    assert_eq!(recent[1].message_id, "audit-6");
    assert_eq!(recent[2].message_id, "audit-5");

    // get_recent with count larger than buffer
    let all = audit.get_recent(100).await;
    assert_eq!(all.len(), 8);
}
