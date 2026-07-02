use event_base_core::error::CoreError;
use event_base_core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use event_base_core::topic::TopicRouter;
use event_base_core::wal::wal::{Wal, WalRecord};
use event_base_core::worker_registry::{WorkerInfo, WorkerRegistry};
use event_base_core::{NodeType, set_node_name, set_node_type};
use event_base_test::support::{RecordingProducer, RecordingWal};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

fn message(topic: &str, payload: &[u8], delivery_mode: DeliveryMode) -> EMessage {
    EMessage::new(
        MessageTopic(topic.to_string()),
        MessagePayload(payload.to_vec()),
        delivery_mode,
        None,
    )
}

fn worker_info(worker_name: &str, topic: &str) -> WorkerInfo {
    WorkerInfo {
        worker_name: worker_name.to_string(),
        topic: topic.to_string(),
        last_heartbeat: SystemTime::now(),
    }
}

/// Combined test because TopicRouter and WorkerRegistry use OnceLock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_router_full_lifecycle() {
    set_node_name("test-node".to_string());
    set_node_type(NodeType::Host);

    let fake_wal = RecordingWal::new();
    fake_wal
        .seed_worker_registry(HashMap::from([
            ("worker-a".to_string(), worker_info("worker-a", "orders")),
            ("worker-b".to_string(), worker_info("worker-b", "orders")),
        ]))
        .await;

    let wal_handle: Arc<RwLock<Box<dyn Wal>>> = Arc::new(RwLock::new(Box::new(fake_wal.clone())));
    let producer = Arc::new(RecordingProducer::default());

    WorkerRegistry::init(Some(wal_handle.clone()))
        .await
        .expect("worker registry init should succeed");
    TopicRouter::init(producer.clone())
        .expect("topic router init should succeed");

    // ---- init failure on double init ----
    let producer2 = Arc::new(RecordingProducer::default());
    let result = TopicRouter::init(producer2);
    assert!(matches!(result, Err(CoreError::AlreadyInitialized)));

    let router = TopicRouter::global();

    // ---- register/list topics ----
    let empty = router.list_topics().await;
    assert!(empty.is_empty());

    router.register_topic("orders").await;
    router.register_topic("orders").await; // idempotent
    router.register_topic("payments").await;

    let topics = router.list_topics().await;
    assert_eq!(topics.len(), 2);
    assert!(topics.contains(&"orders".to_string()));
    assert!(topics.contains(&"payments".to_string()));

    // ---- standard send ----
    let msg = message("orders", b"hello", DeliveryMode::Standard);
    router
        .send("orders", msg, None, None)
        .await
        .expect("send should succeed");
    assert_eq!(producer.sent.lock().await.len(), 1);
    assert_eq!(producer.sent.lock().await[0].payload.0, b"hello");

    // ---- try_send ----
    let msg = message("orders", b"try-me", DeliveryMode::Standard);
    router
        .send("orders", msg, Some(true), None)
        .await
        .expect("try_send should succeed");
    assert_eq!(producer.try_sent.lock().await.len(), 1);

    // ---- send with timeout ----
    let msg = message("orders", b"timeout-me", DeliveryMode::Standard);
    router
        .send("orders", msg, None, Some(Duration::from_millis(100)))
        .await
        .expect("send_timeout should succeed");
    assert_eq!(producer.timeout_sent.lock().await.len(), 1);

    // ---- delayed message ----
    let mut delayed = message("orders", b"delayed", DeliveryMode::Standard);
    delayed.deliver_at = Some(SystemTime::now() + Duration::from_secs(60));
    router
        .send("orders", delayed, None, None)
        .await
        .expect("delayed send should succeed");
    assert_eq!(producer.sent.lock().await.len(), 1); // still 1
    let scheduled = fake_wal.scheduled_records().await;
    assert_eq!(scheduled.len(), 1);

    // ---- past delivery time returns error ----
    let mut past = message("orders", b"past", DeliveryMode::Standard);
    past.deliver_at = Some(SystemTime::now() - Duration::from_secs(10));
    let result = router.send("orders", past, None, None).await;
    assert!(matches!(result, Err(CoreError::ErrorTime)));

    // ---- replay with filter ----
    let pending_msg = message("orders", b"recover-me", DeliveryMode::Standard);
    let delayed_msg = {
        let mut msg = message("orders", b"deliver-later", DeliveryMode::Standard);
        msg.deliver_at = Some(SystemTime::now() + Duration::from_secs(30));
        msg
    };
    let ignored_msg = message("ignored-topic", b"ignore-me", DeliveryMode::Standard);

    fake_wal
        .seed_pending(WalRecord::from_msg(pending_msg.clone()))
        .await;
    fake_wal
        .seed_pending(WalRecord::from_msg(delayed_msg.clone()))
        .await;
    fake_wal
        .seed_pending(WalRecord::from_msg(ignored_msg.clone()))
        .await;

    let summary = router
        .replay(Some(&["orders"]))
        .await
        .expect("replay should succeed");
    // Each previous send() appended a WAL record; with 3 sends + 3 seeded = 6
    // pending records matching "orders": 5 non-delayed get recovered, 1 delayed
    assert_eq!(summary.recovered, 5);
    assert!(summary.delayed >= 1);
    assert!(summary.errors.is_empty());
    assert!(
        producer
            .sent
            .lock()
            .await
            .iter()
            .any(|m| m.id == pending_msg.id)
    );

    // ---- replay without filter (including non-orders topics) ----
    let msg_a = message("orders", b"a", DeliveryMode::Standard);
    let msg_b = message("payments", b"b", DeliveryMode::Standard);
    fake_wal
        .seed_pending(WalRecord::from_msg(msg_a.clone()))
        .await;
    fake_wal
        .seed_pending(WalRecord::from_msg(msg_b.clone()))
        .await;

    let summary = router.replay(None).await.expect("replay should succeed");
    // At least the 2 newly seeded messages are recovered
    assert!(summary.recovered >= 2);
    assert!(summary.errors.is_empty());

    // ---- replay with empty WAL (no pending messages left) ----
    // Note: replay(None) also recovers records appended during the previous replay's send() calls
    // So recovered may be > 0; just verify no errors
    let empty_summary = router.replay(None).await.expect("replay should succeed");
    assert!(empty_summary.errors.is_empty());

    // ---- get_producer (basic sanity check) ----
    let retrieved = router.get_producer();
    let check_msg = message("test", b"direct", DeliveryMode::Standard);
    retrieved
        .send(check_msg)
        .await
        .expect("send through retrieved producer should succeed");
    assert!(producer.sent.lock().await.len() >= 2);

    // ---- delay scheduler: fetch ready messages & deliver ----
    // Schedule a message directly in WAL with past deliver_at (ready now)
    let mut ready_msg = message("orders", b"ready-now", DeliveryMode::Standard);
    ready_msg.deliver_at = Some(SystemTime::now() - Duration::from_secs(1));
    let ready_id = ready_msg.id.clone();
    fake_wal
        .schedule(WalRecord::from_msg(ready_msg))
        .await
        .expect("schedule directly");
    assert!(
        fake_wal
            .scheduled_records()
            .await
            .iter()
            .any(|r| r.message.id == ready_id)
    );

    // Spawn delay scheduler in background — it will pick up ready messages
    let scheduler = tokio::spawn(async move {
        TopicRouter::run_delay_scheduler().await;
    });

    // Wait for the message to be delivered (scheduler polls every 500ms)
    tokio::time::sleep(Duration::from_millis(800)).await;

    // The ready message should now have been sent to the producer
    let all_sent = producer.sent.lock().await;
    let delivered = all_sent.iter().any(|m| m.id == ready_id);
    assert!(delivered, "delay scheduler should deliver ready message");

    // The scheduled record should be removed after delivery
    let remaining = fake_wal.scheduled_records().await;
    assert!(
        !remaining.iter().any(|r| r.message.id == ready_id),
        "scheduled record should be consumed after delivery"
    );

    drop(all_sent);
    scheduler.abort();
}
