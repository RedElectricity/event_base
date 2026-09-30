#![cfg(feature = "redis")]

//! Coordination‑plane semantics on the shared Redis fabric:
//!
//! * fan‑out topics (`_system.metrics`, `_system.shutdown`,
//!   `_system.topic_sync`) — every node's consumer group gets its own copy;
//! * business topics — one shared group, competing consumers (exactly one
//!   node per message);
//! * fan‑out groups start at `$` so a late‑joining node never replays stale
//!   control traffic;
//! * `_system.*` announcements from a node that has no local consumer reach
//!   the node that does (worker discovery → Host).

use event_base_core::constant::{SYSTEM_TOPIC_METRICS, SYSTEM_TOPIC_WORKER_DISCOVERY};
use event_base_core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use event_base_core::queues::factory::QueueFactory;
use event_base_queue::redis_streams::{RedisQueueConfig, RedisStreamQueueFactory};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn redis_url() -> Option<String> {
    match std::env::var("EVENT_BASE_TEST_REDIS_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            eprintln!(
                "SKIP redis_coordination: set EVENT_BASE_TEST_REDIS_URL to run \
                 (the CI job runs it against a redis:7-alpine service)"
            );
            None
        }
    }
}

fn test_prefix(tag: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("ebtest:{tag}:{}:{nanos}", std::process::id())
}

fn message(topic: &str, payload: &[u8]) -> EMessage {
    EMessage::new(
        MessageTopic(topic.to_string()),
        MessagePayload(payload.to_vec()),
        DeliveryMode::Standard,
        None,
    )
}

async fn node_factory(url: &str, prefix: &str, node: &str) -> RedisStreamQueueFactory {
    let config = RedisQueueConfig::new(url)
        .with_prefix(prefix)
        .with_node(node)
        .with_block_ms(200);
    RedisStreamQueueFactory::with_config(config)
        .await
        .expect("build node factory")
}

#[tokio::test]
async fn fanout_topics_deliver_to_every_node() {
    let Some(url) = redis_url() else { return };
    let prefix = test_prefix("fanout");
    let fa = node_factory(&url, &prefix, "nodeA").await;
    let fb = node_factory(&url, &prefix, "nodeB").await;

    let (_pa, cfa) = fa.create_queue(SYSTEM_TOPIC_METRICS).expect("mq A");
    let (_pb, cfb) = fb.create_queue(SYSTEM_TOPIC_METRICS).expect("mq B");
    let mut ra = cfa.create_consumer();
    let mut rb = cfb.create_consumer();

    // Warm both per‑node groups (lazy XGROUP CREATE at `$`) BEFORE publishing,
    // otherwise the `$` start would legitimately skip the message.
    let _ = tokio::time::timeout(Duration::from_secs(3), ra.claim()).await;
    let _ = tokio::time::timeout(Duration::from_secs(3), rb.claim()).await;

    fa.create_global_producer()
        .expect("producer")
        .send(message(SYSTEM_TOPIC_METRICS, b"nodeA metrics"))
        .await
        .expect("publish");

    // Each node's group sees its own copy of the single stream entry.
    let a = tokio::time::timeout(Duration::from_secs(5), ra.receive())
        .await
        .expect("A receives its copy")
        .expect("decodes");
    let b = tokio::time::timeout(Duration::from_secs(5), rb.receive())
        .await
        .expect("B receives its copy")
        .expect("decodes");
    assert_eq!(a.payload.0, b"nodeA metrics".to_vec());
    assert_eq!(b.payload.0, b"nodeA metrics".to_vec());
}

#[tokio::test]
async fn business_topics_compete_across_nodes() {
    let Some(url) = redis_url() else { return };
    let prefix = test_prefix("compete");
    let fa = node_factory(&url, &prefix, "nodeA").await;
    let fb = node_factory(&url, &prefix, "nodeB").await;

    let (producer, cfa) = fa.create_queue("jobs").expect("jobs A");
    let (_, cfb) = fb.create_queue("jobs").expect("jobs B");
    let mut ra = cfa.create_consumer();
    let mut rb = cfb.create_consumer();

    // Warm the shared group (second creator gets BUSYGROUP, tolerated).
    let _ = tokio::time::timeout(Duration::from_secs(3), ra.claim()).await;
    let _ = tokio::time::timeout(Duration::from_secs(3), rb.claim()).await;

    producer
        .send(message("jobs", b"one job"))
        .await
        .expect("publish");

    let got_a = tokio::time::timeout(Duration::from_secs(3), ra.receive())
        .await
        .ok()
        .flatten()
        .is_some();
    let got_b = tokio::time::timeout(Duration::from_secs(3), rb.receive())
        .await
        .ok()
        .flatten()
        .is_some();
    assert!(
        got_a ^ got_b,
        "competing consumers must split delivery: got_a={got_a} got_b={got_b}"
    );
}

#[tokio::test]
async fn late_joining_node_does_not_replay_fanout_backlog() {
    let Some(url) = redis_url() else { return };
    let prefix = test_prefix("late");
    let fa = node_factory(&url, &prefix, "nodeA").await;

    // A publishes one metrics message with only its own group existing.
    let (_pa, cfa) = fa.create_queue(SYSTEM_TOPIC_METRICS).expect("mq A");
    let mut ra = cfa.create_consumer();
    let _ = tokio::time::timeout(Duration::from_secs(3), ra.claim()).await;
    fa.create_global_producer()
        .expect("producer")
        .send(message(SYSTEM_TOPIC_METRICS, b"stale"))
        .await
        .expect("publish");
    tokio::time::timeout(Duration::from_secs(5), ra.receive())
        .await
        .expect("A gets it")
        .expect("decodes");

    // B joins only now: its `$`‑started group must see nothing historical.
    let fb = node_factory(&url, &prefix, "nodeB").await;
    let (_pb, cfb) = fb.create_queue(SYSTEM_TOPIC_METRICS).expect("mq B");
    let mut rb = cfb.create_consumer();
    let late = rb.claim().await.expect("claim on fresh group");
    assert!(
        late.is_none(),
        "late joiner must not replay the fanout backlog"
    );
}

#[tokio::test]
async fn discovery_published_by_node_without_local_consumer() {
    let Some(url) = redis_url() else { return };
    let prefix = test_prefix("discovery");
    let host = node_factory(&url, &prefix, "host").await;
    let worker = node_factory(&url, &prefix, "worker").await;

    // Only the host registered/attaches a consumer for the discovery topic…
    let (_hp, ch) = host
        .create_queue(SYSTEM_TOPIC_WORKER_DISCOVERY)
        .expect("host queue");
    let mut rh = ch.create_consumer();
    let _ = tokio::time::timeout(Duration::from_secs(3), rh.claim()).await;

    // …while the emitting node has none: the message must still be published.
    worker
        .create_global_producer()
        .expect("producer")
        .send(message(SYSTEM_TOPIC_WORKER_DISCOVERY, b"worker announced"))
        .await
        .expect("cross-node system publish must not error or drop");

    let got = tokio::time::timeout(Duration::from_secs(5), rh.receive())
        .await
        .expect("host receives the worker's announcement")
        .expect("decodes");
    assert_eq!(got.payload.0, b"worker announced".to_vec());
}
