#![cfg(feature = "redis")]

use event_base_core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use event_base_core::queues::factory::QueueFactory;
use event_base_queue::redis_streams::{RedisQueueConfig, RedisStreamQueueFactory};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Resolves the test Redis URL, or `None` (skip) when it is not configured.
///
/// Tests write under a unique key prefix and never delete, so point this at a
/// disposable database (CI uses an ephemeral container).
fn redis_url() -> Option<String> {
    match std::env::var("EVENT_BASE_TEST_REDIS_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            eprintln!(
                "SKIP redis_queue: set EVENT_BASE_TEST_REDIS_URL (e.g. redis://127.0.0.1:6379) \
                 to run the Redis Streams queue backend tests"
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
    format!("ebtest:{}:{tag}:{}", std::process::id(), nanos)
}

/// A factory with short block times tuned for tests (the default 500 ms would
/// slow the negative-claim assertions down).
async fn factory(tag: &str) -> Option<RedisStreamQueueFactory> {
    let url = redis_url()?;
    let config = RedisQueueConfig::new(url)
        .with_prefix(test_prefix(tag))
        .with_block_ms(50);
    Some(
        RedisStreamQueueFactory::with_config(config)
            .await
            .expect("build redis queue factory"),
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
async fn redis_health_check_and_name() {
    let Some(f) = factory("health").await else {
        return;
    };
    assert_eq!(f.name(), "redis");
    f.health_check().await.expect("redis should answer PING");
}

#[tokio::test]
async fn redis_send_and_receive_round_trip() {
    let Some(f) = factory("send-recv").await else {
        return;
    };
    let (producer, consumer_factory) = f.create_queue("t1").expect("create_queue");

    let msg = message("t1", b"hello redis");
    producer
        .send(msg.clone())
        .await
        .expect("send should succeed");
    // try_send is an alias of send for XADD.
    producer
        .try_send(msg.clone())
        .await
        .expect("try_send should succeed");
    producer
        .send_timeout(msg.clone(), Duration::from_secs(5))
        .await
        .expect("send_timeout should succeed");

    let mut consumer = consumer_factory.create_consumer();
    let first = tokio::time::timeout(Duration::from_secs(5), consumer.receive())
        .await
        .expect("a message should arrive")
        .expect("payload must decode");
    assert_eq!(first.id, msg.id);
    assert_eq!(first.payload, msg.payload);

    let second = tokio::time::timeout(Duration::from_secs(5), consumer.receive())
        .await
        .expect("second message should arrive")
        .expect("decodes");
    assert_eq!(second.id, msg.id);

    let third = tokio::time::timeout(Duration::from_secs(5), consumer.receive())
        .await
        .expect("third message should arrive")
        .expect("decodes");
    assert_eq!(third.id, msg.id);

    // Queue is now drained; receive must not fabricate a fourth message.
    let nothing = tokio::time::timeout(Duration::from_millis(400), consumer.receive()).await;
    assert!(
        nothing.is_err(),
        "receive should keep blocking with an empty stream"
    );
}

#[tokio::test]
async fn redis_claim_ack_nack_flow() {
    let Some(f) = factory("claim").await else {
        return;
    };
    let (producer, consumer_factory) = f.create_queue("t2").expect("create_queue");
    let mut consumer = consumer_factory.create_consumer();

    // Claiming an empty stream yields None (after the block timeout).
    let empty = consumer.claim().await.expect("claim should not error");
    assert!(empty.is_none());

    producer
        .send(message("t2", b"ack me"))
        .await
        .expect("send should succeed");
    let claimed = consumer
        .claim()
        .await
        .expect("claim should succeed")
        .expect("message should be claimable");
    assert_eq!(claimed.message.payload.0, b"ack me");
    consumer
        .ack(&claimed.claim_id)
        .await
        .expect("ack should succeed");

    // The claim is consumed: a second claim sees nothing new.
    let again = consumer.claim().await.expect("claim should not error");
    assert!(again.is_none(), "acked entries must not re-appear");

    // Unknown claim ids error like the memory backend.
    consumer
        .ack("no-such-claim")
        .await
        .expect_err("ack of an unknown claim must fail");
    consumer
        .nack("no-such-claim")
        .await
        .expect_err("nack of an unknown claim must fail");

    // nack re-injects the message for another delivery.
    producer
        .send(message("t2", b"retry me"))
        .await
        .expect("send should succeed");
    let claimed = consumer
        .claim()
        .await
        .expect("claim should succeed")
        .expect("message should be claimable");
    assert_eq!(claimed.message.payload.0, b"retry me");
    consumer
        .nack(&claimed.claim_id)
        .await
        .expect("nack should succeed");
    let redelivered = consumer
        .claim()
        .await
        .expect("claim should succeed")
        .expect("nacked message should be requeued");
    assert_eq!(redelivered.message.id, claimed.message.id);
    consumer
        .ack(&redelivered.claim_id)
        .await
        .expect("ack after retry should succeed");
}

#[tokio::test]
async fn redis_claim_batch_drains_pending() {
    let Some(f) = factory("batch").await else {
        return;
    };
    let (producer, consumer_factory) = f.create_queue("t3").expect("create_queue");
    for i in 0..4 {
        producer
            .send(message("t3", format!("m{i}").as_bytes()))
            .await
            .expect("send should succeed");
    }
    let mut consumer = consumer_factory.create_consumer();
    let batch = consumer
        .claim_batch(10)
        .await
        .expect("claim_batch should succeed");
    assert_eq!(batch.len(), 4, "all four messages should be claimed");
    let mut ids: Vec<Vec<u8>> = batch.iter().map(|c| c.message.payload.0.clone()).collect();
    ids.sort();
    assert_eq!(
        ids,
        vec![
            b"m0".to_vec(),
            b"m1".to_vec(),
            b"m2".to_vec(),
            b"m3".to_vec()
        ]
    );
    for claimed in batch {
        consumer
            .ack(&claimed.claim_id)
            .await
            .expect("ack should succeed");
    }
}

#[tokio::test]
async fn redis_group_consumers_do_not_double_deliver() {
    let Some(f) = factory("group").await else {
        return;
    };
    let (producer, consumer_factory) = f.create_queue("t4").expect("create_queue");

    // Two independent consumers (as two nodes would create) share the group:
    // each message lands with exactly one of them.
    let mut c1 = consumer_factory.create_consumer();
    let mut c2 = consumer_factory.create_consumer();
    producer
        .send(message("t4", b"only once"))
        .await
        .expect("send should succeed");

    let first = tokio::time::timeout(Duration::from_secs(5), c1.claim())
        .await
        .expect("one consumer should get it")
        .expect("claim should not error");
    assert!(first.is_some(), "a consumer should receive the message");
    let other = c2.claim().await.expect("claim should not error");
    assert!(
        other.is_none(),
        "the same entry must not be delivered twice"
    );
    c1.ack(&first.unwrap().claim_id)
        .await
        .expect("ack should succeed");
}

#[tokio::test]
async fn redis_routing_producer_honours_registration() {
    let Some(f) = factory("routing").await else {
        return;
    };

    // Unknown _system.* topics are **published** (they must be able to reach
    // a consumer registered on another node — e.g. worker discovery from a
    // Worker to the Host). Attaching a consumer later still sees them:
    // non‑fanout groups are created at id `0`.
    let global = f.create_global_producer().expect("global producer");
    global
        .send(message("_system.unseen", b"cross node"))
        .await
        .expect("unknown system topics must not error");
    let (_, unseen_factory) = f.create_queue("_system.unseen").expect("create_queue");
    let mut unseen = unseen_factory.create_consumer();
    let got = tokio::time::timeout(Duration::from_secs(5), unseen.receive())
        .await
        .expect("system topics are delivered to late groups")
        .expect("decodes");
    assert_eq!(got.payload.0, b"cross node");

    // A locally registered topic routes to its stream…
    let (topic_producer, consumer_factory) = f.create_queue("orders").expect("create_queue");
    let _ = topic_producer;
    global
        .send(message("orders", b"via router"))
        .await
        .expect("registered topics route through the global producer");
    let mut consumer = consumer_factory.create_consumer();
    let msg = tokio::time::timeout(Duration::from_secs(5), consumer.receive())
        .await
        .expect("the routed message should arrive")
        .expect("decodes");
    assert_eq!(msg.payload.0, b"via router");

    // …and unlike the memory backend, unknown *user* topics are still sent —
    // in a distributed setup other nodes own those streams.
    global
        .send(message("remote.only", b"cross node"))
        .await
        .expect("unknown user topics are published, not rejected");
}

#[tokio::test]
async fn redis_main_consumer_reads_default_stream() {
    let Some(f) = factory("main").await else {
        return;
    };
    // create_main_consumer points at `{prefix}:_default`; nothing is routed
    // there by default (topics get their own streams), so the claim loop just
    // has to report "empty" rather than error.
    let main = f.create_main_consumer().expect("main consumer");
    let mut guard = main.lock().await;
    let empty = guard.claim().await.expect("claim should not error");
    assert!(empty.is_none());
}
