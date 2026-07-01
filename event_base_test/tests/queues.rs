use event_base_core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use event_base_core::queues::{EConsumer, EProducer};
use event_base_queue::{flume, mpmc};

fn message(topic: &str, payload: &[u8]) -> EMessage {
    EMessage::new(
        MessageTopic(topic.to_string()),
        MessagePayload(payload.to_vec()),
        DeliveryMode::Standard,
        None,
    )
}

#[tokio::test]
async fn flume_queue_claim_ack_nack_and_timeout() {
    let (producer, mut consumer) = flume::memory_queue(1);

    let first = message("flume", b"first");
    producer
        .send(first.clone())
        .await
        .expect("send should succeed");
    let claimed = consumer
        .claim()
        .await
        .expect("claim should succeed")
        .expect("message should be claimed");
    assert_eq!(claimed.message.id, first.id);
    consumer
        .ack(&claimed.claim_id)
        .await
        .expect("ack should succeed");

    let second = message("flume", b"second");
    producer
        .send(second.clone())
        .await
        .expect("send should succeed");
    let claimed = consumer
        .claim()
        .await
        .expect("claim should succeed")
        .expect("message should be claimed");
    consumer
        .nack(&claimed.claim_id)
        .await
        .expect("nack should succeed");
    let requeued = consumer
        .receive()
        .await
        .expect("message should be requeued");
    assert_eq!(requeued.id, second.id);

    let invalid = consumer
        .nack("missing-claim")
        .await
        .expect_err("invalid claim should fail");
    assert!(invalid.to_string().contains("Invalid Claim Id"));
}

#[tokio::test]
async fn mpmc_queue_claim_ack_nack_and_timeout() {
    let (producer, mut consumer) = mpmc::memory_queue(1);

    let first = message("mpmc", b"first");
    producer
        .send(first.clone())
        .await
        .expect("send should succeed");
    let claimed = consumer
        .claim()
        .await
        .expect("claim should succeed")
        .expect("message should be claimed");
    assert_eq!(claimed.message.id, first.id);
    consumer
        .ack(&claimed.claim_id)
        .await
        .expect("ack should succeed");

    let second = message("mpmc", b"second");
    producer
        .send(second.clone())
        .await
        .expect("send should succeed");
    let claimed = consumer
        .claim()
        .await
        .expect("claim should succeed")
        .expect("message should be claimed");
    consumer
        .nack(&claimed.claim_id)
        .await
        .expect("nack should succeed");
    let requeued = consumer
        .receive()
        .await
        .expect("message should be requeued");
    assert_eq!(requeued.id, second.id);

    let invalid = consumer
        .nack("missing-claim")
        .await
        .expect_err("invalid claim should fail");
    assert!(invalid.to_string().contains("Invalid Claim Id"));
}
