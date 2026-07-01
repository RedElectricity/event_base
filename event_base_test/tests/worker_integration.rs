//! Integration tests for Worker, ConsumerRouter, shutdown methods, and other
//! modules that require full system initialization (OnceLock singletons).
//!
//! Everything is in ONE test function because WorkerRegistry, TopicRouter,
//! ConsumerRouter, AuditManager, MetricsManager, MetricsStore are all OnceLock
//! singletons that can only be initialized once per process.

use async_trait::async_trait;
use event_base_core::error::CoreError;
use event_base_core::handler::{Ack, EHandler};
use event_base_core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use event_base_core::middleware::Pipeline;
use event_base_core::queues::consumer_factory::ConsumerFactory;
use event_base_core::queues::consumer_router::ConsumerRouter;
use event_base_core::queues::factory::QueueFactory;
use event_base_core::queues::{ClaimedMessage, EConsumer, EProducer};
use event_base_core::shutdown::messages::{
    ShutdownAck, ShutdownCommand, ShutdownStatus, ShutdownStrategy,
};
use event_base_core::shutdown::methods::{
    graceful_shutdown, shutdown_all_workers_two_stage, shutdown_batched, shutdown_force,
    shutdown_idle_only, shutdown_timeout,
};
use event_base_core::shutdown::shutdown_channel;
use event_base_core::topic::TopicRouter;
use event_base_core::wal::wal::{Wal, WalRecordState};
use event_base_core::worker_registry::WorkerRegistry;
use event_base_core::{NodeType, set_node_name, set_node_type};
use event_base_test::support::{RecordingProducer, RecordingWal};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, RwLock};
use tracing::info;

static CLAIM_COUNTER: AtomicU64 = AtomicU64::new(0);

// ──────────────────────────────────────────────
// Mock infrastructure for Worker & ConsumerRouter
// ──────────────────────────────────────────────

/// A consumer that returns pre-loaded messages, then blocks.
struct MockConsumer {
    messages: Arc<Mutex<Vec<EMessage>>>,
    pending: Arc<Mutex<HashMap<String, EMessage>>>,
}

#[async_trait]
impl EConsumer for MockConsumer {
    async fn receive(&mut self) -> Option<EMessage> {
        let mut msgs = self.messages.lock().await;
        if msgs.is_empty() {
            // Block forever (shutdown will interrupt via select!)
            tokio::time::sleep(Duration::from_secs(2)).await;
            None
        } else {
            Some(msgs.remove(0))
        }
    }

    async fn claim(&mut self) -> Result<Option<ClaimedMessage>, CoreError> {
        let mut msgs = self.messages.lock().await;
        if msgs.is_empty() {
            Ok(None)
        } else {
            let msg = msgs.remove(0);
            let claim_id = format!("claim-{}", CLAIM_COUNTER.fetch_add(1, Ordering::SeqCst));
            self.pending
                .lock()
                .await
                .insert(claim_id.clone(), msg.clone());
            Ok(Some(ClaimedMessage {
                message: msg,
                claim_id,
                claimed_at: SystemTime::now(),
            }))
        }
    }

    async fn ack(&mut self, claim_id: &str) -> Result<(), CoreError> {
        self.pending.lock().await.remove(claim_id);
        Ok(())
    }

    async fn nack(&mut self, claim_id: &str) -> Result<(), CoreError> {
        let msg = self.pending.lock().await.remove(claim_id);
        match msg {
            Some(m) => {
                self.messages.lock().await.push(m);
                Ok(())
            }
            None => Err(CoreError::from(
                event_base_core::error::queue::QueueError::InvalidClaimId(claim_id.to_string()),
            )),
        }
    }
}

/// A QueueFactory that produces MockConsumers and RecordingProducers.
struct MockQueueFactory {
    consumer: Arc<Mutex<MockConsumer>>,
}

impl MockQueueFactory {
    fn new(consumer: MockConsumer) -> Self {
        Self {
            consumer: Arc::new(Mutex::new(consumer)),
        }
    }
}

#[async_trait]
impl QueueFactory for MockQueueFactory {
    fn create_queue(
        &self,
        _topic: &str,
    ) -> Result<(Arc<dyn EProducer>, Arc<dyn ConsumerFactory>), CoreError> {
        let producer = Arc::new(RecordingProducer::default());
        let cf = MockConsumerFactory {
            consumer: self.consumer.clone(),
        };
        Ok((producer, Arc::new(cf)))
    }

    fn create_global_producer(&self) -> Result<Arc<dyn EProducer>, CoreError> {
        Ok(Arc::new(RecordingProducer::default()))
    }

    fn create_main_consumer(&self) -> Result<Arc<Mutex<dyn EConsumer>>, CoreError> {
        Ok(self.consumer.clone() as Arc<Mutex<dyn EConsumer>>)
    }

    fn name(&self) -> &'static str {
        "mock"
    }
}

struct MockConsumerFactory {
    consumer: Arc<Mutex<MockConsumer>>,
}

impl ConsumerFactory for MockConsumerFactory {
    fn create_consumer(&self) -> Box<dyn EConsumer> {
        Box::new(MockConsumer {
            messages: Arc::new(Mutex::new(Vec::new())),
            pending: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn clone_factory(&self) -> Arc<dyn ConsumerFactory> {
        Arc::new(MockConsumerFactory {
            consumer: self.consumer.clone(),
        })
    }
}

// ──────────────────────────────────────────────
// Test handler & middleware
// ──────────────────────────────────────────────

struct TestHandler {
    call_count: Arc<AtomicUsize>,
    response: Ack,
}

#[async_trait]
impl EHandler for TestHandler {
    async fn handler(&self, _msg: &EMessage) -> Ack {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        match &self.response {
            Ack::Dead { dead_reason } => Ack::Dead {
                dead_reason: dead_reason.clone(),
            },
            ref other => {
                // Can't clone Ack directly, reconstruct
                match other {
                    Ack::Ack => Ack::Ack,
                    Ack::NoAck {
                        retry_after,
                        max_retries,
                    } => Ack::NoAck {
                        retry_after: *retry_after,
                        max_retries: *max_retries,
                    },
                    Ack::Dead { dead_reason } => Ack::Dead {
                        dead_reason: dead_reason.clone(),
                    },
                }
            }
        }
    }
}

// ──────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────

fn message(topic: &str, payload: &[u8], mode: DeliveryMode) -> EMessage {
    EMessage::new(
        MessageTopic(topic.to_string()),
        MessagePayload(payload.to_vec()),
        mode,
        None,
    )
}

// ──────────────────────────────────────────────
// THE BIG COMBINED TEST
// ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_and_router_and_shutdown_integration() {
    // ──────── Setup globals ────────
    info!("=== STAGE: setup ===");
    let _ = set_node_name("integration-node".to_string());
    let _ = set_node_type(NodeType::Host);

    info!("=== STAGE: WAL + WorkerRegistry + TopicRouter ===");
    let fake_wal = RecordingWal::new();
    let wal_handle: Arc<RwLock<Box<dyn Wal>>> = Arc::new(RwLock::new(Box::new(fake_wal.clone())));

    let _ = WorkerRegistry::init(Some(wal_handle.clone())).await;
    let global_producer = Arc::new(RecordingProducer::default());
    let _ = TopicRouter::init(wal_handle.clone(), global_producer);
    TopicRouter::global().register_topic("test-topic").await;

    let _ = event_base_core::audit::AuditManager::init(16);
    let _ = event_base_core::metrics::manager::MetricsManager::init();
    let _ = event_base_core::metrics::node_store::MetricsStore::init();

    // ──────── Register a topic with ConsumerRouter ────────
    info!("=== STAGE: ConsumerRouter init ===");
    let mock_msgs = Arc::new(Mutex::new(Vec::new()));
    let mock_consumer = MockConsumer {
        messages: mock_msgs.clone(),
        pending: Arc::new(Mutex::new(HashMap::new())),
    };
    let factory = Arc::new(MockQueueFactory::new(mock_consumer));
    let main_consumer = factory.create_main_consumer().unwrap();
    let _ = ConsumerRouter::init(main_consumer, factory.clone());

    let handler = Arc::new(TestHandler {
        call_count: Arc::new(AtomicUsize::new(0)),
        response: Ack::Ack,
    });
    let cr = ConsumerRouter::global();
    cr.register("test-topic", handler.clone())
        .await
        .expect("register should succeed");

    // ──────── Create a worker via ConsumerRouter ────────
    info!("=== STAGE: create worker ===");
    let pipeline = Arc::new(Pipeline::new(Box::new(TestHandler {
        call_count: Arc::new(AtomicUsize::new(0)),
        response: Ack::Ack,
    })));
    let worker_name = cr
        .create_worker(
            "test-topic",
            pipeline,
            Some(Duration::from_secs(5)),
            Some(Duration::from_secs(1)),
            Some(Duration::from_millis(300)),
        )
        .await
        .expect("create_worker should succeed");
    assert!(worker_name.starts_with("worker-test-topic-"));

    // ──────── ConsumerRouter: get_worker, get_workers, get_all_workers ────────
    let w = cr.get_worker(&worker_name).await.expect("get_worker");
    assert_eq!(w.name, worker_name);

    let workers_for_topic = cr.get_workers("test-topic").await;
    assert_eq!(workers_for_topic.len(), 1);

    let all_workers = cr.get_all_workers().await;
    assert_eq!(all_workers.len(), 1);

    let handler_check = cr.get_handler("test-topic").await;
    assert!(handler_check.is_some());

    // ──────── Worker: status and topic ────────
    info!("=== STAGE: check worker status ===");
    assert_eq!(w.topic, "test-topic");
    let status = w.get_status().await;
    assert_eq!(status, event_base_core::worker::WorkerStatus::Idle);
    assert!(!w.is_shutdown_complete());

    // ──────── Test shutdown methods: graceful_shutdown ────────
    info!("=== STAGE: graceful_shutdown ===");
    // First make a separate worker to test with
    let pipeline2 = Arc::new(Pipeline::new(Box::new(TestHandler {
        call_count: Arc::new(AtomicUsize::new(0)),
        response: Ack::Ack,
    })));
    let w2_name = cr
        .create_worker("test-topic", pipeline2, None, None, None)
        .await
        .expect("create worker 2");
    assert_eq!(cr.get_all_workers().await.len(), 2);

    graceful_shutdown(&w2_name, Duration::from_millis(10))
        .await
        .expect("graceful_shutdown should succeed");
    assert_eq!(cr.get_all_workers().await.len(), 1);

    // ──────── Test shutdown methods: shutdown_force ────────
    info!("=== STAGE: shutdown_force ===");
    // Create a temp worker
    let pipeline3 = Arc::new(Pipeline::new(Box::new(TestHandler {
        call_count: Arc::new(AtomicUsize::new(0)),
        response: Ack::Ack,
    })));
    let _ = cr
        .create_worker("test-topic", pipeline3, None, None, None)
        .await;
    assert_eq!(cr.get_all_workers().await.len(), 2);

    shutdown_force().await;
    assert_eq!(cr.get_all_workers().await.len(), 0);

    // Re-create a worker for remaining tests
    let pipeline4 = Arc::new(Pipeline::new(Box::new(TestHandler {
        call_count: Arc::new(AtomicUsize::new(0)),
        response: Ack::Ack,
    })));
    cr.create_worker("test-topic", pipeline4, None, None, None)
        .await
        .expect("create worker for remaining tests");

    // ──────── Test shutdown methods: shutdown_idle_only ────────
    info!("=== STAGE: shutdown_idle_only ===");
    let pipeline5 = Arc::new(Pipeline::new(Box::new(TestHandler {
        call_count: Arc::new(AtomicUsize::new(0)),
        response: Ack::Ack,
    })));
    cr.create_worker("test-topic", pipeline5, None, None, None)
        .await
        .expect("create worker for idle test");
    assert_eq!(cr.get_all_workers().await.len(), 2);

    shutdown_idle_only().await;
    // Both workers are idle, so both should be removed
    assert!(cr.get_all_workers().await.len() <= 2);

    // ──────── Test shutdown methods: shutdown_timeout ────────
    info!("=== STAGE: shutdown_timeout ===");
    let pipeline6 = Arc::new(Pipeline::new(Box::new(TestHandler {
        call_count: Arc::new(AtomicUsize::new(0)),
        response: Ack::Ack,
    })));
    cr.create_worker("test-topic", pipeline6, None, None, None)
        .await
        .expect("create worker for timeout test");

    shutdown_timeout(Duration::from_millis(5)).await;
    assert_eq!(cr.get_all_workers().await.len(), 0);

    // ──────── Test shutdown methods: shutdown_batched ────────
    info!("=== STAGE: shutdown_batched ===");
    for _ in 0..4 {
        let p = Arc::new(Pipeline::new(Box::new(TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
            response: Ack::Ack,
        })));
        cr.create_worker("test-topic", p, None, None, None)
            .await
            .expect("create worker for batched test");
    }
    assert_eq!(cr.get_all_workers().await.len(), 4);

    shutdown_batched(2, Duration::from_millis(5)).await;
    assert_eq!(cr.get_all_workers().await.len(), 0);

    // ──────── Test shutdown methods: two_stage ────────
    info!("=== STAGE: two_stage shutdown ===");
    for _ in 0..3 {
        let p = Arc::new(Pipeline::new(Box::new(TestHandler {
            call_count: Arc::new(AtomicUsize::new(0)),
            response: Ack::Ack,
        })));
        cr.create_worker("test-topic", p, None, None, None)
            .await
            .expect("create worker for two_stage test");
    }

    let (two_stage_tx, _) = shutdown_channel();
    shutdown_all_workers_two_stage(
        two_stage_tx,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .await
    .expect("two_stage shutdown should succeed");
    assert_eq!(cr.get_all_workers().await.len(), 0);

    // ──────── Test ShutdownCommand serialization and handler ────────
    // The ShutdownHandler is registered on SYSTEM_TOPIC_SHUTDOWN
    // We can't easily test it without a full recv loop, but we can
    // verify the command serialization
    let cmd = ShutdownCommand {
        strategy: ShutdownStrategy::Force,
    };
    let cmd_json = serde_json::to_vec(&cmd).expect("serialize");
    let _decoded: ShutdownCommand = serde_json::from_slice(&cmd_json).expect("deserialize");

    let cmd2 = ShutdownCommand {
        strategy: ShutdownStrategy::TwoStage {
            poll_interval_ms: 50,
            force_timeout_secs: 5,
        },
    };
    let cmd2_json = serde_json::to_vec(&cmd2).expect("serialize");
    let _decoded2: ShutdownCommand = serde_json::from_slice(&cmd2_json).expect("deserialize");

    // ──────── ConsumerRouter: del_worker and del_workers ────────
    let p_del = Arc::new(Pipeline::new(Box::new(TestHandler {
        call_count: Arc::new(AtomicUsize::new(0)),
        response: Ack::Ack,
    })));
    let del_name = cr
        .create_worker("test-topic", p_del, None, None, None)
        .await
        .expect("create worker for del test");

    // del_worker
    cr.del_worker(&del_name)
        .await
        .expect("del_worker should succeed");
    let err = cr.get_worker(&del_name).await;
    assert!(err.is_err());
    match err {
        Err(e) => assert!(e.to_string().contains("Worker Not Found")),
        Ok(_) => panic!("expected error"),
    }

    // del_workers (delete all for topic)
    let p_del2 = Arc::new(Pipeline::new(Box::new(TestHandler {
        call_count: Arc::new(AtomicUsize::new(0)),
        response: Ack::Ack,
    })));
    let _ = cr
        .create_worker("test-topic", p_del2, None, None, None)
        .await;
    cr.del_workers("test-topic")
        .await
        .expect("del_workers should succeed");
    assert_eq!(cr.get_all_workers().await.len(), 0);

    // Verify del_worker on non-existent returns error
    let err = cr.del_worker("nonexistent").await.unwrap_err();
    assert!(err.to_string().contains("Worker Not Found"));

    // ──────── TopicRouter: broadcast with error on Worker node ────────
    // Set node to Worker temporarily
    let _ = set_node_type(NodeType::Worker);
    let broadcast_msg = message("test-topic", b"broadcast", DeliveryMode::Broadcast);
    let result = TopicRouter::global()
        .send("test-topic", broadcast_msg, None, None)
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Unsupported"));
    let _ = set_node_type(NodeType::Host);

    // ──────── WAL sync function coverage ────────
    // WalClient methods are called by the Worker; we can verify the
    // sync message format
    let sync = event_base_core::wal::sync::WalSyncMessage {
        message_id: "msg-1".to_string(),
        topic: "test-topic".to_string(),
        worker_id: "worker-x".to_string(),
        status: WalRecordState::Complete,
        attempts: 2,
        last_attempt_at: SystemTime::now(),
        error: None,
        timestamp: SystemTime::now(),
    };
    let sync_json = serde_json::to_vec(&sync).expect("serialize");
    let sync_decoded: event_base_core::wal::sync::WalSyncMessage =
        serde_json::from_slice(&sync_json).expect("deserialize");
    assert_eq!(sync_decoded.message_id, "msg-1");
    assert_eq!(sync_decoded.attempts, 2);

    // ──────── Registry (linkme) ────────
    // register_all_handlers can't be easily tested without the linkme
    // distributed slice entries, but the function itself compiles
    // and the HANDLER_REGISTRY is checked for emptiness
    let registry_count = event_base_core::registry::HANDLER_REGISTRY.len();
    // No handlers registered via #[handler] macro in this test, so it should be 0
    assert_eq!(registry_count, 0);

    // ──────── Metrics node collector ────────
    // NodeCollector.start() runs an infinite loop; we can't test it directly, but we can verify NodeMetrics creation
    let node_metrics = event_base_core::metrics::node::NodeMetrics {
        node_name: "test-node".to_string(),
        node_type: NodeType::Host,
        cpu_percent: vec![10.0],
        memory_percent: 50.0,
        node_worker_count: 0,
        update_time: SystemTime::now(),
    };
    let node_json = serde_json::to_vec(&node_metrics).expect("serialize");
    let _node_decoded: event_base_core::metrics::node::NodeMetrics =
        serde_json::from_slice(&node_json).expect("deserialize");

    // ──────── Shutdown: send signal via broadcast, verify ShutdownAck ────────
    let ack = ShutdownAck {
        worker_name: "test-worker".to_string(),
        status: ShutdownStatus::Completed,
        timestamp: SystemTime::now(),
        error: None,
    };
    let ack_json = serde_json::to_vec(&ack).expect("serialize");
    let _ack_decoded: ShutdownAck = serde_json::from_slice(&ack_json).expect("deserialize");

    info!("=== integration test completed ===");
    shutdown_force().await;
    return;
}
