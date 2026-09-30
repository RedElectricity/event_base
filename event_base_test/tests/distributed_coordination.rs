//! End‑to‑end coverage for the coordination plane that was previously only
//! half‑wired: worker discovery announcements → `WorkerRegistry` → broadcast
//! fan‑out, heartbeat + stale sweep, and the targeted‑message relay/drop path
//! for unreachable `to_worker` targets.
//!
//! Boots the same primitives `start_system!` uses, on the production memory
//! backend (crossfire), as a single Host process.

use async_trait::async_trait;
use event_base_core::handler::{Ack, EHandler};
use event_base_core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use event_base_core::middleware::Pipeline;
use event_base_core::queues::consumer_router::ConsumerRouter;
use event_base_core::queues::factory::QueueFactory;
use event_base_core::shutdown::shutdown_channel;
use event_base_core::system_handlers::system::SystemHandlerBuilder;
use event_base_core::system_handlers::worker::{WORKER_STALE_TIMEOUT, heartbeat_once};
use event_base_core::topic::TopicRouter;
use event_base_core::wal::wal::Wal;
use event_base_core::worker_registry::{WorkerInfo, WorkerRegistry};
use event_base_core::{NodeType, set_node_name, set_node_type};
use event_base_queue::crossfire;
use event_base_wal::memory::MemoryWal;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, RwLock};

#[derive(Default)]
struct CountingHandler {
    calls: AtomicUsize,
    seen_targets: Mutex<Vec<Option<String>>>,
}

#[async_trait]
impl EHandler for CountingHandler {
    async fn handler(&self, msg: &EMessage) -> Ack {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.seen_targets.lock().await.push(msg.to_worker.clone());
        Ack::Ack
    }
}

fn total(h: &[Arc<CountingHandler>]) -> usize {
    h.iter().map(|c| c.calls.load(Ordering::SeqCst)).sum()
}

async fn poll_until<F, Fut>(timeout: Duration, what: &str, mut probe: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if probe().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met within {timeout:?}: {what}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn registered_names(topic: &str) -> Vec<String> {
    WorkerRegistry::global()
        .read()
        .await
        .get_workers(topic)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|i| i.worker_name)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordination_plane_end_to_end() {
    set_node_name("coord-node".to_string());
    set_node_type(NodeType::Host);

    let registry_wal: Arc<RwLock<Box<dyn Wal>>> = Arc::new(RwLock::new(Box::new(MemoryWal::new())));
    let factory = Arc::new(crossfire::MemoryQueueFactory::new(10_000));
    TopicRouter::init(factory.create_global_producer().expect("global producer")).expect("init");
    ConsumerRouter::init(
        factory.create_main_consumer().expect("main consumer"),
        factory.clone(),
        None,
    )
    .expect("consumer router init");
    WorkerRegistry::init(Some(registry_wal))
        .await
        .expect("registry");
    let (shutdown_tx, _shutdown_rx) = shutdown_channel();
    // The WalSyncHandler's WAL handle is a second fresh MemoryWal: WalSync is
    // not exercised by this test (the booted system only wires it Host‑side).
    SystemHandlerBuilder::new(Arc::new(RwLock::new(MemoryWal::new())), shutdown_tx, 32)
        .register_all()
        .await
        .expect("register_all");

    // ── two business workers on one topic ────────────────────────────────
    let handler_a = Arc::new(CountingHandler::default());
    let handler_b = Arc::new(CountingHandler::default());
    let entry_handler = Arc::new(CountingHandler::default());
    let all = [handler_a.clone(), handler_b.clone(), entry_handler.clone()];

    let router = ConsumerRouter::global();
    router
        .write()
        .await
        .register("orders", entry_handler.clone())
        .await
        .expect("register orders");
    let name_a = router
        .write()
        .await
        .create_worker(
            "orders",
            Arc::new(Pipeline::from_arc(handler_a.clone())),
            None,
            None,
            None,
        )
        .await
        .expect("worker a");
    let name_b = router
        .write()
        .await
        .create_worker(
            "orders",
            Arc::new(Pipeline::from_arc(handler_b.clone())),
            None,
            None,
            None,
        )
        .await
        .expect("worker b");

    // Worker names carry the node qualifier (globally unique → the key that
    // cross‑node `to_worker` routing hangs off).
    assert!(name_a.starts_with("coord-node@worker-orders-"), "{name_a}");
    assert!(name_b.starts_with("coord-node@worker-orders-"), "{name_b}");

    // ── STAGE 1: discovery announcements populate the registry ───────────
    // This was the unwired half: nothing emitted discovery messages, so the
    // registry — and with it broadcast / list_workers — was always empty.
    let (a1, b1) = (name_a.clone(), name_b.clone());
    poll_until(Duration::from_secs(10), "both workers registered", || {
        let (a1, b1) = (a1.clone(), b1.clone());
        async move {
            let names = registered_names("orders").await;
            names.contains(&a1) && names.contains(&b1)
        }
    })
    .await;

    // ── STAGE 2: broadcast fans out one copy per registered worker ───────
    let before = total(&all);
    TopicRouter::global()
        .read()
        .await
        .send(
            "orders",
            EMessage::new(
                MessageTopic("orders".into()),
                MessagePayload(b"bc".to_vec()),
                DeliveryMode::Broadcast,
                None,
            ),
            None,
            None,
        )
        .await
        .expect("broadcast send");
    poll_until(Duration::from_secs(10), "both handlers saw one copy", {
        let all = all.clone().to_vec();
        move || {
            let all = all.clone();
            async move { total(&all) == before + 2 }
        }
    })
    .await;
    // Each worker must see only its own target (misrouted copies were
    // relayed before processing, never handled by the wrong worker).
    assert_eq!(
        handler_a.seen_targets.lock().await.as_slice(),
        &[Some(name_a.clone())]
    );
    assert_eq!(
        handler_b.seen_targets.lock().await.as_slice(),
        &[Some(name_b.clone())]
    );

    // ── STAGE 3: heartbeat updates, stale sweep drops only the zombie ────
    WorkerRegistry::global()
        .read()
        .await
        .register(WorkerInfo {
            worker_name: "coord-node@worker-orders-zombie".to_string(),
            topic: "orders".to_string(),
            last_heartbeat: SystemTime::now() - WORKER_STALE_TIMEOUT * 2,
        })
        .await
        .expect("seed zombie");
    heartbeat_once(false).await;
    let (a3, b3) = (name_a.clone(), name_b.clone());
    poll_until(Duration::from_secs(10), "heartbeats applied", || {
        let (a3, b3) = (a3.clone(), b3.clone());
        async move {
            let infos: Vec<WorkerInfo> = WorkerRegistry::global()
                .read()
                .await
                .get_workers("orders")
                .await
                .unwrap_or_default();
            infos.iter().any(|i| i.worker_name == a3)
                && infos.iter().any(|i| i.worker_name == b3)
                && infos
                    .iter()
                    .any(|i| i.worker_name == "coord-node@worker-orders-zombie")
        }
    })
    .await;
    let removed = WorkerRegistry::global()
        .read()
        .await
        .cleanup_stale(WORKER_STALE_TIMEOUT)
        .await
        .expect("cleanup");
    assert_eq!(removed, vec!["coord-node@worker-orders-zombie".to_string()]);
    let names = registered_names("orders").await;
    assert!(
        names.contains(&name_a) && names.contains(&name_b),
        "live workers evicted: {names:?}"
    );

    // ── STAGE 4: targeted message for an unreachable worker terminates ───
    let before4 = total(&all);
    TopicRouter::global()
        .read()
        .await
        .send(
            "orders",
            EMessage::new(
                MessageTopic("orders".into()),
                MessagePayload(b"ghost".to_vec()),
                DeliveryMode::Standard,
                Some("ghost-node@worker-orders-gone".to_string()),
            ),
            None,
            None,
        )
        .await
        .expect("ghost-targeted send");
    // Follow with a normal message: it must still be handled, proving the
    // relay loop terminated at the hop cap instead of wedging the queue.
    TopicRouter::global()
        .read()
        .await
        .send(
            "orders",
            EMessage::new(
                MessageTopic("orders".into()),
                MessagePayload(b"after".to_vec()),
                DeliveryMode::Standard,
                None,
            ),
            None,
            None,
        )
        .await
        .expect("followup send");
    let all4 = all.clone();
    poll_until(
        Duration::from_secs(20),
        "followup handled, ghost never handled",
        {
            move || {
                let all4 = all4.to_vec();
                async move { total(&all4) == before4 + 1 }
            }
        },
    )
    .await;
    // The ghost copy must not be in anyone's handled set:
    let ghosts_visible = handler_a
        .seen_targets
        .lock()
        .await
        .iter()
        .chain(handler_b.seen_targets.lock().await.iter())
        .any(|t| t.as_deref() == Some("ghost-node@worker-orders-gone"));
    assert!(!ghosts_visible, "ghost-targeted message was processed");
}
