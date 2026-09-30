//! Router that distributes claimed messages to local workers.
//!
//! The [`ConsumerRouter`] is the core dispatcher: it claims messages from the
//! main consumer, selects an idle worker for the message's topic, and forwards
//! the message to that worker's internal producer. It also manages worker
//! lifecycles (creation, registration, deletion).

use crate::constant::SYSTEM_TOPIC_WORKER_DISCOVERY;
use crate::error::CoreError;
use crate::error::topic::TopicError;
use crate::handler::EHandler;
use crate::message::DeliveryMode::Standard;
use crate::message::{EMessage, MessagePayload, MessageTopic};
use crate::middleware::Pipeline;
use crate::queues::consumer_factory::ConsumerFactory;
use crate::queues::factory::QueueFactory;
use crate::queues::{EConsumer, EProducer};
use crate::topic::TopicRouter;
use crate::worker::{LocalInboxConsumer, MAX_TARGETED_HOPS, Worker};
use crate::worker_registry::WorkerDiscoveryMessage;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;
use tracing::error;

static CONSUMER_ROUTER: OnceLock<RwLock<ConsumerRouter>> = OnceLock::new();

const DEFAULT_BATCH_SIZE: usize = 64;

/// How often a topic dispatcher sweeps for abandoned (unacknowledged) claims
/// left by consumers/nodes that died mid‑flight. On shared durable backends
/// (Redis consumer groups) this rescues those entries; in‑memory backends have
/// no cross‑process pending state, so the sweep is a no‑op there.
pub const TOPIC_REAP_INTERVAL: Duration = Duration::from_secs(30);

/// A claim must be idle at least this long before a dispatcher reclaims it —
/// comfortably above a healthy handler's worst case so a slow (but alive)
/// worker is never robbed mid‑processing.
pub const TOPIC_REAP_MIN_IDLE: Duration = Duration::from_secs(120);

/// Worker name → (`Worker` instance, join handle).
type WorkerIndex = HashMap<String, (Arc<Worker>, JoinHandle<()>)>;

/// The global router that dispatches messages to workers.
///
/// It maintains:
/// - A map of topics to their associated producer, consumer factory, handler, and workers.
/// - A map of worker names to their `Worker` instances and join handles.
/// - A list of idle workers per topic for fast dispatch.
pub struct ConsumerRouter {
    consumer: Arc<Mutex<dyn EConsumer>>,
    factory: Arc<dyn QueueFactory>,
    local_topics: RwLock<HashMap<String, TopicEntry>>, // (topic -> TopicEntry)
    worker_index: RwLock<WorkerIndex>,
    idle_workers: Mutex<HashMap<String, Vec<String>>>, // (topic -> list of idle worker names)
    dispatch_workers: Arc<Mutex<HashMap<String, Vec<String>>>>, // (topic -> worker names for topic dispatchers)
    dispatch_enabled: Arc<Mutex<HashMap<String, bool>>>, // (topic -> dispatcher/inbox mode enabled)
    local_inboxes: Arc<RwLock<HashMap<String, mpsc::Sender<EMessage>>>>, // (worker_name -> local inbox tx)
    dispatch_generation: Arc<AtomicU64>,
    batch_size: usize,
}

/// Internal entry for a registered topic.
struct TopicEntry {
    pub producer: Arc<dyn EProducer>,
    pub consumer_factory: Arc<dyn ConsumerFactory>,
    pub handler: Arc<dyn EHandler>,
    /// Optional pipeline template used when creating ephemeral (one‑shot)
    /// workers for dynamic scaling.  If `None`, the ephemeral worker will
    /// use a pipeline built from just the handler (no middleware).
    pub pipeline: Option<Arc<Pipeline>>,
    pub workers: Vec<String>, // names of workers for this topic
}

impl ConsumerRouter {
    /// Initializes the global consumer router.
    ///
    /// # Arguments
    /// * `consumer` - The main consumer (wrapped in `Mutex`) that will claim messages.
    /// * `factory` - The queue factory used to create resources.
    /// * `batch_size` - Maximum messages to claim in one batch. `None` defaults to 64.
    ///
    /// # Errors
    /// Returns `CoreError::AlreadyInitialized` if called more than once.
    pub fn init(
        consumer: Arc<Mutex<dyn EConsumer>>,
        factory: Arc<dyn QueueFactory>,
        batch_size: Option<usize>,
    ) -> Result<(), CoreError> {
        let router = ConsumerRouter {
            consumer,
            local_topics: RwLock::new(HashMap::new()),
            worker_index: RwLock::new(HashMap::new()),
            factory,
            idle_workers: Mutex::new(HashMap::new()),
            dispatch_workers: Arc::new(Mutex::new(HashMap::new())),
            dispatch_enabled: Arc::new(Mutex::new(HashMap::new())),
            local_inboxes: Arc::new(RwLock::new(HashMap::new())),
            dispatch_generation: Arc::new(AtomicU64::new(0)),
            batch_size: batch_size.unwrap_or(DEFAULT_BATCH_SIZE),
        };
        CONSUMER_ROUTER
            .set(RwLock::new(router))
            .map_err(|_| CoreError::AlreadyInitialized)?;
        Ok(())
    }

    /// Returns a reference to the global consumer router.
    ///
    /// # Panics
    /// Panics if the router has not been initialized.
    pub fn global() -> &'static RwLock<ConsumerRouter> {
        CONSUMER_ROUTER
            .get()
            .expect("ConsumerRouter not initialized")
    }

    /// The main dispatch loop.
    ///
    /// It continuously claims messages from the main consumer. For each claimed
    /// message, it determines the target worker based on the `to_worker` field
    /// or by selecting an idle worker for the topic. It then forwards the message
    /// to the worker's producer and acknowledges the claim.
    ///
    /// **Dynamic scaling**: when no idle worker is available for a topic, an
    /// ephemeral one‑shot worker is automatically created to handle the message.
    /// That worker processes exactly one message and then exits — it is never
    /// added to the idle pool and is cleaned up by the tokio runtime when the
    /// spawned task completes.
    ///
    /// Run the CR dispatch loop. Claims up to `self.batch_size` messages per
    /// batch to amortise lock contention, then dispatches each to the
    /// appropriate worker.  All acks/nacks for one batch are issued in a
    /// single lock acquisition.
    pub async fn recv(&self) -> Result<(), CoreError> {
        loop {
            // ── Batch claim ──
            let batch = {
                let mut consumer = self.consumer.lock().await;
                match consumer.claim_batch(self.batch_size).await {
                    Ok(b) => b,
                    Err(e) => {
                        error!("[CONSUMER ROUTER]Batch claim failed: {}", e);
                        continue;
                    }
                }
            };

            if batch.is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
                continue;
            }

            // ── Dispatch (consumer lock NOT held) + collect ack/nack IDs ──
            let mut to_ack: Vec<String> = Vec::with_capacity(batch.len());
            let mut to_nack: Vec<String> = Vec::with_capacity(4);

            for claimed in batch {
                let msg = claimed.message;
                let claim_id = claimed.claim_id;

                if !self.local_topics.read().await.contains_key(&msg.topic.0) {
                    to_nack.push(claim_id);
                    continue;
                }

                let worker = if let Some(target) = &msg.to_worker {
                    let workers = self.worker_index.read().await;
                    workers
                        .get(target.as_str())
                        // Bare (unqualified) target resolves against this
                        // node's workers; qualified targets only hit exactly.
                        .or_else(|| {
                            crate::try_get_node_name()
                                .and_then(|node| workers.get(&format!("{node}@{target}")))
                        })
                        .map(|(w, _)| w.clone())
                } else {
                    self.select_local_idle_worker(&msg.topic.0).await
                };

                match worker {
                    Some(w) => match w.producer.send(msg.clone()).await {
                        Ok(_) => to_ack.push(claim_id),
                        Err(e) => {
                            to_nack.push(claim_id);
                            tracing::error!("Fail to dispatch message:{}", e);
                        }
                    },
                    None if msg.to_worker.is_some() => {
                        // Targeted at a worker that is not local: relay it
                        // back onto the topic queue so the owning node's
                        // workers get another claim. Bounded by hops count to
                        // avoid a ping‑pong loop against a dead target.
                        if msg.attempts >= MAX_TARGETED_HOPS {
                            tracing::warn!(
                                target = %msg.to_worker.as_deref().unwrap_or("?"),
                                message_id = %msg.id,
                                "targeted message exceeded max relay hops; dropping"
                            );
                            to_ack.push(claim_id);
                        } else {
                            let relay = {
                                let mut m = msg.clone();
                                m.attempts += 1;
                                m
                            };
                            let topic_producer = self
                                .local_topics
                                .read()
                                .await
                                .get(&relay.topic.0)
                                .map(|e| e.producer.clone());
                            match topic_producer {
                                Some(p) => match p.send(relay).await {
                                    Ok(_) => to_ack.push(claim_id),
                                    Err(e) => {
                                        to_nack.push(claim_id);
                                        tracing::error!("Failed to relay targeted message: {e}");
                                    }
                                },
                                // No local producer for the topic: plain
                                // nack re‑injects the unchanged copy.
                                None => to_nack.push(claim_id),
                            }
                        }
                    }
                    None => {
                        // ── Dynamic scaling: spawn an ephemeral one‑shot worker ──
                        let topic = msg.topic.0.clone();
                        match self.create_ephemeral_worker(&topic, msg).await {
                            Ok(_) => to_ack.push(claim_id),
                            Err(e) => {
                                to_nack.push(claim_id);
                                tracing::error!("Failed to create ephemeral worker: {}", e);
                            }
                        }
                    }
                }
            }

            // ── Batch ack/nack (single consumer lock) ──
            if !to_ack.is_empty() || !to_nack.is_empty() {
                let mut consumer = self.consumer.lock().await;
                for id in &to_ack {
                    let _ = consumer.ack(id).await;
                }
                for id in &to_nack {
                    let _ = consumer.nack(id).await;
                }
            }
        }
    }

    /// Registers a topic with its handler.
    ///
    /// This creates a queue for the topic via the factory and stores the producer,
    /// consumer factory, and handler for future worker creation.
    ///
    /// # Errors
    /// Returns `CoreError::TopicAlreadyExists` if the topic is already registered.
    pub async fn register(&self, topic: &str, handler: Arc<dyn EHandler>) -> Result<(), CoreError> {
        let mut map = self.local_topics.write().await;
        if map.contains_key(topic) {
            return Err(CoreError::from(TopicError::AlreadyExists(
                topic.to_string(),
            )));
        }
        let (producer, consumer_factory) = self.factory.create_queue(topic)?;
        map.insert(
            topic.to_string(),
            TopicEntry {
                producer,
                consumer_factory: consumer_factory.clone(),
                handler,
                pipeline: None,
                workers: vec![],
            },
        );
        Ok(())
    }

    /// Per‑topic dispatch loop — the sole reader of a topic's queue while any
    /// of this node's workers is alive (workers sit on their inboxes).
    ///
    /// Targeting rules (this is what makes `to_worker` meaningful on shared
    /// backends — the loop must **honour** a pre‑set target, not overwrite it):
    /// * `to_worker` matches a local worker (qualified, or bare resolving to
    ///   this node's `{node}@{target}` form) → deliver to that inbox.
    /// * `to_worker` names a worker that is not local → this is another node's
    ///   message (or a dead target): ack, bump `attempts`, and re‑inject it on
    ///   the topic queue so the owning node gets a fresh claim. Bounded by
    ///   [`MAX_TARGETED_HOPS`] to terminate relay loops against dead workers.
    /// * no target → round‑robin among local workers, stamping the chosen
    ///   name so the worker's own filter passes it through.
    fn spawn_topic_dispatcher(
        &self,
        topic: String,
        mut consumer: Box<dyn EConsumer>,
        producer: Arc<dyn EProducer>,
    ) {
        let dispatch_workers = self.dispatch_workers.clone();
        let local_inboxes = self.local_inboxes.clone();
        let dispatch_generation = self.dispatch_generation.clone();
        tokio::spawn(async move {
            let mut cursor: usize = 0;
            let mut seen_generation = u64::MAX;
            let mut cached_workers: Vec<(String, mpsc::Sender<EMessage>)> = Vec::new();
            let node = crate::try_get_node_name();
            // Reclaimed PEL entries (from consumers that died mid‑claim) are
            // drained ahead of fresh claims. In‑memory backends return nothing
            // here, so this stays empty and behavior is unchanged.
            let mut reclaimed: std::collections::VecDeque<(EMessage, String)> =
                std::collections::VecDeque::new();
            let mut last_reap = tokio::time::Instant::now();

            loop {
                // ── Periodic stale sweep (crash recovery on shared backends) ──
                if last_reap.elapsed() >= TOPIC_REAP_INTERVAL {
                    last_reap = tokio::time::Instant::now();
                    match consumer.claim_stale(TOPIC_REAP_MIN_IDLE).await {
                        Ok(entries) => {
                            for c in entries {
                                reclaimed.push_back((c.message, c.claim_id));
                            }
                        }
                        Err(e) => {
                            tracing::debug!(topic = %topic, error = %e, "stale reap failed");
                        }
                    }
                }

                let (mut msg, claim_id) = match reclaimed.pop_front() {
                    Some(pair) => pair,
                    None => match consumer.claim().await {
                        Ok(Some(c)) => (c.message, c.claim_id),
                        Ok(None) | Err(_) => {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                            continue;
                        }
                    },
                };

                let current_generation = dispatch_generation.load(Ordering::Acquire);
                if current_generation != seen_generation || cached_workers.is_empty() {
                    let worker_names = {
                        let map = dispatch_workers.lock().await;
                        map.get(&topic).cloned().unwrap_or_default()
                    };
                    let inboxes = local_inboxes.read().await;
                    cached_workers = worker_names
                        .into_iter()
                        .filter_map(|name| inboxes.get(&name).cloned().map(|tx| (name, tx)))
                        .collect();
                    seen_generation = current_generation;
                    if !cached_workers.is_empty() {
                        cursor %= cached_workers.len();
                    }
                }

                if cached_workers.is_empty() {
                    // Nothing to deliver to yet: requeue so another node (or a
                    // worker started shortly) can take it, at a coarse cadence
                    // to keep this off the hot loop.
                    let _ = consumer.nack(&claim_id).await;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }

                // Resolve an explicit target against the local worker set.
                let local_match: Option<usize> = msg.to_worker.as_ref().and_then(|target| {
                    cached_workers.iter().position(|(name, _)| {
                        name == target
                            || node
                                .as_ref()
                                .is_some_and(|n| name == &format!("{n}@{target}"))
                    })
                });

                match local_match {
                    Some(index) => {
                        let (target, inbox) = cached_workers[index].clone();
                        msg.to_worker = Some(target.clone());
                        // Deliver FIRST, ack ONLY on success. A full or closed
                        // inbox must not cost the message (the old order — ack
                        // then a fallible try_send — silently dropped it).
                        match inbox.try_send(msg.clone()) {
                            Ok(()) => {
                                let _ = consumer.ack(&claim_id).await;
                            }
                            Err(e) => {
                                tracing::warn!(topic = %topic, worker = %target, error = %e, "local inbox full/closed; requeueing claim");
                                let _ = consumer.nack(&claim_id).await;
                                // Invalidate the cache so a *closed* worker is
                                // dropped from the set on the next pass.
                                seen_generation = u64::MAX;
                                tokio::time::sleep(Duration::from_millis(5)).await;
                            }
                        }
                    }
                    None if msg.to_worker.is_some() => {
                        let target = msg.to_worker.clone().unwrap_or_default();
                        if msg.attempts >= MAX_TARGETED_HOPS {
                            tracing::warn!(
                                topic = %topic,
                                target = %target,
                                message_id = %msg.id,
                                "targeted message exceeded max relay hops; dropping"
                            );
                            let _ = consumer.ack(&claim_id).await;
                            continue;
                        }
                        msg.attempts += 1;
                        // Ack‑then‑reinject: a "nack with mutation" that keeps
                        // the hop counter honest across nodes.
                        let _ = consumer.ack(&claim_id).await;
                        if let Err(e) = producer.send(msg).await {
                            tracing::warn!(topic = %topic, target = %target, error = %e, "failed to relay targeted message");
                        }
                    }
                    None => {
                        // Untargeted: round‑robin, stamping the selection.
                        let index = cursor % cached_workers.len();
                        cursor = cursor.wrapping_add(1);
                        let (target, inbox) = cached_workers[index].clone();
                        msg.to_worker = Some(target.clone());
                        match inbox.try_send(msg.clone()) {
                            Ok(()) => {
                                let _ = consumer.ack(&claim_id).await;
                            }
                            Err(e) => {
                                tracing::warn!(topic = %topic, worker = %target, error = %e, "local inbox full/closed; requeueing claim");
                                let _ = consumer.nack(&claim_id).await;
                                seen_generation = u64::MAX;
                                tokio::time::sleep(Duration::from_millis(5)).await;
                            }
                        }
                    }
                }
            }
        });
    }

    /// Associates a pipeline template with a topic, so that ephemeral
    /// (one‑shot) workers created during dynamic scaling can run with
    /// the full middleware chain instead of only the raw handler.
    ///
    /// # Errors
    /// Returns `CoreError::TopicNotFound` if the topic is not registered.
    pub async fn set_pipeline(
        &self,
        topic: &str,
        pipeline: Arc<Pipeline>,
    ) -> Result<(), CoreError> {
        let mut map = self.local_topics.write().await;
        let entry = map
            .get_mut(topic)
            .ok_or_else(|| TopicError::NotFound(topic.to_string()))?;
        entry.pipeline = Some(pipeline);
        Ok(())
    }

    /// Selects an idle worker for the given topic and removes it from the idle pool.
    ///
    /// Returns the first idle worker, or `None` if none are available.
    async fn select_local_idle_worker(&self, topic: &str) -> Option<Arc<Worker>> {
        let mut idle_workers = self.idle_workers.lock().await;
        let name = idle_workers.get_mut(topic)?.pop()?;
        self.worker_index
            .read()
            .await
            .get(&name)
            .map(|(w, _)| w.clone())
    }

    /// Returns the handler registered for the given topic.
    pub async fn get_handler(&self, topic: &str) -> Option<Arc<dyn EHandler>> {
        let map = self.local_topics.read().await;
        map.get(topic).map(|e| e.handler.clone())
    }

    /// Registers a worker for a topic.
    ///
    /// Adds the worker to the topic's worker list and to the global worker index.
    ///
    /// # Errors
    /// Returns `CoreError::TopicNotFound` if the topic is not registered.
    pub async fn register_worker(
        &self,
        topic: &str,
        worker: Arc<Worker>,
        handle: JoinHandle<()>,
    ) -> Result<(), CoreError> {
        let mut map = self.local_topics.write().await;
        let mut workers_map = self.worker_index.write().await;
        workers_map.insert(worker.name.clone(), (worker.clone(), handle));
        if let Some(entry) = map.get_mut(topic) {
            entry.workers.push(worker.name.clone());
            let mut dispatch_workers = self.dispatch_workers.lock().await;
            dispatch_workers
                .entry(topic.to_string())
                .or_default()
                .push(worker.name.clone());
            self.dispatch_generation.fetch_add(1, Ordering::Release);
            Ok(())
        } else {
            Err(CoreError::from(TopicError::NotFound(topic.to_string())))
        }
    }

    /// Creates a new worker for the given topic.
    ///
    /// It instantiates a `Worker` with the provided pipeline and shutdown settings,
    /// spawns its task, and registers it.
    ///
    /// # Arguments
    /// * `topic` - The topic to consume from.
    /// * `pipeline` - The processing pipeline (middleware + handler).
    /// * `timeout` - Optional per‑message processing timeout.
    /// * `shutdown_timeout` - Optional timeout for graceful shutdown.
    /// * `shutdown_check_interval` - Interval to check for idle status during shutdown.
    ///
    /// # Returns
    /// The name of the created worker.
    ///
    /// # Errors
    /// Returns `CoreError::TopicNotFound` if the topic is not registered.
    pub async fn create_worker(
        &self,
        topic: &str,
        pipeline: Arc<Pipeline>,
        timeout: Option<Duration>,
        shutdown_timeout: Option<Duration>,
        shutdown_check_interval: Option<Duration>,
    ) -> Result<String, CoreError> {
        let (producer, consumer_factory) = {
            let map = self.local_topics.read().await;
            let entry = map
                .get(topic)
                .ok_or_else(|| TopicError::NotFound(topic.to_string()))?;
            (entry.producer.clone(), entry.consumer_factory.clone())
        }; // ← map dropped here, releasing the read lock

        let consumer = consumer_factory.create_consumer();

        let worker = Arc::new(Worker::new(
            topic.to_string(),
            consumer,
            pipeline,
            producer.clone(),
            timeout,
            shutdown_check_interval.unwrap_or(Duration::from_millis(50)),
            shutdown_timeout,
        ));

        let (inbox_tx, inbox_consumer) = LocalInboxConsumer::new(self.batch_size * 4);
        worker.attach_inbox(inbox_consumer).await;
        self.local_inboxes
            .write()
            .await
            .insert(worker.name.clone(), inbox_tx);

        let should_start_dispatcher = {
            let mut enabled = self.dispatch_enabled.lock().await;
            match enabled.get(topic).copied() {
                Some(true) => false,
                _ => {
                    enabled.insert(topic.to_string(), true);
                    true
                }
            }
        };
        if should_start_dispatcher {
            let consumer = consumer_factory.create_consumer();
            self.spawn_topic_dispatcher(topic.to_string(), consumer, producer.clone());
        }

        let worker_handle = worker.clone();

        let handle = tokio::spawn(async move {
            worker_handle.start().await;
        });

        self.register_worker(topic, worker.clone(), handle).await?;

        self.announce_worker(&worker).await;

        Ok(worker.name.clone())
    }

    /// Publishes a `_system.worker_discovery` announcement so the Host's
    /// [`WorkerRegistry`](crate::worker_registry::WorkerRegistry) learns about
    /// a newly created local worker. This is what populates the registry that
    /// broadcast fan‑out, gRPC `list_workers` and stale cleanup depend on.
    ///
    /// Fire‑and‑forget: failures are logged, never propagate — worker creation
    /// must not fail because coordination chatter could not be sent. Ephemeral
    /// (one‑shot) workers are deliberately **not** announced: they are not
    /// addressable and vanish after one message.
    async fn announce_worker(&self, worker: &Worker) {
        // System workers are process‑local plumbing: they are reached through
        // the coordination plane itself (per‑node fan‑out groups on shared
        // backends), never through the Host's WorkerRegistry/broadcast path.
        // Announcing them would pollute the registry with hundreds of ghost
        // entries on every boot.
        if worker.topic.starts_with("_system.") {
            return;
        }
        let Some(router) = TopicRouter::try_global() else {
            return; // unit tests construct workers without a booted system
        };
        let discovery = WorkerDiscoveryMessage {
            worker_name: worker.name.clone(),
            topic: worker.topic.clone(),
            started_at: SystemTime::now(),
        };
        let mut msg = EMessage::new(
            MessageTopic(SYSTEM_TOPIC_WORKER_DISCOVERY.to_string()),
            MessagePayload(
                bincode::encode_to_vec(&discovery, bincode::config::standard()).unwrap_or_default(),
            ),
            Standard,
            None,
        );
        // `source` carries the emitting node id — the wire format stays at its
        // published shape (no struct change needed for node identity).
        msg.metadata.source = crate::try_get_node_name();
        // try_send, never blocking: worker creation must not stall on a full
        // coordination queue. The heartbeat loop periodically re‑announces
        // discovery, so a dropped announcement self‑heals.
        if let Err(e) = router.read().await.send_system(msg, Some(true), None).await {
            tracing::warn!(worker = %worker.name, error = %e, "failed to announce worker");
        }
    }

    /// Creates an **ephemeral one‑shot worker** for the given topic.
    ///
    /// Unlike [`create_worker`](Self::create_worker), this method:
    /// * Does **not** register the worker in the router's worker index or
    ///   idle pool — the worker is invisible to future dispatch.
    /// * Passes the message directly to the worker's `process_one` method
    ///   instead of entering an infinite receive loop.
    /// * The spawned task exits after processing the single message.
    ///
    /// This is the core of the dynamic‑scaling mechanism: when `recv()`
    /// finds no idle worker, it calls this method so the message is handled
    /// immediately without blocking.
    ///
    /// If the topic has a pipeline template set via
    /// [`set_pipeline`](Self::set_pipeline), the ephemeral worker uses it
    /// (preserving the full middleware chain).  Otherwise it falls back to
    /// a pipeline built from just the raw handler.
    ///
    /// # Errors
    /// Returns `CoreError::TopicNotFound` if the topic is not registered.
    async fn create_ephemeral_worker(&self, topic: &str, msg: EMessage) -> Result<(), CoreError> {
        let (producer, consumer_factory, handler, pipeline) = {
            let map = self.local_topics.read().await;
            let entry = map
                .get(topic)
                .ok_or_else(|| TopicError::NotFound(topic.to_string()))?;
            (
                entry.producer.clone(),
                entry.consumer_factory.clone(),
                entry.handler.clone(),
                entry.pipeline.clone(),
            )
        }; // ← map dropped, releasing the read lock

        let consumer = consumer_factory.create_consumer();

        // Use the stored pipeline template if available; otherwise build
        // a minimal pipeline from the raw handler (no middleware).
        let pipeline = pipeline.unwrap_or_else(|| Arc::new(Pipeline::from_arc(handler)));

        let worker = Arc::new(Worker::new(
            topic.to_string(),
            consumer,
            pipeline,
            producer,
            None,                      // no per‑message timeout
            Duration::from_millis(50), // shutdown check interval
            None,                      // no shutdown timeout
        ));

        let w = worker.clone();
        tokio::spawn(async move {
            w.process_one(msg).await;
        });

        Ok(())
    }

    /// Retrieves a worker by its name.
    ///
    /// # Errors
    /// Returns `CoreError::WorkerNotFound` if the worker does not exist.
    pub async fn get_worker(&self, worker_name: &str) -> Result<Arc<Worker>, CoreError> {
        let workers = self.worker_index.read().await;
        let (worker, _) = workers
            .get(worker_name)
            .ok_or_else(|| CoreError::WorkerNotFound(worker_name.to_string()))?;
        Ok(worker.clone())
    }

    /// Returns all workers for a given topic.
    pub async fn get_workers(&self, topic: &str) -> Vec<Arc<Worker>> {
        let entries = self.local_topics.read().await;
        let worker_map = self.worker_index.read().await;
        let mut workers = Vec::new();
        if let Some(entry) = entries.get(topic) {
            for worker_index in entry.workers.clone() {
                if let Some((worker, _)) = worker_map.get(&worker_index) {
                    workers.push(worker.clone());
                }
            }
        }
        workers
    }

    /// Returns all workers known to the router.
    pub async fn get_all_workers(&self) -> Vec<Arc<Worker>> {
        let worker_map = self.worker_index.read().await;
        worker_map
            .values()
            .map(|(worker, _)| worker.clone())
            .collect()
    }

    /// Deletes a worker by its name.
    ///
    /// The worker's task is aborted, and it is removed from all topic lists.
    ///
    /// # Errors
    /// Returns `CoreError::WorkerNotFound` if the worker does not exist.
    pub async fn del_worker(&self, worker_name: &str) -> Result<(), CoreError> {
        let mut workers = self.worker_index.write().await;
        if let Some((_worker, handle)) = workers.remove(worker_name) {
            handle.abort();
            self.local_inboxes.write().await.remove(worker_name);
            let mut map = self.local_topics.write().await;
            for entry in map.values_mut() {
                entry.workers.retain(|id| id != worker_name);
            }
            let mut dispatch_workers = self.dispatch_workers.lock().await;
            for workers in dispatch_workers.values_mut() {
                workers.retain(|id| id != worker_name);
            }
            self.dispatch_generation.fetch_add(1, Ordering::Release);
            Ok(())
        } else {
            Err(CoreError::WorkerNotFound(worker_name.to_string()))
        }
    }

    /// Deletes all workers for a given topic and removes the topic registration.
    ///
    /// All worker tasks are aborted, and the topic entry is removed.
    ///
    /// # Errors
    /// Returns `CoreError` if the topic does not exist or worker operations fail.
    pub async fn del_workers(&self, topic: &str) -> Result<(), CoreError> {
        let mut entries = self.local_topics.write().await;
        let mut worker_map = self.worker_index.write().await;
        if let Some(entry) = entries.get_mut(topic) {
            for worker_index in entry.workers.clone() {
                if let Some((_, handle)) = worker_map.remove(&worker_index) {
                    handle.abort();
                    self.local_inboxes.write().await.remove(&worker_index);
                }
            }
        }
        entries.remove(topic);
        self.dispatch_workers.lock().await.remove(topic);
        self.dispatch_enabled.lock().await.remove(topic);
        self.dispatch_generation.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Marks a worker as idle for its topic, adding it to the idle pool so it
    /// can be selected for dispatch.
    pub(crate) async fn set_idle(
        &self,
        topic: String,
        worker_name: String,
    ) -> Result<(), CoreError> {
        let mut idle_workers = self.idle_workers.lock().await;
        idle_workers.entry(topic).or_default().push(worker_name);
        Ok(())
    }

    /// Marks a worker as working (removes it from the idle list).
    pub(crate) async fn set_working(
        &self,
        topic: String,
        worker_name: String,
    ) -> Result<(), CoreError> {
        let mut idle_workers = self.idle_workers.lock().await;
        if let Some(list) = idle_workers.get_mut(&topic) {
            list.retain(|x| *x != worker_name)
        }
        Ok(())
    }
}
