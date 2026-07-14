//! Router that distributes claimed messages to local workers.
//!
//! The [`ConsumerRouter`] is the core dispatcher: it claims messages from the
//! main consumer, selects an idle worker for the message's topic, and forwards
//! the message to that worker's internal producer. It also manages worker
//! lifecycles (creation, registration, deletion).

use crate::error::CoreError;
use crate::error::topic::TopicError;
use crate::handler::EHandler;
use crate::message::EMessage;
use crate::middleware::Pipeline;
use crate::queues::consumer_factory::ConsumerFactory;
use crate::queues::factory::QueueFactory;
use crate::queues::{EConsumer, EProducer};
use crate::worker::{LocalInboxConsumer, Worker};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::error;

static CONSUMER_ROUTER: OnceLock<RwLock<ConsumerRouter>> = OnceLock::new();

const DEFAULT_BATCH_SIZE: usize = 64;

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
    worker_index: RwLock<HashMap<String, (Arc<Worker>, JoinHandle<()>)>>, // (worker_name -> (worker, handle))
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
                    workers.get(target).map(|(w, _)| w.clone())
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
                    None => {
                        // ── Dynamic scaling: spawn an ephemeral one‑shot worker ──
                        let topic = msg.topic.0.clone();
                        match self.create_ephemeral_worker(&topic, msg).await {
                            Ok(_) => to_ack.push(claim_id),
                            Err(e) => {
                                to_nack.push(claim_id);
                                tracing::error!(
                                    "Failed to create ephemeral worker: {}", e
                                );
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

    fn spawn_topic_dispatcher(&self, topic: String, mut consumer: Box<dyn EConsumer>) {
        let dispatch_workers = self.dispatch_workers.clone();
        let local_inboxes = self.local_inboxes.clone();
        let dispatch_generation = self.dispatch_generation.clone();
        tokio::spawn(async move {
            let mut cursor: usize = 0;
            let mut seen_generation = u64::MAX;
            let mut cached_workers: Vec<(String, mpsc::Sender<EMessage>)> = Vec::new();

            loop {
                let Some(mut msg) = consumer.receive().await else {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
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
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                }

                let index = cursor % cached_workers.len();
                cursor = cursor.wrapping_add(1);
                let (target, inbox) = cached_workers[index].clone();
                msg.to_worker = Some(target.clone());
                if let Err(e) = inbox.try_send(msg) {
                    tracing::warn!(topic = %topic, worker = %target, error = %e, "topic dispatcher local inbox full or closed");
                    seen_generation = u64::MAX;
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
            self.spawn_topic_dispatcher(topic.to_string(), consumer);
        }

        let worker_handle = worker.clone();

        let handle = tokio::spawn(async move {
            worker_handle.start().await;
        });

        self.register_worker(topic, worker.clone(), handle).await?;

        Ok(worker.name.clone())
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
    async fn create_ephemeral_worker(
        &self,
        topic: &str,
        msg: EMessage,
    ) -> Result<(), CoreError> {
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
            None,                                  // no per‑message timeout
            Duration::from_millis(50),              // shutdown check interval
            None,                                  // no shutdown timeout
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
        let (worker, _) = workers.get(worker_name).ok_or_else(|| {
            return CoreError::WorkerNotFound(worker_name.to_string());
        })?;
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
