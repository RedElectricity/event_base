//! Router that distributes claimed messages to local workers.
//!
//! The [`ConsumerRouter`] is the core dispatcher: it claims messages from the
//! main consumer, selects an idle worker for the message's topic, and forwards
//! the message to that worker's internal producer. It also manages worker
//! lifecycles (creation, registration, deletion).

use crate::error::CoreError;
use crate::error::topic::TopicError;
use crate::handler::EHandler;
use crate::middleware::Pipeline;
use crate::queues::consumer_factory::ConsumerFactory;
use crate::queues::factory::QueueFactory;
use crate::queues::{EConsumer, EProducer};
use crate::worker::Worker;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::error;

static CONSUMER_ROUTER: OnceLock<Arc<ConsumerRouter>> = OnceLock::new();

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
    batch_size: usize,
}

/// Internal entry for a registered topic.
struct TopicEntry {
    pub producer: Arc<dyn EProducer>,
    pub consumer_factory: Arc<dyn ConsumerFactory>,
    pub handler: Arc<dyn EHandler>,
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
        let router = Arc::new(ConsumerRouter {
            consumer,
            local_topics: RwLock::new(HashMap::new()),
            worker_index: RwLock::new(HashMap::new()),
            factory,
            idle_workers: Mutex::new(HashMap::new()),
            batch_size: batch_size.unwrap_or(DEFAULT_BATCH_SIZE),
        });
        CONSUMER_ROUTER
            .set(router)
            .map_err(|_| CoreError::AlreadyInitialized)?;
        Ok(())
    }

    /// Returns a reference to the global consumer router.
    ///
    /// # Panics
    /// Panics if the router has not been initialized.
    pub fn global() -> Arc<ConsumerRouter> {
        CONSUMER_ROUTER
            .get()
            .expect("ConsumerRouter not initialized")
            .clone()
    }

    /// The main dispatch loop.
    ///
    /// It continuously claims messages from the main consumer. For each claimed
    /// message, it determines the target worker based on the `to_worker` field
    /// or by selecting an idle worker for the topic. It then forwards the message
    /// to the worker's producer and acknowledges the claim. If no worker is
    /// available, the message is negatively acknowledged and requeued.
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
                let msg = &claimed.message;
                let claim_id = claimed.claim_id.clone();

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
                        to_nack.push(claim_id);
                        tracing::warn!("No free worker, requeue the message: {}", msg.id);
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
        let (producer, consumer_factory) = self.factory.create_queue(topic)?;
        let mut map = self.local_topics.write().await;
        if map.contains_key(topic) {
            return Err(CoreError::from(TopicError::AlreadyExists(
                topic.to_string(),
            )));
        }
        map.insert(
            topic.to_string(),
            TopicEntry {
                producer,
                consumer_factory,
                handler,
                workers: vec![],
            },
        );
        Ok(())
    }

    /// Selects an idle worker for the given topic.
    ///
    /// Returns the first idle worker, or `None` if none are available.
    async fn select_local_idle_worker(&self, topic: &str) -> Option<Arc<Worker>> {
        let idle_workers = self.idle_workers.lock().await;
        let name = idle_workers.get(topic)?.first()?.clone();
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

        let worker_handle = worker.clone();

        let handle = tokio::spawn(async move {
            worker_handle.start().await;
        });

        self.register_worker(topic, worker.clone(), handle).await?;

        Ok(worker.name.clone())
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
            let mut map = self.local_topics.write().await;
            for entry in map.values_mut() {
                entry.workers.retain(|id| id != worker_name);
            }
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
                }
            }
        }
        entries.remove(topic);
        Ok(())
    }

    /// Marks a worker as idle for its topic (used internally).
    ///
    /// This adds the worker to the idle list so it can be selected for dispatch.
    pub(crate) async fn set_idle(
        &self,
        topic: String,
        worker_name: String,
    ) -> Result<(), CoreError> {
        let mut idle_workers = self.idle_workers.lock().await;
        if let Some(list) = idle_workers.get_mut(&topic) {
            list.push(worker_name);
        }
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
