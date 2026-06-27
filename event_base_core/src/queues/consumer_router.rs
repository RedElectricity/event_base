use crate::error::CoreError;
use crate::error::topic::TopicError;
use crate::handler::EHandler;
use crate::middleware::Pipeline;
use crate::queues::consumer_factory::ConsumerFactory;
use crate::queues::factory::QueueFactory;
use crate::queues::{EConsumer, EProducer};
use crate::shutdown::ShutdownReceiver;
use crate::worker::Worker;
use crate::worker::WorkerStatus::Idle;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

static CONSUMER_ROUTER: OnceLock<Arc<ConsumerRouter>> = OnceLock::new();

pub struct ConsumerRouter {
    consumer: Arc<Mutex<dyn EConsumer>>,
    factory: Arc<dyn QueueFactory>,
    local_topics: RwLock<HashMap<String, TopicEntry>>, // (topic, workers_name)
    worker_index: RwLock<HashMap<String, (Arc<Worker>, JoinHandle<()>)>>, // (worker_name, worker)
    idle_workers: Mutex<HashMap<String, Vec<String>>>, // (topic, worker_name)
}

struct TopicEntry {
    pub producer: Arc<dyn EProducer>,
    pub consumer_factory: Arc<dyn ConsumerFactory>,
    pub handler: Arc<dyn EHandler>,
    pub workers: Vec<String>,
}

impl ConsumerRouter {
    pub fn init(
        consumer: Arc<Mutex<dyn EConsumer>>,
        factory: Arc<dyn QueueFactory>,
    ) -> Result<(), CoreError> {
        let router = Arc::new(ConsumerRouter {
            consumer,
            local_topics: RwLock::new(HashMap::new()),
            worker_index: RwLock::new(HashMap::new()),
            factory,
            idle_workers: Mutex::new(HashMap::new()),
        });
        CONSUMER_ROUTER
            .set(router)
            .map_err(|_| CoreError::AlreadyInitialized)?;
        Ok(())
    }

    pub fn global() -> Arc<ConsumerRouter> {
        CONSUMER_ROUTER
            .get()
            .expect("ConsumerRouter not initialized")
            .clone()
    }

    pub async fn recv(&self) -> Result<(), CoreError> {
        loop {
            let mut consumer = self.consumer.lock().await;
            let claimed = { consumer.claim().await? };

            let Some(claimed) = claimed else { continue };

            let msg = &claimed.message;
            let claim_id = &claimed.claim_id;

            let local_workers = self.local_topics.read().await;

            if !local_workers.contains_key(&msg.topic.0) {
                drop(local_workers);
                consumer.nack(claim_id).await?;
                continue;
            }

            let workers = self.worker_index.read().await;

            if let Some(target) = &msg.to_worker {
                if !workers.contains_key(target) {
                    drop(workers);
                    consumer.nack(claim_id).await?;
                    continue;
                }

                let (worker, _) = workers.get(target).unwrap();
                match worker.producer.send(msg.clone()).await {
                    Ok(_) => {
                        consumer.ack(claim_id).await?;
                    }
                    Err(e) => {
                        consumer.nack(claim_id).await?;
                        tracing::error!("Fail to dispatch message:{}", e);
                    }
                }
            } else {
                consumer.ack(claim_id).await?;
                let worker = self.select_local_idle_worker(&msg.topic.0).await;
                if let Some(w) = worker {
                    consumer.ack(claim_id).await?;
                    match w.producer.send(msg.clone()).await {
                        Ok(_) => {
                            consumer.ack(claim_id).await?;
                        }
                        Err(e) => {
                            consumer.nack(claim_id).await?;
                            tracing::error!("Fail to dispatch message:{}", e);
                        }
                    }
                } else {
                    drop(workers);
                    consumer.nack(claim_id).await?;
                    tracing::warn!("No free worker, requeue the message: {}", &msg.id);
                    continue;
                }
            }
        }
    }

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

    async fn select_local_idle_worker(&self, topic: &str) -> Option<Arc<Worker>> {
        let idle_workers = self.idle_workers.lock().await;
        let workers = idle_workers.get(topic);
        if let Some(workers) = workers {
            if workers.is_empty() {
                return None;
            }
            if let Some(name) = workers.first().clone() {
                if let Some((w, _)) = self.worker_index.read().await.get(name) {
                    return Some(w.clone());
                };
                return None;
            };
            return None;
        }
        None
    }

    pub async fn get_handler(&self, topic: &str) -> Option<Arc<dyn EHandler>> {
        let map = self.local_topics.read().await;
        map.get(topic).map(|e| e.handler.clone())
    }

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

    pub async fn create_worker(
        &self,
        topic: &str,
        pipeline: Arc<Pipeline>,
        timeout: Option<Duration>,
        shutdown_timeout: Option<Duration>,
        shutdown_check_interval: Option<Duration>,
        shutdown_rx: ShutdownReceiver,
    ) -> Result<String, CoreError> {
        let map = self.local_topics.read().await;
        let (producer, consumer_factory) = {
            let entry = map
                .get(topic)
                .ok_or_else(|| TopicError::NotFound(topic.to_string()))?;
            (entry.producer.clone(), entry.consumer_factory.clone())
        };

        let consumer = consumer_factory.create_consumer();

        let worker = Arc::new(Worker::new(
            topic.to_string(),
            consumer,
            pipeline,
            producer.clone(),
            timeout,
            shutdown_check_interval.unwrap_or(Duration::from_millis(50)),
            shutdown_timeout,
            shutdown_rx,
        ));

        let worker_handle = worker.clone();

        let handle = tokio::spawn(async move {
            worker_handle.start().await;
        });

        self.register_worker(topic, worker.clone(), handle).await?;

        Ok(worker.name.clone())
    }

    pub async fn get_worker(&self, worker_name: &str) -> Result<Arc<Worker>, CoreError> {
        let workers = self.worker_index.read().await;
        let (worker, _) = workers.get(worker_name).ok_or_else(|| {
            return CoreError::WorkerNotFound(worker_name.to_string());
        })?;
        Ok(worker.clone())
    }

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

    pub async fn get_all_workers(&self) -> Vec<Arc<Worker>> {
        let worker_map = self.worker_index.read().await;
        worker_map
            .values()
            .map(|(worker, _)| worker.clone())
            .collect()
    }

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
