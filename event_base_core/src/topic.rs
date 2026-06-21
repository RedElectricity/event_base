use crate::error::CoreError;
use crate::error::topic::TopicError;
use crate::handler::EHandler;
use crate::message::DeliveryMode::Broadcast;
use crate::message::{EMessage, MessageTopic};
use crate::middleware::Pipeline;
use crate::queues::consumer_factory::ConsumerFactory;
use crate::queues::factory::QueueFactory;
use crate::queues::{EConsumer, EProducer};
use crate::shutdown::ShutdownReceiver;
use crate::wal::wal::{Wal, WalRecord};
use crate::worker::Worker;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

static TOPIC_ROUTER: OnceLock<Arc<TopicRouter>> = OnceLock::new();

pub struct TopicRouter {
    inner: RwLock<HashMap<String, TopicEntry>>,
    factory: Arc<dyn QueueFactory>,
    wal: Option<Arc<tokio::sync::Mutex<dyn Wal>>>,
    workers: RwLock<HashMap<String, (Arc<Worker>, JoinHandle<()>)>>,
}

struct TopicEntry {
    pub producer: Arc<dyn EProducer>,
    pub consumer_factory: Arc<dyn ConsumerFactory>,
    pub handler: Arc<dyn EHandler>,
    pub workers: Vec<String>,
}

#[derive(Debug, Default)]
pub struct ReplaySummary {
    pub recovered: usize,
    pub delayed: usize,
    pub errors: Vec<(String, CoreError)>,
}

impl TopicRouter {
    pub fn init(
        wal: Option<Arc<tokio::sync::Mutex<dyn Wal>>>,
        factory: Arc<dyn QueueFactory>,
    ) -> Result<(), CoreError> {
        let router = Arc::new(TopicRouter {
            inner: RwLock::new(HashMap::new()),
            factory,
            wal,
            workers: RwLock::new(HashMap::new()),
        });
        TOPIC_ROUTER
            .set(router)
            .map_err(|_| CoreError::AlreadyInitialized)?;
        Ok(())
    }

    pub fn global() -> Arc<TopicRouter> {
        TOPIC_ROUTER
            .get()
            .expect("TopicRouter not initialized")
            .clone()
    }

    pub async fn replay(
        &self,
        wal: &Arc<tokio::sync::Mutex<dyn Wal>>,
        topics: Option<&[&str]>,
    ) -> Result<ReplaySummary, CoreError> {
        let mut wal = wal.lock().await;
        let pending = wal.replay_pending().await?;

        let mut summary = ReplaySummary::default();
        let topic_set: Option<HashSet<String>> =
            topics.map(|t| t.iter().map(|s| s.to_string()).collect());

        for record in pending {
            let msg = record.message;

            if let Some(ref allowed_topics) = topic_set {
                if !allowed_topics.contains(&msg.topic.0) {
                    continue;
                }
            }

            if let Some(deliver_at) = msg.deliver_at {
                if deliver_at > SystemTime::now() {
                    wal.schedule(WalRecord::from_msg(msg)).await?;
                    summary.delayed += 1;
                    continue;
                }
            }

            let mut msg = msg;
            msg.deliver_at = None;

            match self
                .send(&msg.clone().topic.0, msg.clone(), None, None)
                .await
            {
                Ok(_) => summary.recovered += 1,
                Err(e) => {
                    summary.errors.push((msg.id, e));
                }
            }
        }

        Ok(summary)
    }

    pub async fn register(&self, topic: &str, handler: Arc<dyn EHandler>) -> Result<(), CoreError> {
        let (producer, consumer_factory) = self.factory.create_queue(topic)?;
        let mut map = self.inner.write().await;
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

    pub async fn send(
        &self,
        topic: &str,
        mut msg: EMessage,
        try_send: Option<bool>,
        timeout: Option<Duration>,
    ) -> Result<(), CoreError> {
        if let Some(wal) = &self.wal {
            let record = WalRecord::from_msg(msg.clone());
            let mut wal = wal.lock().await;
            wal.append(record).await?;
        }

        if msg.deliver_at != None {
            if msg.deliver_at < Option::from(SystemTime::now()) {
                return Err(CoreError::ErrorTime);
            }
            if let Some(wal) = &self.wal {
                let record = WalRecord::from_msg(msg.clone());
                let wal = wal.lock().await;
                wal.schedule(record).await?;
            }
            return Ok(());
        }

        if msg.delivery_mode == Broadcast {
            let mut map = self.inner.write().await;
            if let Some(entry) = map.get_mut(topic) {
                for worker_index in &entry.workers {
                    let workers = self.workers.read().await;
                    let (worker, _) = workers.get(worker_index).unwrap();
                    let mut copy = msg.clone();
                    copy.id = format!("{}-{}", msg.id, worker.clone().name);
                    if try_send.unwrap_or(false) {
                        worker.producer.try_send(copy.clone())?;
                    } else if let Some(to) = timeout {
                        worker.producer.send_timeout(copy.clone(), to).await?;
                    } else {
                        worker.producer.send(copy).await?;
                    }
                }
            }
            return Ok(());
        }

        msg.topic = MessageTopic(topic.to_string());
        let producer = self
            .get_producer(topic)
            .await
            .ok_or_else(|| TopicError::NotFound(topic.to_string()))?;
        if try_send.unwrap_or(false) {
            producer.try_send(msg.clone())?;
        } else if let Some(to) = timeout {
            producer.send_timeout(msg.clone(), to).await?;
        } else {
            producer.send(msg).await?;
        }
        Ok(())
    }

    pub async fn run_delay_scheduler(wal: Arc<dyn Wal>, router: Arc<TopicRouter>) {
        loop {
            match wal.fetch_ready().await {
                Ok(ready_records) => {
                    for record in ready_records {
                        // 投递前清除 deliver_at（避免再次延迟）
                        let mut msg = record.message;
                        msg.deliver_at = None;
                        if let Err(e) = router.send(&msg.clone().topic.0, msg, None, None).await {
                            tracing::error!("Failed to deliver delayed message: {}", e);
                        }
                    }
                }
                Err(e) => tracing::error!("Failed to fetch ready: {}", e),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    pub async fn get_producer(&self, topic: &str) -> Option<Arc<dyn EProducer>> {
        let map = self.inner.read().await;
        map.get(topic).map(|e| e.producer.clone())
    }

    pub async fn create_consumer(&self, topic: &str) -> Option<Box<dyn EConsumer>> {
        let map = self.inner.read().await;
        map.get(topic)
            .map(|entry| entry.consumer_factory.create_consumer())
    }

    pub async fn get_handler(&self, topic: &str) -> Option<Arc<dyn EHandler>> {
        let map = self.inner.read().await;
        map.get(topic).map(|e| e.handler.clone())
    }

    pub async fn list_topics(&self) -> Vec<String> {
        let map = self.inner.read().await;
        map.keys().cloned().collect()
    }

    pub async fn register_worker(
        &self,
        topic: &str,
        worker: Arc<Worker>,
        handle: JoinHandle<()>,
    ) -> Result<(), CoreError> {
        let mut map = self.inner.write().await;
        let mut workers_map = self.workers.write().await;
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
        let (producer, consumer_factory) = {
            let map = self.inner.read().await;
            let entry = map
                .get(topic)
                .ok_or_else(|| TopicError::NotFound(topic.to_string()))?;
            (entry.producer.clone(), entry.consumer_factory.clone())
        };

        let worker_id = {
            let map = self.inner.read().await;
            let entry = map.get(topic).unwrap();
            format!("{}-{}", topic, entry.workers.len())
        };

        let consumer = consumer_factory.create_consumer();

        let worker = Arc::new(Worker::new(
            worker_id,
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

    pub async fn get_worker(&self, worker_name: &str) -> Arc<Worker> {
        let workers = self.workers.read().await;
        let (worker, _) = workers.get(worker_name).unwrap();
        worker.clone()
    }

    pub async fn get_workers(&self, topic: &str) -> Vec<Arc<Worker>> {
        let entries = self.inner.read().await;
        let worker_map = self.workers.read().await;
        let mut workers = Vec::new();
        for worker_index in entries.get(topic).unwrap().workers.clone() {
            workers.push(worker_map.get(&worker_index).unwrap().0.clone());
        }
        workers
    }

    pub async fn get_all_workers(&self) -> Vec<Arc<Worker>> {
        let worker_map = self.workers.read().await;
        let mut workers = Vec::new();
        for worker_index in worker_map.keys() {
            workers.push(worker_map.get(worker_index).unwrap().0.clone());
        }
        workers
    }

    pub async fn del_worker(&self, worker_name: &str) -> Result<(), CoreError> {
        let mut workers = self.workers.write().await;
        if let Some((_worker, handle)) = workers.remove(worker_name) {
            handle.abort();
            let mut map = self.inner.write().await;
            for entry in map.values_mut() {
                entry.workers.retain(|id| id != worker_name);
            }
            Ok(())
        } else {
            Err(CoreError::WorkerNotFound(worker_name.to_string()))
        }
    }

    pub async fn del_workers(&self, topic: &str) -> Result<(), CoreError> {
        let mut entries = self.inner.write().await;
        let mut worker_map = self.workers.write().await;
        if let Some(entry) = entries.get_mut(topic) {
            for worker_index in entry.workers.clone() {
                let (_, handle) = worker_map.get(&worker_index).unwrap();
                handle.abort();
                worker_map.remove(&worker_index);
            }
        }
        entries.remove(topic);
        Ok(())
    }
}
