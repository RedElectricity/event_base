use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};
use crate::error::CoreError;
use crate::handler::EHandler;
use crate::message::{EMessage, MessageTopic};
use crate::message::DeliveryMode::Broadcast;
use crate::queues::{EConsumer, EProducer};
use crate::queues::consumer_factory::ConsumerFactory;
use crate::queues::factory::QueueFactory;
use crate::shutdown::ShutdownReceiver;
use crate::wal::wal::{Wal, WalRecord};
use crate::worker::Worker;

static TOPIC_ROUTER: OnceLock<Arc<TopicRouter>> = OnceLock::new();

pub struct TopicRouter {
    inner: RwLock<HashMap<String, TopicEntry>>,
    factory: Arc<dyn QueueFactory>,
    wal: Option<Arc<tokio::sync::Mutex<dyn Wal>>>,
}

struct TopicEntry {
    pub producer: Arc<dyn EProducer>,
    pub consumer_factory: Arc<dyn ConsumerFactory>,
    pub handler: Arc<dyn EHandler>,
    pub workers: Vec<Arc<Worker>>,
}

impl TopicRouter {
    pub fn init(wal: Option<Arc<tokio::sync::Mutex<dyn Wal>>>,
                factory: Arc<dyn QueueFactory>,) -> Result<(), CoreError> {
        let router = Arc::new(TopicRouter {
            inner: RwLock::new(HashMap::new()),
            factory,
            wal,
        });
        TOPIC_ROUTER
            .set(router)
            .map_err(|_| CoreError::AlreadyInitialized())?;
        Ok(())
    }

    pub fn global() -> Arc<TopicRouter> {
        TOPIC_ROUTER.get().expect("TopicRouter not initialized").clone()
    }

    pub async fn register(
        &self,
        topic: &str,
        handler: Arc<dyn EHandler>,
    ) -> Result<(), CoreError> {
        let (producer, consumer_factory) = self.factory.create_queue(topic)?;
        let mut map = self.inner.write().await;
        if map.contains_key(topic) {
            return Err(CoreError::TopicAlreadyExists(topic.to_string()));
        }
        map.insert(
            topic.to_string(),
            TopicEntry { producer, consumer_factory, handler, workers: vec![] },
        );
        Ok(())
    }

    pub async fn send(&self, topic: &str, mut msg: EMessage) -> Result<(), CoreError> {
        if let Some(wal) = &self.wal {
            let record = WalRecord::from_msg(msg.clone());
            let mut wal = wal.lock().await;
            wal.append(record).await?;
        }

        if msg.deliver_at != None {
            if msg.deliver_at < Option::from(SystemTime::now()) {
                return Err(CoreError::ErrorTime())
            }
            if let Some(wal) = &self.wal {
                let record = WalRecord::from_msg(msg.clone());
                let wal = wal.lock().await;
                wal.schedule(record).await?;
            }
            return Ok(())
        }

        if msg.delivery_mode == Broadcast {
            let mut map = self.inner.write().await;
            if let Some(entry) = map.get_mut(topic) {
                for worker in &entry.workers {
                    let mut copy = msg.clone();
                    copy.id = format!("{}-{}", msg.id, worker.name);
                    worker.producer.send(copy).await?;
                }
            }
            return Ok(());
        }

        msg.topic = MessageTopic(topic.to_string());
        let producer = self.get_producer(topic).await
            .ok_or_else(|| CoreError::TopicNotFound(topic.to_string()))?;
        producer.send(msg).await
    }

    pub async fn run_delay_scheduler(
        wal: Arc<dyn Wal>,
        router: Arc<TopicRouter>,
    ) {
        loop {
            let now = SystemTime::now();
            match wal.fetch_ready(now).await {
                Ok(ready_records) => {
                    for record in ready_records {
                        // 投递前清除 deliver_at（避免再次延迟）
                        let mut msg = record.message;
                        msg.deliver_at = None;
                        if let Err(e) = router.send(&msg.clone().topic.0, msg).await {
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
        map.get(topic).map(|entry| entry.consumer_factory.create_consumer())
    }

    pub async fn get_handler(&self, topic: &str) -> Option<Arc<dyn EHandler>> {
        let map = self.inner.read().await;
        map.get(topic).map(|e| e.handler.clone())
    }

    pub async fn list_topics(&self) -> Vec<String> {
        let map = self.inner.read().await;
        map.keys().cloned().collect()
    }

    pub async fn register_worker(&self, topic: &str, worker: Arc<Worker>) -> Result<(), CoreError> {
        let mut map = self.inner.write().await;
        if let Some(entry) = map.get_mut(topic) {
            entry.workers.push(worker);
            Ok(())
        } else {
            Err(CoreError::TopicNotFound(topic.to_string()))
        }
    }

    pub async fn create_worker(
        &self,
        topic: &str,
        handler: Arc<dyn EHandler>,
        wal: Arc<tokio::sync::Mutex<dyn Wal>>,
        timeout: Option<Duration>,
        shutdown_rx: ShutdownReceiver,
    ) -> Result<Arc<Worker>, CoreError> {
        let (producer, consumer_factory) = {
            let map = self.inner.read().await;
            let entry = map.get(topic).ok_or_else(|| CoreError::TopicNotFound(topic.to_string()))?;
            (entry.producer.clone(), entry.consumer_factory.clone())
        };

        let worker_id = {
            let map = self.inner.read().await;
            let entry = map.get(topic).unwrap();
            format!("{}-{}", topic, entry.workers.len())
        };

        let consumer = consumer_factory.create_consumer();

        let worker = Arc::new(Worker::new(worker_id, consumer, handler, producer.clone(),
                                          timeout, shutdown_rx, wal));

        self.register_worker(topic, worker.clone()).await?;

        Ok(worker)
    }

}

