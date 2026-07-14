use async_trait::async_trait;
use event_base_core::error::CoreError;
use event_base_core::error::queue::QueueError;
use event_base_core::message::EMessage;
use event_base_core::queues::consumer_factory::ConsumerFactory;
use event_base_core::queues::factory::QueueFactory;
use event_base_core::queues::{ClaimedMessage, EConsumer, EProducer};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use crossfire::{mpmc, MAsyncRx, MAsyncTx};
use crossfire::mpmc::Array;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

#[derive(Clone)]
pub struct CrossfireProducer {
    tx: MAsyncTx<Array<EMessage>>,
}

pub struct CrossfireConsumer {
    tx: MAsyncTx<Array<EMessage>>, // for NoAck msg
    rx: MAsyncRx<Array<EMessage>>,
    pending: Arc<Mutex<HashMap<String, EMessage>>>,
}

// Left for test use
pub fn memory_queue(capacity: usize) -> (CrossfireProducer, CrossfireConsumer) {
    let (tx, rx) = mpmc::bounded_async::<EMessage>(capacity);
    (
        CrossfireProducer { tx: tx.clone() },
        CrossfireConsumer {
            tx,
            rx,
            pending: Arc::new(Mutex::new(HashMap::default())),
        },
    )
}

#[async_trait]
impl EProducer for CrossfireProducer {
    async fn send(&self, e: EMessage) -> Result<(), CoreError> {
        if let Err(e) = self.tx.send(e).await {
            return Err(CoreError::from(QueueError::Send(e.to_string())));
        }
        Ok(())
    }

    async fn try_send(&self, msg: EMessage) -> Result<(), CoreError> {
        if self.tx.is_full() {
            return Err(QueueError::Full.into());
        }
        if let Err(e) = self.tx.send(msg).await {
            return Err(CoreError::from(QueueError::Send(e.to_string())));
        }
        Ok(())
    }

    async fn send_timeout(&self, msg: EMessage, timeout: Duration) -> Result<(), CoreError> {
        tokio::time::timeout(timeout, 
                             self.send(msg))
            .await.unwrap_or_else(|_elapsed| Err(CoreError::from(QueueError::Timeout)))
    }
}

// ── RoutingProducer：根据 msg.topic 分发到 per-topic channel ──────────
//
// TopicRouter::send() → global_producer (RoutingProducer)
//   → 查 routing table → 有 per-topic tx → 直送 per-topic channel → worker
//                      → 无 → 走 default_tx（系统消息保底）
//
// 每个 topic 独立 channel，下游阻塞不会传染到其他 topic。

#[derive(Clone)]
pub struct RoutingProducer {
    default_tx: MAsyncTx<Array<EMessage>>,
    topic_producers: Arc<RwLock<HashMap<String, MAsyncTx<Array<EMessage>>>>>,
}

#[async_trait]
impl EProducer for RoutingProducer {
    async fn send(&self, msg: EMessage) -> Result<(), CoreError> {
        let topic = &msg.topic.0;
        let (found, tx) = {
            let routes = self.topic_producers.read().await;
            match routes.get(topic) {
                Some(t) => (true, t.clone()),
                None => (false, self.default_tx.clone()),
            }
        };
        if !found && topic.starts_with("_system.") {
            return Ok(());
        }
        if !found {
            return Err(CoreError::from(QueueError::Send(format!(
                "topic '{}' is not registered",
                topic
            ))));
        }
        tx.send(msg).await.map_err(|e| CoreError::from(QueueError::Send(e.to_string())))
    }

    async fn try_send(&self, msg: EMessage) -> Result<(), CoreError> {
        let topic = &msg.topic.0;
        let tx = {
            let routes = self.topic_producers.read().await;
            match routes.get(topic) {
                Some(t) => {
                    let tx = t.clone();
                    if tx.is_full() {
                        return Err(QueueError::Full.into());
                    }
                    tx
                }
                None => {
                    if topic.starts_with("_system.") {
                        return Ok(());
                    }
                    return Err(CoreError::from(QueueError::Send(format!(
                        "topic '{}' is not registered",
                        topic
                    ))));
                }
            }
        };
        tx.send(msg).await.map_err(|e| CoreError::from(QueueError::Send(e.to_string())))
    }

    async fn send_timeout(&self, msg: EMessage, timeout: Duration) -> Result<(), CoreError> {
        tokio::time::timeout(timeout, self.send(msg))
            .await.unwrap_or_else(|_| Err(CoreError::from(QueueError::Timeout)))
    }
}

#[async_trait]
impl EConsumer for CrossfireConsumer {
    async fn receive(&mut self) -> Option<EMessage> {
        let msg = self.rx.recv().await;
        if let Ok(msg) = msg {
            return Option::from(msg);
        }
        None
    }

    async fn claim(&mut self) -> Result<Option<ClaimedMessage>, CoreError> {
        let msg = match self.rx.recv().await {
            Ok(m) => m,
            Err(_) => return Ok(None),
        };

        let claim_id = Uuid::new_v4();
        let mut pending = self.pending.lock().await;
        pending.insert(claim_id.clone().to_string(), msg.clone());
        Ok(Some(ClaimedMessage {
            message: msg,
            claim_id: claim_id.to_string(),
            claimed_at: SystemTime::now(),
        }))
    }

    async fn claim_batch(&mut self, max: usize) -> Result<Vec<ClaimedMessage>, CoreError> {
        if max == 0 {
            return Ok(Vec::new());
        }
        let mut batch = Vec::with_capacity(max);
        let mut pending = self.pending.lock().await;
        for _ in 0..max {
            match self.rx.try_recv() {
                Ok(msg) => {
                    let claim_id = Uuid::new_v4().to_string();
                    pending.insert(claim_id.clone(), msg.clone());
                    batch.push(ClaimedMessage {
                        message: msg,
                        claim_id,
                        claimed_at: SystemTime::now(),
                    });
                }
                Err(_) => break,
            }
        }
        Ok(batch)
    }

    async fn ack(&mut self, claim_id: &str) -> Result<(), CoreError> {
        let mut pending = self.pending.lock().await;
        pending.remove(claim_id);
        Ok(())
    }

    async fn nack(&mut self, claim_id: &str) -> Result<(), CoreError> {
        let mut pending = self.pending.lock().await;
        if let Some(msg) = pending.get(claim_id) {
            if let Err(e) = self.tx.send(msg.clone()).await {
                return Err(CoreError::from(QueueError::Send(e.to_string())));
            }
            pending.remove(claim_id);
            return Ok(());
        }
        Err(QueueError::InvalidClaimId(claim_id.to_string()).into())
    }
}

pub struct MemoryConsumerFactory {
    tx: MAsyncTx<Array<EMessage>>,
    rx: MAsyncRx<Array<EMessage>>,
}

impl MemoryConsumerFactory {
    pub fn new(tx: MAsyncTx<Array<EMessage>>, rx: MAsyncRx<Array<EMessage>>) -> Self {
        Self { tx, rx }
    }
}

#[async_trait]
impl ConsumerFactory for MemoryConsumerFactory {
    fn create_consumer(&self) -> Box<dyn EConsumer> {
        Box::new(CrossfireConsumer {
            tx: self.tx.clone(),
            rx: self.rx.clone(),
            pending: Arc::new(Mutex::new(HashMap::default())),
        })
    }

    fn clone_factory(&self) -> Arc<dyn ConsumerFactory> {
        Arc::new(MemoryConsumerFactory {
            tx: self.tx.clone(),
            rx: self.rx.clone(),
        })
    }
}

// ---------- Queue Factory ----------
pub struct MemoryQueueFactory {
    tx: MAsyncTx<Array<EMessage>>,
    rx: MAsyncRx<Array<EMessage>>,
    capacity: usize,
    /// topic → per-topic producer tx，由 create_queue 注册，RoutingProducer 据此分流
    topic_producers: Arc<RwLock<HashMap<String, MAsyncTx<Array<EMessage>>>>>,
}

impl MemoryQueueFactory {
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = mpmc::bounded_async::<EMessage>(capacity);
        Self {
            tx,
            rx,
            capacity,
            topic_producers: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl QueueFactory for MemoryQueueFactory {
    fn create_queue(
        &self,
        topic: &str,
    ) -> Result<(Arc<dyn EProducer>, Arc<dyn ConsumerFactory>), CoreError> {
        // 每个 topic 独立 channel，避免所有 topic 共享同一条队列
        let (tx, rx) = mpmc::bounded_async::<EMessage>(self.capacity);

        // 注册到 routing table，让 RoutingProducer 能直送达 per-topic channel
        self.topic_producers.try_write().expect("create_queue: routing table lock contention")
            .insert(topic.to_string(), tx.clone());

        let producer = Arc::new(CrossfireProducer {
            tx: tx.clone(),
        });
        let consumer_factory =
            Arc::new(MemoryConsumerFactory::new(tx, rx));
        Ok((producer, consumer_factory))
    }

    fn create_global_producer(&self) -> Result<Arc<dyn EProducer>, CoreError> {
        Ok(Arc::new(RoutingProducer {
            default_tx: self.tx.clone(),
            topic_producers: self.topic_producers.clone(),
        }))
    }

    fn create_main_consumer(&self) -> Result<Arc<Mutex<dyn EConsumer>>, CoreError> {
        Ok(Arc::new(Mutex::new(CrossfireConsumer {
            tx: self.tx.clone(),
            rx: self.rx.clone(),
            pending: Arc::new(Default::default()),
        })))
    }

    fn name(&self) -> &'static str {
        "memory"
    }

    async fn health_check(&self) -> Result<(), CoreError> {
        Ok(())
    }
}
