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
use tokio::sync::Mutex;
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
}

impl MemoryQueueFactory {
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = mpmc::bounded_async::<EMessage>(capacity);
        Self { tx, rx }
    }
}

#[async_trait]
impl QueueFactory for MemoryQueueFactory {
    fn create_queue(
        &self,
        _topic: &str,
    ) -> Result<(Arc<dyn EProducer>, Arc<dyn ConsumerFactory>), CoreError> {
        let producer = Arc::new(CrossfireProducer {
            tx: self.tx.clone(),
        });
        let consumer_factory =
            Arc::new(MemoryConsumerFactory::new(self.tx.clone(), self.rx.clone()));
        Ok((producer, consumer_factory))
    }

    fn create_global_producer(&self) -> Result<Arc<dyn EProducer>, CoreError> {
        Ok(Arc::new(CrossfireProducer {
            tx: self.tx.clone(),
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
