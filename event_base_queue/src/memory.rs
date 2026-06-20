use async_trait::async_trait;
use event_base_core::error::CoreError;
use event_base_core::message::EMessage;
use event_base_core::queues::consumer_factory::ConsumerFactory;
use event_base_core::queues::factory::QueueFactory;
use event_base_core::queues::{EConsumer, EProducer};
use event_base_core::worker_registry::{WorkerInfo, WorkerRegistry};
use flume::{Receiver, Sender, bounded, unbounded};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct MemoryProducer {
    tx: Sender<EMessage>,
    len: Arc<AtomicUsize>,
}

pub struct MemoryConsumer {
    rx: Receiver<EMessage>,
    len: Arc<AtomicUsize>,
}

// Left for test use
pub fn memory_queue(capacity: usize) -> (MemoryProducer, MemoryConsumer) {
    let (tx, rx) = if capacity > 0 {
        bounded(capacity)
    } else {
        unbounded()
    };
    (
        MemoryProducer {
            tx,
            len: Arc::new(Default::default()),
        },
        MemoryConsumer {
            rx,
            len: Arc::new(Default::default()),
        },
    )
}

#[async_trait]
impl EProducer for MemoryProducer {
    async fn send(&self, e: EMessage) -> Result<(), CoreError> {
        self.len.fetch_add(1, Ordering::Relaxed);
        if let Err(e) = self.tx.send(e) {
            self.len.fetch_sub(1, Ordering::Release);
            return Err(CoreError::QueueSendError(e.to_string()));
        }
        Ok(())
    }
}

#[async_trait]
impl EConsumer for MemoryConsumer {
    async fn receive(&mut self) -> Option<EMessage> {
        let msg = self.rx.recv_async().await;
        if msg.is_ok() {
            self.len.fetch_sub(1, Ordering::Acquire);
        }
        Option::from(msg.unwrap())
    }

    fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }
}

// ---------- Consumer Factory ----------
pub struct MemoryConsumerFactory {
    rx: Receiver<EMessage>,
}

impl MemoryConsumerFactory {
    pub fn new(rx: Receiver<EMessage>) -> Self {
        Self { rx }
    }
}

#[async_trait]
impl ConsumerFactory for MemoryConsumerFactory {
    fn create_consumer(&self) -> Box<dyn EConsumer> {
        Box::new(MemoryConsumer {
            rx: self.rx.clone(),
            len: Arc::new(Default::default()),
        })
    }

    fn clone_factory(&self) -> Arc<dyn ConsumerFactory> {
        Arc::new(MemoryConsumerFactory {
            rx: self.rx.clone(),
        })
    }
}

// ---------- Queue Factory ----------
pub struct MemoryQueueFactory {
    capacity: usize,
}

impl MemoryQueueFactory {
    pub fn new(capacity: usize) -> Self {
        Self { capacity }
    }
}

#[async_trait]
impl QueueFactory for MemoryQueueFactory {
    fn create_queue(
        &self,
        _topic: &str,
    ) -> Result<(Arc<dyn EProducer>, Arc<dyn ConsumerFactory>), CoreError> {
        let (tx, rx) = if self.capacity > 0 {
            bounded(self.capacity)
        } else {
            unbounded()
        };
        let producer = Arc::new(MemoryProducer {
            tx,
            len: Arc::new(Default::default()),
        });
        let consumer_factory = Arc::new(MemoryConsumerFactory::new(rx));
        Ok((producer, consumer_factory))
    }

    fn name(&self) -> &'static str {
        "memory"
    }
}
