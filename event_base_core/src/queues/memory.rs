use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use async_trait::async_trait;
use tokio::sync::mpsc;
use flume::{bounded, unbounded, Receiver, Sender};
use crate::error::CoreError;
use crate::message::EMessage;
use crate::queues::{EConsumer, EProducer};
use crate::queues::consumer_factory::ConsumerFactory;
use crate::queues::factory::QueueFactory;

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
    (MemoryProducer { tx, len: Arc::new(Default::default()) }, MemoryConsumer { rx, len: Arc::new(Default::default()) })

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
        let msg = self.rx.recv();
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
        Box::new(MemoryConsumer { rx: self.rx.clone(), len: Arc::new(Default::default()) })
    }

    fn clone_factory(&self) -> Arc<dyn ConsumerFactory> {
        Arc::new(MemoryConsumerFactory { rx: self.rx.clone() })
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
        let producer = Arc::new(MemoryProducer { tx, len: Arc::new(Default::default()) });
        let consumer_factory = Arc::new(MemoryConsumerFactory::new(rx));
        Ok((producer, consumer_factory))
    }

    fn name(&self) -> &'static str {
        "memory"
    }
}