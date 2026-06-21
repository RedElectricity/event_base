use async_trait::async_trait;
use event_base_core::error::queue::QueueError;
use event_base_core::error::CoreError;
use event_base_core::message::EMessage;
use event_base_core::queues::consumer_factory::ConsumerFactory;
use event_base_core::queues::factory::QueueFactory;
use event_base_core::queues::{EConsumer, EProducer};
use flume::{bounded, unbounded, Receiver, Sender};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

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
        if let Err(e) = self.tx.send(e) {
            return Err(CoreError::from(QueueError::Send(e.to_string())));
        }
        Ok(())
    }

    fn try_send(&self, msg: EMessage) -> Result<(), CoreError> {
        if let Err(_e) = self.tx.try_send(msg) {
            return Err(CoreError::from(QueueError::Full));
        }
        Ok(())
    }

    async fn send_timeout(&self, msg: EMessage, timeout: Duration) -> Result<(), CoreError> {
        let _ = tokio::time::timeout(timeout, self.send(msg))
            .await
            .map_err(|_| QueueError::Timeout);
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
