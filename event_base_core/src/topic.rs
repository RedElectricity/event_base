//! Topic routing and message sending with WAL persistence.
//!
//! The `TopicRouter` manages message delivery to topics, handles delayed messages,
//! broadcast to workers, and recovery from the write-ahead log.

use crate::error::CoreError;
use crate::message::DeliveryMode::Broadcast;
use crate::message::{EMessage, MessageTopic};
use crate::queues::EProducer;
use crate::wal::wal::{Wal, WalRecord};
use crate::worker_registry::WorkerRegistry;
use crate::{NodeType, get_node_type};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

static TOPIC_ROUTER: OnceLock<Arc<TopicRouter>> = OnceLock::new();

/// Manages topic-based message routing, WAL, and producer interaction.
pub struct TopicRouter {
    inner: RwLock<Vec<String>>,
    wal: Arc<RwLock<Box<dyn Wal>>>,
    producer: Arc<dyn EProducer>,
}

/// Summary of a replay operation.
#[derive(Debug, Default)]
pub struct ReplaySummary {
    /// Number of messages successfully recovered.
    pub recovered: usize,
    /// Number of messages that were delayed (future delivery).
    pub delayed: usize,
    /// List of errors encountered per message ID.
    pub errors: Vec<(String, CoreError)>,
}

impl TopicRouter {
    /// Initializes the global topic router with a WAL and a producer.
    ///
    /// # Errors
    /// Returns `CoreError::AlreadyInitialized` if called more than once.
    pub fn init(
        wal: Arc<RwLock<Box<dyn Wal>>>,
        producer: Arc<dyn EProducer>,
    ) -> Result<(), CoreError> {
        let router = Arc::new(TopicRouter {
            inner: RwLock::new(Vec::new()),
            producer,
            wal,
        });
        TOPIC_ROUTER
            .set(router)
            .map_err(|_| CoreError::AlreadyInitialized)?;
        Ok(())
    }

    /// Returns a reference to the global topic router.
    ///
    /// # Panics
    /// Panics if the router has not been initialized.
    pub fn global() -> Arc<TopicRouter> {
        TOPIC_ROUTER
            .get()
            .expect("TopicRouter not initialized")
            .clone()
    }

    /// Replays pending messages from the WAL, optionally filtering by topics.
    ///
    /// Messages with a future `deliver_at` are re-scheduled; others are sent
    /// immediately.
    ///
    /// # Errors
    /// Returns `CoreError` if WAL operations fail.
    pub async fn replay(&self, topics: Option<&[&str]>) -> Result<ReplaySummary, CoreError> {
        let pending = {
            let wal = &mut self.wal.write().await;
            wal.replay_pending().await?
        };

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
                    let wal = &mut self.wal.write().await;
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

    /// Sends a message to the given topic, with optional try-send or timeout.
    ///
    /// The message is first appended to the WAL. If `deliver_at` is set, it is scheduled.
    /// For broadcast messages, it is sent to all workers registered for the topic.
    ///
    /// # Errors
    /// Returns `CoreError` if WAL append fails, producer send fails, or the node type
    /// is invalid for broadcast.
    pub async fn send(
        &self,
        topic: &str,
        mut msg: EMessage,
        try_send: Option<bool>,
        timeout: Option<Duration>,
    ) -> Result<(), CoreError> {
        let record = WalRecord::from_msg(msg.clone());
        let mut wal = self.wal.write().await;
        wal.append(record).await?;

        if msg.deliver_at.is_some() {
            if msg.deliver_at < Option::from(SystemTime::now()) {
                return Err(CoreError::ErrorTime);
            }
            let record = WalRecord::from_msg(msg.clone());
            wal.schedule(record).await?;
            return Ok(());
        }

        drop(wal);

        if msg.delivery_mode == Broadcast {
            if get_node_type() == Arc::from(NodeType::Worker) {
                return Err(CoreError::Unsupported(
                    "Unsupported node type, send broadcast message must host".to_string(),
                ));
            }
            let workers = WorkerRegistry::global().get_workers(topic).await?;
            for worker_index in workers {
                let mut copy = msg.clone();
                copy.id = format!("{}-{}", msg.id, worker_index.worker_name.clone());
                copy.to_worker = Option::from(worker_index.worker_name.clone());
                if try_send.unwrap_or(false) {
                    self.producer.try_send(copy.clone()).await?;
                } else if let Some(to) = timeout {
                    self.producer.send_timeout(copy.clone(), to).await?;
                } else {
                    self.producer.send(copy).await?;
                }
            }

            return Ok(());
        }

        msg.topic = MessageTopic(topic.to_string());

        if try_send.unwrap_or(false) {
            self.producer.try_send(msg.clone()).await?;
        } else if let Some(to) = timeout {
            self.producer.send_timeout(msg.clone(), to).await?;
        } else {
            self.producer.send(msg).await?;
        }
        Ok(())
    }

    /// Background task that periodically checks for ready delayed messages and sends them.
    pub async fn run_delay_scheduler() {
        let router = TopicRouter::global();
        loop {
            let ready_records = {
                let wal = router.wal.read().await;
                wal.fetch_ready().await.unwrap_or_default()
            };

            for record in ready_records {
                let mut msg = record.message;
                msg.deliver_at = None;
                if let Err(e) = router.send(&msg.clone().topic.0, msg, None, None).await {
                    tracing::error!("Failed to deliver delayed message: {}", e);
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Returns the list of registered topics.
    pub async fn list_topics(&self) -> Vec<String> {
        let list = self.inner.read().await;
        list.clone()
    }

    /// Registers a topic (idempotent).
    pub async fn register_topic(&self, topic: &str) {
        let mut topics = self.inner.write().await;
        if !topics.contains(&topic.to_string()) {
            topics.push(topic.to_string());
        }
    }

    /// Returns the underlying producer.
    pub fn get_producer(&self) -> Arc<dyn EProducer> {
        self.producer.clone()
    }
}
