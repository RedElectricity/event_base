//! Topic routing and message sending.
//!
//! The `TopicRouter` manages message delivery to topics, including broadcast
//! to workers and delayed message scheduling. WAL persistence is handled
//! externally by callers (e.g. via `WalClient` for system topics).

use crate::error::CoreError;
use crate::message::DeliveryMode::Broadcast;
use crate::message::{EMessage, MessageTopic};
use crate::queues::EProducer;
use crate::wal::wal::WalRecord;
use crate::worker_registry::WorkerRegistry;
use crate::{NodeType, get_node_type};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

static TOPIC_ROUTER: OnceLock<Arc<TopicRouter>> = OnceLock::new();

/// Manages topic-based message routing and producer interaction.
///
/// WAL persistence is NOT handled here — callers that need durability must
/// append to the WAL before calling [`send`](TopicRouter::send).
pub struct TopicRouter {
    inner: RwLock<Vec<String>>,
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
    /// Initializes the global topic router with a producer.
    ///
    /// # Errors
    /// Returns `CoreError::AlreadyInitialized` if called more than once.
    pub fn init(producer: Arc<dyn EProducer>) -> Result<(), CoreError> {
        let router = Arc::new(TopicRouter {
            inner: RwLock::new(Vec::new()),
            producer,
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
    /// immediately.  WAL access is obtained via [`WorkerRegistry::global`].
    ///
    /// # Errors
    /// Returns `CoreError` if WAL operations fail.
    pub async fn replay(&self, topics: Option<&[&str]>) -> Result<ReplaySummary, CoreError> {
        let wr = WorkerRegistry::global();
        let wal = wr.wal().ok_or_else(|| CoreError::Unsupported("WAL not available".into()))?;

        let pending = {
            let mut guard = wal.write().await;
            guard.replay_pending().await?
        };

        let mut summary = ReplaySummary::default();
        let topic_filter: Option<Vec<String>> =
            topics.map(|t| t.iter().map(|s| s.to_string()).collect());

        for record in pending {
            let msg = record.message;

            if let Some(ref allowed) = topic_filter {
                if !allowed.contains(&msg.topic.0) {
                    continue;
                }
            }

            if let Some(deliver_at) = msg.deliver_at {
                if deliver_at > SystemTime::now() {
                    let guard = wal.write().await;
                    guard.schedule(WalRecord::from_msg(msg)).await?;
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
    /// This method does NOT write to the WAL — the caller is responsible for
    /// any durability guarantees (e.g., via `WalClient` or manual WAL append).
    /// For broadcast messages, copies are sent to all workers registered for the topic.
    ///
    /// # Errors
    /// Returns `CoreError` if producer send fails or the node type is invalid for broadcast.
    pub async fn send(
        &self,
        topic: &str,
        mut msg: EMessage,
        try_send: Option<bool>,
        timeout: Option<Duration>,
    ) -> Result<(), CoreError> {
        // Delayed messages are scheduled directly via the WAL.
        if msg.deliver_at.is_some() {
            let wr = WorkerRegistry::global();
            let wal = wr.wal().ok_or_else(|| CoreError::Unsupported("WAL not available".into()))?;
            let guard = wal.write().await;
            guard.schedule(WalRecord::from_msg(msg)).await?;
            return Ok(());
        }

        if msg.delivery_mode == Broadcast {
            if get_node_type() == Arc::from(NodeType::Worker) {
                return Err(CoreError::Unsupported(
                    "Unsupported node type, send broadcast message must host".to_string(),
                ));
            }
            let workers = WorkerRegistry::global().get_workers(topic).await?;
            for worker_index in workers {
                let mut copy = msg.clone();
                copy.id = format!("{}-{}", msg.id, worker_index.worker_name);
                copy.to_worker = Some(worker_index.worker_name);
                if try_send.unwrap_or(false) {
                    self.producer.try_send(copy).await?;
                } else if let Some(to) = timeout {
                    self.producer.send_timeout(copy, to).await?;
                } else {
                    self.producer.send(copy).await?;
                }
            }
            return Ok(());
        }

        msg.topic = MessageTopic(topic.to_string());

        if try_send.unwrap_or(false) {
            self.producer.try_send(msg).await?;
        } else if let Some(to) = timeout {
            self.producer.send_timeout(msg, to).await?;
        } else {
            self.producer.send(msg).await?;
        }
        Ok(())
    }

    /// Sends a system message without WAL persistence.
    ///
    /// System topics (WAL sync, audit, metrics, etc.) carry metadata that is
    /// already part of the WAL state — writing them again would be redundant.
    /// This method skips the WAL append entirely and only pushes the message
    /// to the underlying producer.
    pub async fn send_system(
        &self,
        msg: EMessage,
        try_send: Option<bool>,
        timeout: Option<Duration>,
    ) -> Result<(), CoreError> {
        if try_send.unwrap_or(false) {
            self.producer.try_send(msg).await?;
        } else if let Some(to) = timeout {
            self.producer.send_timeout(msg, to).await?;
        } else {
            self.producer.send(msg).await?;
        }
        Ok(())
    }

    /// Background task that periodically checks for ready delayed messages and sends them.
    ///
    /// WAL access is obtained via [`WorkerRegistry::global`].
    pub async fn run_delay_scheduler() {
        let router = TopicRouter::global();
        loop {
            let ready_records = {
                let wr = WorkerRegistry::global();
                let wal = match wr.wal() {
                    Some(w) => w,
                    None => {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                };
                let guard = wal.read().await;
                guard.fetch_ready().await.unwrap_or_default()
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
