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

static TOPIC_ROUTER: OnceLock<RwLock<TopicRouter>> = OnceLock::new();

/// Manages topic-based message routing and producer interaction.
///
/// WAL persistence is NOT handled here — callers that need durability must
/// append to the WAL before calling [`send`](TopicRouter::send).
pub struct TopicRouter {
    inner: RwLock<Vec<String>>,
    producer: Arc<dyn EProducer>,
    /// Fallback send timeout applied by [`send`](Self::send) when the caller
    /// passes `timeout = None` and is not doing a `try_send`. `None` (the
    //  default) preserves the historical block‑until‑space behaviour; a fleet
    //  that wants backpressure to surface as an error rather than a stalled
    //  await sets this once at boot.
    default_send_timeout: RwLock<Option<Duration>>,
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
        let router = TopicRouter {
            inner: RwLock::new(Vec::new()),
            producer,
            default_send_timeout: RwLock::new(None),
        };
        TOPIC_ROUTER
            .set(RwLock::new(router))
            .map_err(|_| CoreError::AlreadyInitialized)?;
        Ok(())
    }

    /// Returns a reference to the global topic router.
    ///
    /// # Panics
    /// Panics if the router has not been initialized.
    pub fn global() -> &'static RwLock<TopicRouter> {
        TOPIC_ROUTER.get().expect("TopicRouter not initialized")
    }

    /// Returns the global topic router if it has been initialized, else `None`.
    ///
    /// Used by infrastructure-level senders (worker discovery announcements)
    /// that must degrade gracefully in unit tests which construct routers or
    /// workers without booting the full system.
    pub fn try_global() -> Option<&'static RwLock<TopicRouter>> {
        TOPIC_ROUTER.get()
    }

    /// Replays pending messages from the WAL, optionally filtering by topics.
    ///
    /// Messages with a future `deliver_at` are re-scheduled; others are sent
    /// immediately.  WAL access is obtained via [`WorkerRegistry::global`].
    ///
    /// # Errors
    /// Returns `CoreError` if WAL operations fail.
    pub async fn replay(&self, topics: Option<&[&str]>) -> Result<ReplaySummary, CoreError> {
        let wr = WorkerRegistry::global().read().await;
        let wal = wr
            .wal()
            .ok_or_else(|| CoreError::Unsupported("WAL not available".into()))?;

        let pending = {
            let mut guard = wal.write().await;
            guard.replay_pending().await?
        };

        let mut summary = ReplaySummary::default();
        let topic_filter: Option<Vec<String>> =
            topics.map(|t| t.iter().map(|s| s.to_string()).collect());

        for record in pending {
            let msg = record.message;

            if let Some(ref allowed) = topic_filter
                && !allowed.contains(&msg.topic.0)
            {
                continue;
            }

            if let Some(deliver_at) = msg.deliver_at
                && deliver_at > SystemTime::now()
            {
                let guard = wal.write().await;
                guard.schedule(WalRecord::from_msg(msg)).await?;
                summary.delayed += 1;
                continue;
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
        // Note: past `deliver_at` is tolerated — the WAL scheduler will deliver
        // them immediately on the next tick.
        if let Some(_deliver_at) = msg.deliver_at {
            let wr = WorkerRegistry::global().read().await;
            let wal = wr
                .wal()
                .ok_or_else(|| CoreError::Unsupported("WAL not available".into()))?;
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
            let workers = WorkerRegistry::global()
                .read()
                .await
                .get_workers(topic)
                .await?;
            for worker_index in workers {
                let mut copy = msg.clone();
                copy.id = format!("{}-{}", msg.id, worker_index.worker_name);
                copy.to_worker = Some(worker_index.worker_name);
                self.emit(copy, try_send, timeout).await?;
            }
            return Ok(());
        }

        msg.topic = MessageTopic(topic.to_string());

        self.emit(msg, try_send, timeout).await
    }

    /// Pushes one message to the producer, honouring `try_send`/`timeout`, and
    /// falling back to the router‑wide [`set_default_send_timeout`] when the
    /// caller passed neither. Centralizes the three former copy‑paste send
    /// strategies so backpressure policy lives in one place.
    async fn emit(
        &self,
        msg: EMessage,
        try_send: Option<bool>,
        timeout: Option<Duration>,
    ) -> Result<(), CoreError> {
        if try_send.unwrap_or(false) {
            self.producer.try_send(msg).await
        } else if let Some(to) = timeout.or(*self.default_send_timeout.read().await) {
            self.producer.send_timeout(msg, to).await
        } else {
            self.producer.send(msg).await
        }
    }

    /// Sets a router‑wide fallback send timeout. When a [`send`](Self::send) /
    /// [`send_system`](Self::send_system) call supplies neither `try_send=true`
    /// nor an explicit `timeout`, this bound is applied instead of blocking
    /// forever on a full queue. `None` (default) keeps the blocking behaviour.
    pub async fn set_default_send_timeout(&self, timeout: Option<Duration>) {
        *self.default_send_timeout.write().await = timeout;
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
        self.emit(msg, try_send, timeout).await
    }

    /// Background task that periodically checks for ready delayed messages and sends them.
    ///
    /// WAL access is obtained via [`WorkerRegistry::global`].
    pub async fn run_delay_scheduler() {
        let router = TopicRouter::global().read().await;
        loop {
            let ready_records = {
                let wr = WorkerRegistry::global().read().await;
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
