//! Worker implementation that consumes messages, applies a middleware pipeline,
//! and handles acknowledgment, retries, and dead-lettering.

use crate::audit::{AuditEventType, AuditRecord, AuditResult};
use crate::constant::{SYSTEM_TOPIC_AUDIT, SYSTEM_TOPIC_SHUTDOWN_ACK};
use crate::dead_letter::DeadReason;
use crate::error::CoreError;
use crate::handler::Ack::{Ack, Dead, NoAck};
use crate::message::DeliveryMode::{Repeated, Standard};
use crate::message::{EMessage, MessagePayload, MessageTopic};
use crate::middleware::Pipeline;
use crate::queues::consumer_router::ConsumerRouter;
use crate::queues::{EConsumer, EProducer};
use crate::shutdown::messages::{ShutdownAck, ShutdownStatus};
use crate::topic::TopicRouter;
use crate::wal::sync::WalClient;
use crate::worker::WorkerStatus::{Idle, Working};
use std::option::Option;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::time::timeout;
use tracing::{error, warn};
use uuid::Uuid;

/// A worker that consumes messages from a topic, processes them via a pipeline,
/// and handles retries/dead-lettering based on the returned `Ack`.
pub struct Worker {
    pub topic: String,
    pub name: String,
    pub consumer: Arc<Mutex<Box<dyn EConsumer>>>,
    pub pipeline: Arc<Pipeline>,
    pub producer: Arc<dyn EProducer>,
    pub time_out: Option<Duration>,
    shutdown_notify: Arc<Notify>,
    pub shutdown_check_interval: Duration,
    pub shutdown_timeout: Option<Duration>,
    pub status: Arc<Mutex<WorkerStatus>>,
    wal: WalClient,
    shutdown_complete: Arc<AtomicBool>,
    /// Cached audit message template — avoids re‑creating topic / delivery fields.
    audit_template: EMessage,
}

/// Possible statuses of a worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStatus {
    /// Not currently processing a message.
    Idle,
    /// Actively processing a message.
    Working,
}

impl Worker {
    /// Creates a new worker instance.
    ///
    /// The worker name is generated automatically from the topic and a UUID.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        topic: String,
        consumer: Box<dyn EConsumer>,
        pipeline: Arc<Pipeline>,
        producer: Arc<dyn EProducer>,
        time_out: Option<Duration>,
        shutdown_check_interval: Duration,
        shutdown_timeout: Option<Duration>,
    ) -> Self {
        let name = format!("worker-{}-{}", topic, Uuid::new_v4());
        Self {
            topic,
            name: name.clone(),
            consumer: Arc::new(Mutex::new(consumer)),
            pipeline,
            producer,
            time_out,
            shutdown_check_interval,
            shutdown_timeout,
            shutdown_notify: Arc::new(Notify::new()),
            status: Arc::new(Mutex::new(Idle)),
            wal: WalClient::new(name),
            shutdown_complete: Arc::new(AtomicBool::new(false)),
            audit_template: EMessage::new(
                MessageTopic(SYSTEM_TOPIC_AUDIT.to_string()),
                MessagePayload(Vec::new()),
                Standard,
                None,
            ),
        }
    }

    /// Starts the worker's main loop.
    ///
    /// It repeatedly receives messages, processes them, and handles the returned `Ack`.
    /// It also monitors the shutdown notify to perform graceful shutdown.
    pub async fn start(&self) {
        let mut consumer = self.consumer.lock().await;

        loop {
            tokio::select! {
                _ = self.shutdown_notify.notified() => {
                    let _ = self.shutdown(self.shutdown_check_interval, self.shutdown_timeout).await;
                    break;
                }
                msg = consumer.receive() => {
                    self.set_status(Working).await;
                    if let Some(msg) = msg {
                        let _ = self.process_msg(msg).await;
                    }
                    self.set_status(Idle).await;
                }
            }
        }
        self.shutdown_complete.store(true, Ordering::SeqCst);
    }

    async fn process_msg(&self, mut msg: EMessage) -> Result<(), CoreError> {
        let is_system = self.topic.starts_with("_system.");

        if !is_system && self.topic != SYSTEM_TOPIC_AUDIT {
            if let Err(e) = self
                .send_audit_msg(self.generate_audit_msg(
                    msg.clone(),
                    AuditResult::Start,
                    AuditEventType::ProcessingStarted,
                    None,
                    None,
                ))
                .await
            {
                error!("[AUDIT_ERROR] Failed to send audit msg: {}", e);
            }
        }

        let start_time = SystemTime::now();

        if !is_system {
            self.wal
                .mark_processing(msg.clone().id.as_str(), msg.clone().topic.0.as_str())
                .await?;
        }
        let status;

        if let Some(time) = self.time_out {
            match timeout(time, self.pipeline.run(&mut msg)).await {
                Ok(result) => {
                    status = result;
                }
                Err(_) => {
                    status = Dead {
                        dead_reason: DeadReason::Timeout,
                    };
                }
            }
        } else {
            status = self.pipeline.run(&mut msg).await;
        }

        // 系统消息：pipeline 跑完直接返回，不写 WAL/audit
        if is_system {
            return Ok(());
        }

        let finish_time = SystemTime::now();

        let process_time = finish_time.duration_since(start_time)?;

        match status {
            Ack => {
                if let Repeated(times) = msg.delivery_mode {
                    msg.consumed_count += 1;
                    if times == msg.consumed_count {
                        self.wal
                            .mark_complete(
                                msg.clone().id.as_str(),
                                msg.clone().topic.0.as_str(),
                                msg.clone().attempts,
                            )
                            .await?;
                        return Ok(());
                    }
                    self.wal
                        .mark_pending(msg.clone().id.as_str(), msg.clone().topic.0.as_str())
                        .await?;
                    self.requeue_message(msg).await?;
                    return Ok(());
                }
                if self.topic != SYSTEM_TOPIC_AUDIT {
                    if let Err(e) = self
                        .send_audit_msg(self.generate_audit_msg(
                            msg.clone(),
                            AuditResult::Success,
                            AuditEventType::ProcessingCompleted,
                            None,
                            Option::from(process_time),
                        ))
                        .await
                    {
                        eprintln!("[AUDIT_ERROR] Failed to send audit msg: {}", e);
                    }
                }
                self.wal
                    .mark_complete(
                        msg.clone().id.as_str(),
                        msg.clone().topic.0.as_str(),
                        msg.clone().attempts,
                    )
                    .await?;
                Ok(())
            }

            NoAck {
                retry_after,
                max_retries,
            } => {
                msg.attempts += 1;
                self.wal
                    .mark_pending(msg.clone().id.as_str(), msg.clone().topic.0.as_str())
                    .await?;

                if msg.attempts >= max_retries {
                    self.send_to_dead_letter(
                        msg.clone(),
                        DeadReason::MaxRetriesExceeded,
                        process_time,
                    )
                    .await?;
                    Ok(())
                } else {
                    if let Some(delay) = retry_after {
                        msg.deliver_at = SystemTime::now().checked_add(delay);
                        TopicRouter::global()
                            .read().await
                            .send(&msg.clone().topic.0, msg.clone(), None, None)
                            .await?;
                        Ok(())
                    } else {
                        self.producer.send(msg.clone()).await?;
                        Ok(())
                    }
                }
            }
            Dead { dead_reason } => {
                self.wal
                    .mark_dead_letter(
                        msg.clone().id.as_str(),
                        msg.clone().topic.0.as_str(),
                        msg.clone().attempts,
                        dead_reason.to_string(),
                    )
                    .await?;
                self.send_to_dead_letter(msg.clone(), DeadReason::Explicit, process_time)
                    .await?;
                Ok(())
            }
        }
    }

    async fn requeue_message(&self, msg: EMessage) -> Result<(), CoreError> {
        self.wal
            .mark_pending(msg.clone().id.as_str(), msg.clone().topic.0.as_str())
            .await?;
        self.producer.send(msg).await?;
        Ok(())
    }

    fn generate_audit_msg(
        &self,
        msg: EMessage,
        result: AuditResult,
        audit_event_type: AuditEventType,
        error: Option<String>,
        duration: Option<Duration>,
    ) -> AuditRecord {
        AuditRecord {
            message_id: msg.id.clone(),
            topic: msg.topic.0.clone(),
            event_type: audit_event_type,
            worker_id: Some(self.name.clone()),
            timestamp: SystemTime::now(),
            result,
            error,
            duration,
        }
    }

    async fn send_audit_msg(&self, record: AuditRecord) -> Result<(), CoreError> {
        let payload = bincode::encode_to_vec(&record, bincode::config::standard())
            .map_err(|e| CoreError::Serialize(crate::error::serialize::SerializeError::SerializeError(e.to_string())))?;
        let mut msg = self.audit_template.clone();
        msg.payload = MessagePayload(payload);
        msg.id = Uuid::new_v4().to_string();
        TopicRouter::global().read().await.send_system(msg, None, None).await?;
        Ok(())
    }

    async fn send_to_dead_letter(
        &self,
        mut msg: EMessage,
        reason: DeadReason,
        process_time: Duration,
    ) -> Result<(), CoreError> {
        let dead_letter_topic_name = format!("dead_letter.{}", self.topic);
        msg.topic = MessageTopic(dead_letter_topic_name.clone());

        self.wal
            .mark_dead_letter(
                msg.clone().id.as_str(),
                msg.clone().topic.0.as_str(),
                msg.clone().attempts,
                reason.to_string(),
            )
            .await?;

        TopicRouter::global()
            .read().await
            .send(&dead_letter_topic_name, msg.clone(), None, None)
            .await?;

        if self.topic != SYSTEM_TOPIC_AUDIT {
            if let Err(e) = self
                .send_audit_msg(self.generate_audit_msg(
                    msg.clone(),
                    AuditResult::Dead,
                    AuditEventType::DeadLettered,
                    None,
                    Option::from(process_time),
                ))
                .await
            {
                eprintln!("[AUDIT_ERROR] Failed to send audit msg: {}", e);
            }
        }
        Ok(())
    }

    /// Gracefully shuts down the worker.
    ///
    /// Notifies the `start()` loop to wake up and exit, waits until the worker
    /// becomes idle (or until timeout), and sends a shutdown acknowledgment message.
    ///
    /// # Errors
    /// Returns `CoreError` if sending the ack fails.
    pub async fn shutdown(
        &self,
        check_interval: Duration,
        timeout: Option<Duration>,
    ) -> Result<(), CoreError> {
        // Signal the start() loop to wake up and exit
        self.shutdown_notify.notify_one();

        let start = SystemTime::now();

        while self.get_status().await == Working {
            if let Some(to) = timeout {
                if start.elapsed().unwrap_or_default() > to {
                    warn!(
                        "[SHUTDOWN] Worker {} shutdown timeout, force exit",
                        self.name.clone()
                    );
                    break;
                }
            }
            tokio::time::sleep(check_interval).await;
        }

        let ack = ShutdownAck {
            worker_name: self.name.clone(),
            status: ShutdownStatus::Completed,
            timestamp: SystemTime::now(),
            error: None,
        };

        let ack_msg = EMessage::new(
            MessageTopic(SYSTEM_TOPIC_SHUTDOWN_ACK.to_string()),
            MessagePayload(bincode::encode_to_vec(&ack, bincode::config::standard()).map_err(|e| CoreError::Serialize(crate::error::serialize::SerializeError::SerializeError(e.to_string())))?),
            Standard,
            None,
        );

        TopicRouter::global()
            .read().await
            .send(SYSTEM_TOPIC_SHUTDOWN_ACK, ack_msg, None, None)
            .await?;
        Ok(())
    }

    /// Returns `true` if the shutdown process has completed.
    pub fn is_shutdown_complete(&self) -> bool {
        self.shutdown_complete.load(Ordering::SeqCst)
    }

    async fn set_status(&self, status: WorkerStatus) {
        match status {
            Idle => {
                let _ = ConsumerRouter::global()
                    .write().await
                    .set_idle(self.topic.clone(), self.name.clone())
                    .await;
            }
            Working => {
                let _ = ConsumerRouter::global()
                    .write().await
                    .set_working(self.topic.clone(), self.name.clone())
                    .await;
            }
        };
        *self.status.lock().await = status;
    }

    /// Returns the current status of the worker.
    pub async fn get_status(&self) -> WorkerStatus {
        *self.status.lock().await
    }

    /// Processes a **single** message and exits.
    ///
    /// This is used by the dynamic scaling feature in
    /// [`ConsumerRouter`](ConsumerRouter):
    /// when no idle worker is available, an ephemeral one‑shot worker is
    /// created and this method is called.  Unlike [`start`](Self::start),
    /// it does **not** enter a receive loop and does **not** register the
    /// worker in the router's idle pool — the worker is dropped after
    /// processing.
    pub async fn process_one(self: Arc<Self>, msg: EMessage) {
        // Mark as working so the status reflects active processing
        *self.status.lock().await = Working;
        let _ = self.process_msg(msg).await;
        // Intentionally skip self.set_status(Idle) — this worker is
        // ephemeral and will be dropped after the task exits.
    }

    /// **Test helper**: directly invokes `process_msg` for integration testing.
    ///
    /// This is equivalent to the private `process_msg` path but exposed for
    /// test crates to verify processing logic without spawning the full
    /// `start()` loop.
    #[doc(hidden)]
    pub async fn test_process_msg(&self, msg: EMessage) -> Result<(), CoreError> {
        self.process_msg(msg).await
    }

    /// **Test helper**: exposes `requeue_message`.
    #[doc(hidden)]
    pub async fn test_requeue_message(&self, msg: EMessage) -> Result<(), CoreError> {
        self.requeue_message(msg).await
    }

    /// **Test helper**: exposes `generate_audit_msg`.
    #[doc(hidden)]
    pub fn test_generate_audit_msg(
        &self,
        msg: EMessage,
        result: AuditResult,
        audit_event_type: AuditEventType,
        error: Option<String>,
        duration: Option<Duration>,
    ) -> AuditRecord {
        self.generate_audit_msg(msg, result, audit_event_type, error, duration)
    }

    /// **Test helper**: exposes `send_to_dead_letter`.
    #[doc(hidden)]
    pub async fn test_send_to_dead_letter(
        &self,
        msg: EMessage,
        reason: DeadReason,
        process_time: Duration,
    ) -> Result<(), CoreError> {
        self.send_to_dead_letter(msg, reason, process_time).await
    }
}
