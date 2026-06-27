use crate::audit::{AuditEventType, AuditRecord, AuditResult};
use crate::constant::{SYSTEM_TOPIC_AUDIT, SYSTEM_TOPIC_SHUTDOWN_ACK};
use crate::dead_letter::DeadReason;
use crate::error::CoreError;
use crate::handler::Ack::{Ack, Dead, NoAck};
use crate::message::DeliveryMode::{Repeated, Standard};
use crate::message::{EMessage, MessagePayload, MessageTopic};
use crate::middleware::Pipeline;
use crate::queues::{EConsumer, EProducer};
use crate::shutdown::ShutdownReceiver;
use crate::shutdown::messages::{ShutdownAck, ShutdownStatus};
use crate::topic::TopicRouter;
use crate::wal::sync::WalClient;
use crate::worker::WorkerStatus::{Idle, Working};
use std::option::Option;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::{error, warn};
use uuid::Uuid;
use crate::queues::consumer_router::ConsumerRouter;

pub struct Worker {
    pub topic: String,
    pub name: String,
    pub consumer: Arc<Mutex<Box<dyn EConsumer>>>,
    pub pipeline: Arc<Pipeline>,
    pub producer: Arc<dyn EProducer>,
    pub time_out: Option<Duration>,
    pub shutdown_receiver: Arc<Mutex<ShutdownReceiver>>,
    pub shutdown_check_interval: Duration,
    pub shutdown_timeout: Option<Duration>,
    pub status: Arc<Mutex<WorkerStatus>>,
    wal: WalClient,
    shutdown_complete: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStatus {
    Idle,
    Working,
}

impl Worker {
    pub fn new(
        topic: String,
        consumer: Box<dyn EConsumer>,
        pipeline: Arc<Pipeline>,
        producer: Arc<dyn EProducer>,
        time_out: Option<Duration>,
        shutdown_check_interval: Duration,
        shutdown_timeout: Option<Duration>,
        shutdown_receiver: ShutdownReceiver,
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
            shutdown_receiver: Arc::new(Mutex::new(shutdown_receiver)),
            status: Arc::new(Mutex::new(WorkerStatus::Idle)),
            wal: WalClient::new(name),
            shutdown_complete: Arc::new(AtomicBool::new(false)),
        }
    }
    pub async fn start(&self) {
        let mut shutdown_receiver = self.shutdown_receiver.lock().await;
        let mut consumer = self.consumer.lock().await;

        loop {
            tokio::select! {
                _ = shutdown_receiver.recv() => {
                    let _ = self.shutdown(self.shutdown_check_interval, self.shutdown_timeout).await;
                    break;
                }
                msg = consumer.receive() => {
                    self.set_status(Working).await;
                    if let Some(msg) = msg {
                        let _ = self.process_msg(msg).await;
                    }
                    self.set_status(WorkerStatus::Idle).await;
                }
            }
        }
        self.shutdown_complete.store(true, Ordering::SeqCst);
    }

    async fn process_msg(&self, mut msg: EMessage) -> Result<(), CoreError> {
        if self.topic != SYSTEM_TOPIC_AUDIT {
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

        self.wal
            .mark_processing(msg.clone().id.as_str(), msg.clone().topic.0.as_str())
            .await?;
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

    async fn send_audit_msg(&self, msg: AuditRecord) -> Result<(), CoreError> {
        let audit_msg = EMessage::new(
            MessageTopic(SYSTEM_TOPIC_AUDIT.to_string()),
            MessagePayload(serde_json::to_vec(&msg)?),
            Standard,
            None,
        );
        TopicRouter::global()
            .send(SYSTEM_TOPIC_AUDIT, audit_msg, None, None)
            .await?;

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

    pub async fn shutdown(
        &self,
        check_interval: Duration,
        timeout: Option<Duration>,
    ) -> Result<(), CoreError> {
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
            MessagePayload(serde_json::to_vec(&ack)?),
            Standard,
            None,
        );

        TopicRouter::global()
            .send(SYSTEM_TOPIC_SHUTDOWN_ACK, ack_msg, None, None)
            .await?;
        Ok(())
    }

    pub fn is_shutdown_complete(&self) -> bool {
        self.shutdown_complete.load(Ordering::SeqCst)
    }

    async fn set_status(&self, status: WorkerStatus) {
        match status { 
            Idle => {
                let _ = ConsumerRouter::global().set_idle(self.topic.clone(), self.name.clone()).await;
            }
            Working => {
                let _ = ConsumerRouter::global().set_working(self.topic.clone(), self.name.clone()).await;
            }
        };
        *self.status.lock().await = status;
    }

    pub async fn get_status(&self) -> WorkerStatus {
        *self.status.lock().await
    }
}
