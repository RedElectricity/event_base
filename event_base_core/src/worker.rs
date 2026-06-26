use crate::audit::{AuditEventType, AuditRecord, AuditResult};
use crate::constant::{SYSTEM_TOPIC_AUDIT, SYSTEM_TOPIC_SHUTDOWN_ACK};
use crate::dead_letter::DeadReason;
use crate::error::CoreError;
use crate::error::serialize::SerializeError::SerializeError;
use crate::handler::Ack::{Ack, Dead, NoAck};
use crate::message::DeliveryMode::{Repeated, Standard};
use crate::message::{EMessage, MessagePayload, MessageTopic};
use crate::middleware::Pipeline;
use crate::queues::{EConsumer, EProducer};
use crate::shutdown::ShutdownReceiver;
use crate::shutdown::messages::{ShutdownAck, ShutdownStatus};
use crate::topic::TopicRouter;
use crate::wal::sync::WalClient;
use crate::worker::WorkerStatus::Working;
use std::option::Option;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::{error, warn};
use uuid::Uuid;

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
        }
    }
    pub async fn start(&self) {
        let mut shutdown_receiver = self.shutdown_receiver.lock().await;
        let mut consumer = self.consumer.lock().await;

        loop {
            tokio::select! {
                _ = shutdown_receiver.recv() => {
                    self.shutdown(self.shutdown_check_interval, self.shutdown_timeout).await;
                    break;
                }
                msg = consumer.receive() => {
                    self.set_status(Working).await;
                    if let Some(msg) = msg {
                        self.process_msg(msg).await;
                    }
                    self.set_status(WorkerStatus::Idle).await;
                }
            }
        }
    }

    async fn process_msg(&self, mut msg: EMessage) {
        if self.topic != SYSTEM_TOPIC_AUDIT {
            if let Err(e) = self
                .send_audit_msg(self.generate_audit_msg(
                    msg.clone(),
                    AuditResult::Start,
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
            .await
            .expect("Fail to push WAL msg when mark the msg as processing");
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

        let process_time = finish_time
            .duration_since(start_time)
            .map_err(|te| eprintln!("[AUDIT_ERROR] Fail to process time: {}", te))
            .unwrap();

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
                            .await
                            .expect("Fail to push WAL msg when mark the msg as complete");
                        return;
                    }
                    self.wal
                        .mark_pending(msg.clone().id.as_str(), msg.clone().topic.0.as_str())
                        .await
                        .expect("Fail to push WAL msg when mark the msg as pending");
                    self.requeue_message(msg).await;
                    return;
                }
                if self.topic != SYSTEM_TOPIC_AUDIT {
                    if let Err(e) = self
                        .send_audit_msg(self.generate_audit_msg(
                            msg.clone(),
                            AuditResult::Success,
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
                    .await
                    .expect("Fail to push WAL msg when mark the msg as complete");
            }

            NoAck {
                retry_after,
                max_retries,
            } => {
                msg.attempts += 1;
                self.wal
                    .mark_pending(msg.clone().id.as_str(), msg.clone().topic.0.as_str())
                    .await
                    .expect("Fail to push WAL msg when mark the msg as pending");

                if msg.attempts >= max_retries {
                    self.send_to_dead_letter(
                        msg.clone(),
                        DeadReason::MaxRetriesExceeded,
                        process_time,
                    )
                    .await
                    .expect("Fail to send to Dead Letter");
                } else {
                    if let Some(delay) = retry_after {
                        msg.deliver_at = SystemTime::now().checked_add(delay);
                        TopicRouter::global()
                            .send(&msg.clone().topic.0, msg.clone(), None, None)
                            .await
                            .expect("Fail to requeue the NoAck msg");
                    } else {
                        if let Err(e) = self.producer.send(msg.clone()).await {
                            error!("Failed to requeue message {}: {}", &msg.id, e);
                        }
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
                    .await
                    .expect("Fail to push WAL msg when mark the msg to dead letter");
                if let Err(e) = self
                    .send_to_dead_letter(msg.clone(), DeadReason::Explicit, process_time)
                    .await
                {
                    error!("Failed to send to dead letter: {}", e);
                }
            }
        }
    }

    async fn requeue_message(&self, msg: EMessage) {
        self.wal
            .mark_pending(msg.clone().id.as_str(), msg.clone().topic.0.as_str())
            .await
            .expect("Fail to push WAL msg when mark the msg as pending");
        let _ = self.producer.send(msg).await;
    }

    fn generate_audit_msg(
        &self,
        msg: EMessage,
        result: AuditResult,
        error: Option<String>,
        duration: Option<Duration>,
    ) -> AuditRecord {
        AuditRecord {
            message_id: msg.id.clone(),
            topic: msg.topic.0.clone(),
            event_type: AuditEventType::ProcessingStarted,
            worker_id: Some(self.name.clone()),
            timestamp: SystemTime::now(),
            result,
            error,
            duration,
        }
    }

    async fn send_audit_msg(&self, msg: AuditRecord) -> Result<(), CoreError> {
        let audit_msg = EMessage::new(
            MessageTopic(SYSTEM_TOPIC_AUDIT.parse().unwrap()),
            MessagePayload(serde_json::to_vec(&msg).map_err(|e| SerializeError(e.to_string()))?),
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
            .await
            .expect("Fail to push WAL msg when mark the msg to dead letter");

        TopicRouter::global()
            .send(&dead_letter_topic_name, msg.clone(), None, None)
            .await?;

        if self.topic != SYSTEM_TOPIC_AUDIT {
            if let Err(e) = self
                .send_audit_msg(self.generate_audit_msg(
                    msg.clone(),
                    AuditResult::Dead,
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

    pub async fn shutdown(&self, check_interval: Duration, timeout: Option<Duration>) {
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
            MessagePayload(
                serde_json::to_vec(&ack)
                    .map_err(|e| error!("{}", SerializeError(e.to_string()).to_string()))
                    .unwrap(),
            ),
            Standard,
            None,
        );

        TopicRouter::global()
            .send(SYSTEM_TOPIC_SHUTDOWN_ACK, ack_msg, None, None)
            .await
            .expect("[SHUTDOWN ACK] Fail to send shutdown ack message");
    }

    async fn set_status(&self, status: WorkerStatus) {
        *self.status.lock().await = status;
    }

    pub async fn get_status(&self) -> WorkerStatus {
        *self.status.lock().await
    }
}
