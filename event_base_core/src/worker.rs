use crate::audit::{AuditEventType, AuditRecord, AuditResult};
use crate::constant::SYSTEM_TOPIC_AUDIT;
use crate::dead_letter::DeadReason;
use crate::error::CoreError;
use crate::error::serialize::SerializeError;
use crate::handler::Ack::{Ack, Dead, NoAck};
use crate::handler::EHandler;
use crate::message::DeliveryMode::Repeated;
use crate::message::{DeliveryMode, EMessage, MessageMetadata, MessagePayload, MessageTopic};
use crate::queues::{EConsumer, EProducer};
use crate::shutdown::ShutdownReceiver;
use crate::topic::TopicRouter;
use crate::wal::wal::{Wal, WalRecord, WalRecordState};
use std::option::Option;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::time::timeout;
use uuid::Uuid;
use crate::middleware::Pipeline;

pub struct Worker {
    pub topic: String,
    pub name: String,
    pub consumer: Box<dyn EConsumer>,
    pub pipeline: Arc<Pipeline>,
    pub producer: Arc<dyn EProducer>,
    pub time_out: Option<Duration>,
    pub shutdown_receiver: ShutdownReceiver,
    wal: Option<Arc<tokio::sync::Mutex<dyn Wal>>>,
}

impl Worker {
    pub fn new(
        topic: String,
        consumer: Box<dyn EConsumer>,
        pipeline: Arc<Pipeline>,
        producer: Arc<dyn EProducer>,
        time_out: Option<Duration>,
        shutdown_receiver: ShutdownReceiver,
        wal: Arc<tokio::sync::Mutex<dyn Wal>>,
    ) -> Self {
        let wal = Some(wal);
        let name = format!("worker-{}-{}", topic, Uuid::new_v4());
        Self {
            topic,
            name,
            consumer,
            pipeline,
            producer,
            time_out,
            shutdown_receiver,
            wal,
        }
    }
    pub async fn start(&mut self) {
        // 主消费循环
        loop {
            tokio::select! {
                _ = self.shutdown_receiver.recv() => {
                    break;
                }
                msg = self.consumer.receive() => {
                    let msg = msg.unwrap();
                    self.process_msg(msg).await;

                }
            }
        }
    }

    async fn process_msg(&mut self, mut msg: EMessage) {
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
                tracing::error!("[AUDIT_ERROR] Failed to send audit msg: {}", e);
            }
        }

        let start_time = SystemTime::now();

        self.update_wal(&msg.id, WalRecordState::Processing).await;

        let status;

        if let Some(time) = self.time_out {
            match timeout(time, self.pipeline.run(&mut msg)).await {
                Ok(result) => {
                    status = result;
                }
                Err(_) => status = Dead,
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
                        self.update_wal(&msg.id, WalRecordState::Complete).await;
                        return;
                    }
                    self.update_wal(&msg.id, WalRecordState::Pending).await;
                    self.producer.send(msg).await.expect("Fail to requeue msg");
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
                self.update_wal(&msg.id, WalRecordState::Complete).await;
            }

            NoAck {
                retry_after,
                max_retries,
            } => {
                msg.attempts += 1;
                self.update_wal(&msg.id, WalRecordState::Pending).await;

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
                            .send(&msg.clone().topic.0, msg.clone())
                            .await
                            .expect("Fail to requeue the NoAck msg");
                    } else {
                        if let Err(e) = self.producer.send(msg.clone()).await {
                            tracing::error!("Failed to requeue message {}: {}", &msg.id, e);
                        }
                    }
                }
            }
            Dead => {
                self.update_wal(&msg.id, WalRecordState::Failed).await;
                if let Err(e) = self
                    .send_to_dead_letter(msg.clone(), DeadReason::Explicit, process_time)
                    .await
                {
                    tracing::error!("Failed to send to dead letter: {}", e);
                }
            }
        }
    }

    async fn requeue_message(&self, msg: EMessage) {
        self.update_wal(&msg.id, WalRecordState::Pending).await;
        let _ = self.producer.send(msg).await;
    }

    async fn update_wal(&self, msg_id: &str, state: WalRecordState) {
        if let Some(wal) = &self.wal {
            let mut wal = wal.lock().await;
            let _ = wal.update_state(msg_id, state).await;
        }
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

    async fn send_audit_msg(&mut self, msg: AuditRecord) -> Result<(), CoreError> {
        let audit_msg = EMessage::new(
            MessageTopic(SYSTEM_TOPIC_AUDIT.parse().unwrap()),
            MessagePayload(
                serde_json::to_vec(&msg)
                    .map_err(|e| SerializeError::SerializeError(e.to_string()))
                    .unwrap(),
            ),
            DeliveryMode::Standard,
        );
        TopicRouter::global()
            .send(SYSTEM_TOPIC_AUDIT, audit_msg)
            .await?;

        Ok(())
    }

    async fn send_to_dead_letter(
        &mut self,
        mut msg: EMessage,
        reason: DeadReason,
        process_time: Duration,
    ) -> Result<(), CoreError> {
        let dead_letter_topic_name = format!("dead_letter.{}", self.topic);
        msg.topic = MessageTopic(dead_letter_topic_name.clone());

        if let Some(wal) = &mut self.wal {
            let mut wal = wal.lock().await;
            let _ = wal
                .append(WalRecord {
                    record_id: 0, // Auto Generate
                    message: msg.clone(),
                    status: WalRecordState::Failed,
                    last_attempt_at: None,
                    is_dead_letter: true,
                    dead_reason: Some(reason),
                })
                .await?;
        }

        TopicRouter::global()
            .send(&dead_letter_topic_name, msg.clone())
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
}
