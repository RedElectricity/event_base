use std::option::Option;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::time::{timeout, Timeout};
use tokio::time::error::Elapsed;
use uuid::Uuid;
use crate::dead_letter::{DeadLetterMessage, DeadReason};
use crate::error::CoreError;
use crate::handler::EHandler;
use crate::handler::Ack::{Ack, Dead, NoAck};
use crate::message::{EMessage, MessageMetadata, MessageTopic};
use crate::message::DeliveryMode::{Broadcast, Repeated};
use crate::queues::{EConsumer, EProducer};
use crate::shutdown::ShutdownReceiver;
use crate::topic::TopicRouter;
use crate::wal::wal::{Wal, WalRecord, WalRecordState};

pub struct Worker {
    pub topic: String,
    pub name: String,
    pub consumer: Box<dyn EConsumer>,
    pub handler: Arc<dyn EHandler>,
    pub producer: Arc<dyn EProducer>,
    pub time_out: Option<Duration>,
    pub shutdown_receiver: ShutdownReceiver,
    wal: Option<Arc<tokio::sync::Mutex<dyn Wal>>>,
}

impl Worker{
    pub fn new(
        topic: String,
        consumer: Box<dyn EConsumer>,
        handler: Arc<dyn EHandler>,
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
            handler,
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
        self.update_wal(&msg.id, WalRecordState::Processing).await;

        let status;

        if let Some(time) = self.time_out {
            match timeout(time,
                          self.handler.handle(&msg)).await {
                Ok(result) => { status = result; }
                Err(_) => { status = Dead}
            }
        } else {
            status = self.handler.handle(&msg).await;
        }

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
                self.update_wal(&msg.id, WalRecordState::Complete).await;
            },

            NoAck { retry_after, max_retries } => {
                msg.attempts += 1;
                self.update_wal(&msg.id, WalRecordState::Pending).await;

                if msg.attempts >= max_retries {
                    self.send_to_dead_letter(msg, DeadReason::MaxRetriesExceeded).await.expect("Fail to send to Dead Letter");
                } else {
                    if let Some(delay) = retry_after {
                        // TODO: 发送到延迟队列（Host 调度）
                    } else {
                        if let Err(e) = self.producer.send(msg.clone()).await {
                            tracing::error!("Failed to requeue message {}: {}", &msg.id, e);
                        }

                    }
                }
            }
            Dead => {
                // 直接进入死信
                self.update_wal(&msg.id, WalRecordState::Failed).await;
                if let Err(e) = self.send_to_dead_letter(msg.clone(), DeadReason::Explicit).await {
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

    async fn send_to_dead_letter(&mut self, mut msg: EMessage, reason: DeadReason) -> Result<(), CoreError> {
        let dead_letter_topic_name = format!("dead_letter.{}", self.topic);
        msg.topic = MessageTopic(dead_letter_topic_name.clone());

        if let Some(wal) = &mut self.wal {
            let mut wal = wal.lock().await;
            let _ = wal.append(WalRecord {
                record_id: 0, // Auto Generate
                message: msg.clone(),
                status: WalRecordState::Failed,
                last_attempt_at: None,
                is_dead_letter: true,
                dead_reason: Some(reason),
            }).await?;
        }

        TopicRouter::global().send(&dead_letter_topic_name, msg).await?;

        Ok(())
    }
}