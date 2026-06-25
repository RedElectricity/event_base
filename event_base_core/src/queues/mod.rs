pub mod consumer_factory;
pub mod consumer_router;
pub mod factory;

use crate::error::CoreError;
use crate::message::EMessage;
use async_trait::async_trait;
use std::time::{Duration, SystemTime};
#[async_trait]
pub trait EProducer: Send + Sync {
    async fn send(&self, msg: EMessage) -> Result<(), CoreError>;
    fn try_send(&self, msg: EMessage) -> Result<(), CoreError>;
    async fn send_timeout(&self, msg: EMessage, timeout: Duration) -> Result<(), CoreError>;
}

#[async_trait]
pub trait EConsumer: Send + Sync {
    async fn receive(&mut self) -> Option<EMessage>;
    async fn claim(&mut self) -> Result<Option<ClaimedMessage>, CoreError>;
    async fn ack(&mut self, claim_id: &str) -> Result<(), CoreError>;
    async fn nack(&mut self, claim_id: &str) -> Result<(), CoreError>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone)]
pub struct ClaimedMessage {
    pub message: EMessage,
    pub claim_id: String,
    pub claimed_at: SystemTime,
}
