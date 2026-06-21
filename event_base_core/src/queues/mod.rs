pub mod consumer_factory;
pub mod factory;

use crate::error::CoreError;
use crate::message::EMessage;
use async_trait::async_trait;
use std::time::Duration;
#[async_trait]
pub trait EProducer: Send + Sync {
    async fn send(&self, msg: EMessage) -> Result<(), CoreError>;
    fn try_send(&self, msg: EMessage) -> Result<(), CoreError>;
    async fn send_timeout(&self, msg: EMessage, timeout: Duration) -> Result<(), CoreError>;
}

#[async_trait]
pub trait EConsumer: Send + Sync {
    async fn receive(&mut self) -> Option<EMessage>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
