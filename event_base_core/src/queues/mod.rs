pub mod memory;
pub mod factory;
pub mod consumer_factory;

use std::sync::atomic::Ordering;
use async_trait::async_trait;
use dynify::dynify;
use dynosaur::dynosaur;
use crate::message::EMessage;
use crate::error::CoreError;

#[async_trait]
pub trait EProducer: Send + Sync {
    async fn send(&self, msg: EMessage) -> Result<(), CoreError>;
}

#[async_trait]
pub trait EConsumer: Send + Sync {
    async fn receive(&mut self) -> Option<EMessage>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool { self.len() == 0 }
}