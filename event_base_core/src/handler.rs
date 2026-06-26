use crate::dead_letter::DeadReason;
use crate::message::EMessage;
use async_trait::async_trait;
use std::time::Duration;

#[derive(Debug)]
pub enum Ack {
    Ack,
    NoAck {
        retry_after: Option<Duration>,
        max_retries: u32,
    },
    Dead {
        dead_reason: DeadReason,
    },
}

#[async_trait]
pub trait EHandler: Send + Sync {
    async fn handler(&self, msg: &EMessage) -> Ack;
}

#[async_trait]
impl EHandler for Box<dyn EHandler> {
    async fn handler(&self, msg: &EMessage) -> Ack {
        (**self).handler(msg).await
    }
}
