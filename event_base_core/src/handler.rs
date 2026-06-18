use std::io::Error;
use std::time::Duration;
use dynify::dynify;
use crate::error::CoreError;
use crate::message::EMessage;

pub enum Ack {
    Ack,
    NoAck { retry_after: Option<Duration> , max_retries: u32 },
    Dead,
}

#[async_trait::async_trait]
pub trait EHandler: Send + Sync {
    async fn handle(&self, msg: &EMessage) -> Ack;
}