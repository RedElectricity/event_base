use crate::message::EMessage;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct DeadLetterMessage {
    pub original_message: EMessage,
    pub dead_reason: DeadReason,
    pub died_at: SystemTime,
    pub attempts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum DeadReason {
    /// 超过最大重试次数
    MaxRetriesExceeded,
    /// Handler 显式返回 Dead
    Explicit,
    /// 超时
    Timeout,
    /// 找不到对应的 Handler
    NoHandler,
    /// 其他原因
    Other(String),
}
