use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "config")]
pub enum ShutdownStrategy {
    TwoStage {
        poll_interval_ms: u64,
        force_timeout_secs: u64,
    },
    Graceful {
        worker_name: String,
        poll_interval_ms: u64,
    },
    Force,
    Timeout {
        total_timeout_secs: u64,
    },
    StateBasedIdle,
    Batched {
        batch_size: usize,
        interval_ms: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownCommand {
    pub strategy: ShutdownStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownAck {
    pub worker_name: String,
    pub status: ShutdownStatus,
    pub timestamp: SystemTime,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ShutdownStatus {
    Completed,
    Failed,
    Timeout,
}
