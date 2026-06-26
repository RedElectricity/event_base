use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceRecord {
    pub trace_id: Option<String>,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub target: String,
    pub level: TraceLevel,
    pub fields: HashMap<String, serde_json::Value>,
    pub started_at: Option<SystemTime>,
    pub finished_at: Option<SystemTime>,
    pub duration: Option<Duration>,
    pub message_id: Option<String>,
    pub worker_id: Option<String>,
    pub topic: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TraceLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}
