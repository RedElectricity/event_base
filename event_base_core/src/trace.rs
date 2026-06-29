//! Data structures for distributed tracing records.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

/// A complete trace record for a span or event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceRecord {
    /// Optional trace ID for correlation.
    pub trace_id: Option<String>,
    /// Unique ID of this span.
    pub span_id: String,
    /// ID of the parent span (if any).
    pub parent_span_id: Option<String>,
    /// Name of the span/event.
    pub name: String,
    /// Target (module) where the span/event originated.
    pub target: String,
    /// Severity level.
    pub level: TraceLevel,
    /// Key-value fields captured from the span/event.
    pub fields: HashMap<String, serde_json::Value>,
    /// Start time of the span (if available).
    pub started_at: Option<SystemTime>,
    /// Finish time of the span (if available).
    pub finished_at: Option<SystemTime>,
    /// Duration of the span (if finished).
    pub duration: Option<Duration>,
    /// Optional ID of the message being processed.
    pub message_id: Option<String>,
    /// Optional worker ID.
    pub worker_id: Option<String>,
    /// Optional topic.
    pub topic: Option<String>,
}

/// Severity level of a trace record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TraceLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}
