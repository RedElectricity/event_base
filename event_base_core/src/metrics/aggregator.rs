//! In-memory aggregation of audit metrics by topic.
//!
//! The [`MetricsAggregator`] consumes [`AuditRecord`] events and maintains
//! counts and latency summaries for each topic.

use crate::audit::{AuditEventType, AuditRecord};
use std::collections::HashMap;
use std::time::Duration;

/// An aggregator that compiles metrics from audit records.
///
/// It tracks the number of enqueued, completed, failed, and retried messages,
/// as well as the total count and sum of processing latencies per topic.
#[derive(Debug, Clone)]
pub struct MetricsAggregator {
    /// Number of messages enqueued per topic.
    pub enqueued: HashMap<String, u64>,
    /// Number of messages successfully completed per topic.
    pub completed: HashMap<String, u64>,
    /// Number of messages that failed (dead-lettered) per topic.
    pub failed: HashMap<String, u64>,
    /// Number of retry attempts per topic.
    pub retried: HashMap<String, u64>,
    /// Tuple of (count, total_duration) for completed messages, keyed by topic.
    pub latency_sum: HashMap<String, (u64, Duration)>,
}

impl MetricsAggregator {
    /// Feeds a single audit record into the aggregator, updating the relevant counters.
    ///
    /// - `Enqueued` increments the enqueued count.
    /// - `DeadLettered` increments the failed count.
    /// - `Retry` increments the retried count.
    /// - `ProcessingCompleted` increments the completed count and accumulates latency.
    /// - `ProcessingStarted` is ignored for aggregation.
    pub fn feed(&mut self, record: &AuditRecord) {
        match record.event_type {
            AuditEventType::Enqueued => {
                *self.enqueued.entry(record.topic.clone()).or_default() += 1;
            }

            AuditEventType::DeadLettered => {
                *self.failed.entry(record.topic.clone()).or_default() += 1;
            }

            AuditEventType::Retry => {
                *self.retried.entry(record.topic.clone()).or_default() += 1;
            }

            AuditEventType::ProcessingCompleted => {
                *self.completed.entry(record.topic.clone()).or_default() += 1;
                if let Some(duration) = record.duration {
                    let entry = self.latency_sum.entry(record.topic.clone()).or_default();
                    entry.0 += 1;
                    entry.1 += duration;
                }
            }

            AuditEventType::ProcessingStarted => {}
        }
    }

    /// Returns a clone of the current aggregator state as a snapshot.
    pub fn snapshot(&self) -> MetricsAggregator {
        self.clone()
    }
}
