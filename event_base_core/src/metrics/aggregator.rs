use crate::audit::{AuditEventType, AuditRecord};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct MetricsAggregator {
    pub enqueued: HashMap<String, u64>,
    pub completed: HashMap<String, u64>,
    pub failed: HashMap<String, u64>,
    pub retried: HashMap<String, u64>,
    pub latency_sum: HashMap<String, (u64, Duration)>, // (count, sum_duration)
}

impl MetricsAggregator {
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

    pub fn snapshot(&self) -> MetricsAggregator {
        self.clone()
    }
}
