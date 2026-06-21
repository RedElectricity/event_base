use crate::audit::{AuditEventType, AuditRecord};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct MetricsAggregator {
    // 只保留当前值，不存历史
    enqueued: HashMap<String, u64>,
    completed: HashMap<String, u64>,
    failed: HashMap<String, u64>,
    retried: HashMap<String, u64>,
    dead: HashMap<String, u64>,
    latency_sum: HashMap<String, (u64, Duration)>, // (count, sum_duration)
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
            _ => unreachable!(),
        }
    }

    pub fn snapshot(&self) -> MetricsAggregator {
        self.clone()
    }
}
