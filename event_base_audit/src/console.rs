use async_trait::async_trait;
use chrono::{DateTime, Utc};
use event_base_core::audit::{AuditRecord, AuditWriter};
use event_base_core::error::CoreError;

pub struct ConsoleAuditWriter;

#[async_trait]
impl AuditWriter for ConsoleAuditWriter {
    async fn write(&self, record: &AuditRecord) -> Result<(), CoreError> {
        let datetime: DateTime<Utc> = record.timestamp.into();

        println!(
            "[AUDIT] {} | topic={} | msg={} | worker={:?} | result={:?} | duration={:?}ms | error={:?}",
            datetime.format("%Y-%m-%d %H:%M:%S").to_string(),
            record.topic,
            record.message_id,
            record.worker_id,
            record.result,
            record.duration,
            record.error,
        );
        Ok(())
    }
}