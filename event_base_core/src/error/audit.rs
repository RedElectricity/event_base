//! Audit-related errors.
//!
//! These errors occur during audit record handling, such as when writing
//! to an audit sink or when the audit buffer is full.

#[derive(Debug, thiserror::Error)]
pub enum AuditError {
    /// An error occurred while writing an audit record to a sink.
    #[error("Write error: {0}")]
    Write(String),

    /// The audit ring buffer is full and cannot accept more records.
    #[error("Buffer full")]
    BufferFull,

    /// A required tracing span was missing when attempting to record an event.
    #[error("Missing Span")]
    MissingSpan,
}
