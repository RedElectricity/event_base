#[derive(Debug, thiserror::Error)]
pub enum AuditError {
    #[error("Write error: {0}")]
    Write(String),
    #[error("Buffer full")]
    BufferFull,
    #[error("Missing Span")]
    MissingSpan,
}
