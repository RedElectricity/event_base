#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("Record not found: {0}")]
    RecordNotFound(String),
    #[error("WAL corrupted: {0}")]
    Corrupted(String),
    #[error("Backend error: {0}")]
    Backend(String),
    #[error("Write error: {0}")]
    Write(String),
}
