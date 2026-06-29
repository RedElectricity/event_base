//! Write‑Ahead Log (WAL) errors.
//!
//! These errors occur when interacting with the WAL, such as reading, writing,
//! or corruptions.

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    /// A record with the given ID was not found.
    #[error("Record not found: {0}")]
    RecordNotFound(String),

    /// The WAL data is corrupted or malformed.
    #[error("WAL corrupted: {0}")]
    Corrupted(String),

    /// An error occurred in the underlying backend (e.g., file system or database).
    #[error("Backend error: {0}")]
    Backend(String),

    /// A write operation failed.
    #[error("Write error: {0}")]
    Write(String),
}
