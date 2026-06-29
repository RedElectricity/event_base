//! Topic‑related errors.
//!
//! These errors occur when registering or looking up topics.

#[derive(Debug, thiserror::Error)]
pub enum TopicError {
    /// An attempt was made to register a topic that already exists.
    #[error("Topic already exists: {0}")]
    AlreadyExists(String),

    /// The requested topic does not exist.
    #[error("Topic Not Found: {0}")]
    NotFound(String),
}
