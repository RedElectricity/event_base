#[derive(Debug, thiserror::Error)]
pub enum TopicError {
    #[error("Topic already exists: {0}")]
    AlreadyExists(String),

    #[error("Topic Not Found: {0}")]
    NotFound(String),
}
