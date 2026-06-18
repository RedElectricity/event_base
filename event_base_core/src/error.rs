use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("Serialize error: {0}")]
    SerializeError(String),

    #[error("Deserialize error: {0}")]
    DeserializeError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Msg Handler Not Found: {0}")]
    HandlerNotFound(String),

    #[error("Queue Send Error: {0}")]
    QueueSendError(String),

    #[error("Handler Error: {0}")]
    HandlerError(String),
    
    #[error("Wal Record Not Found: {0}")]
    WalRecordNotFound(String),

    #[error("Invalid Parameter: {0}")]
    InvalidParameter(String),

    #[error("Invalid Type: {0}")]
    InvalidData(String),

    #[error("Database Error: {0}")]
    DatabaseError(String),

    #[error("Wal Task Join Error: {0}")]
    TaskJoinError(String),

    #[error("Object already exists")]
    AlreadyInitialized(),

    #[error("Topic already exists: {0}")]
    TopicAlreadyExists(String),

    #[error("Topic Not Found: {0}")]
    TopicNotFound(String),
    
    #[error("Error Time")]
    ErrorTime()
}