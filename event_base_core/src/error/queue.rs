#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("Queue is full")]
    Full,
    #[error("Queue is closed")]
    Closed,
    #[error("Send timeout")]
    Timeout,
    #[error("Send error: {0}")]
    Send(String),
    #[error("Receive error: {0}")]
    Receive(String),
}
