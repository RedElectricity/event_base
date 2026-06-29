//! Handler-related errors.
//!
//! These errors occur during message handler registration or execution.

#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    /// No handler is registered for the given message topic.
    #[error("Msg Handler Not Found: {0}")]
    NotFound(String),

    /// An error occurred while executing the handler.
    #[error("Msg Handler Error: {0}")]
    Error(String),
}
