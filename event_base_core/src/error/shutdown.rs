use std::time::Duration;

// error/shutdown.rs
#[derive(Debug, thiserror::Error)]
pub enum ShutdownError {
    #[error("Timeout: {0:?}")]
    Timeout(Duration),
    #[error("Component '{0}' not found")]
    ComponentNotFound(String),
    #[error("Component '{0}' shutdown failed: {1}")]
    ComponentFailed(String, String),
}
