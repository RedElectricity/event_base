//! Shutdown‑related errors.
//!
//! These errors occur during shutdown coordination, such as timeouts or
//! missing components.

use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum ShutdownError {
    /// The shutdown process timed out.
    #[error("Timeout: {0:?}")]
    Timeout(Duration),

    /// A component required for shutdown was not found.
    #[error("Component '{0}' not found")]
    ComponentNotFound(String),

    /// A component failed during shutdown.
    #[error("Component '{0}' shutdown failed: {1}")]
    ComponentFailed(String, String),
}
