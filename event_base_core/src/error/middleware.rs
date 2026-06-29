//! Middleware-related errors.
//!
//! These errors occur during middleware chain execution, such as when a
//! middleware fails or the chain is interrupted.

#[derive(Debug, thiserror::Error)]
pub enum MiddlewareError {
    /// Execution of a middleware failed.
    #[error("Execution failed: {0}")]
    Execution(String),

    /// The middleware chain was interrupted (e.g., by a short‑circuit).
    #[error("Middleware chain interrupted")]
    Interrupted,
}
