#[derive(Debug, thiserror::Error)]
pub enum MiddlewareError {
    #[error("Execution failed: {0}")]
    Execution(String),
    #[error("Middleware chain interrupted")]
    Interrupted,
}
