#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    #[error("Msg Handler Not Found: {0}")]
    NotFound(String),

    #[error("Msg Handler Error: {0}")]
    Error(String),
}
