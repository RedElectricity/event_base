#[derive(Debug, thiserror::Error)]
pub enum SerializeError {
    #[error("Serialize error: {0}")]
    SerializeError(String),

    #[error("Deserialize error: {0}")]
    DeserializeError(String),
}
