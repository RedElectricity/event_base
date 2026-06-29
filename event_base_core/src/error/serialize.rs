//! Serialization and deserialization errors.
//!
//! These errors occur when encoding or decoding data (e.g., using bincode or JSON).

#[derive(Debug, thiserror::Error)]
pub enum SerializeError {
    /// An error occurred while serializing data to bytes.
    #[error("Serialize error: {0}")]
    SerializeError(String),

    /// An error occurred while deserializing bytes to a data structure.
    #[error("Deserialize error: {0}")]
    DeserializeError(String),
}
