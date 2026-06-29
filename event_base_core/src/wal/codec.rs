//! Codec for encoding and decoding `WalRecord` to/from byte streams.
//!
//! This module defines the [`WalRecordCodec`] trait and provides a default
//! implementation using `bincode` for serialization.

use crate::error::serialize::SerializeError;
use crate::wal::wal::WalRecord;
use bincode::config;

/// A codec that can encode a `WalRecord` into bytes and decode it back.
///
/// Implementations must be `Clone` and `'static` so they can be shared
/// across threads and stored in long‑lived structures.
pub trait WalRecordCodec: Send + Sync + Clone + 'static {
    /// Encodes a `WalRecord` into a byte vector.
    ///
    /// # Errors
    /// Returns `SerializeError` if serialization fails.
    fn encode(&self, record: &WalRecord) -> Result<Vec<u8>, SerializeError>;

    /// Decodes a `WalRecord` from a byte slice.
    ///
    /// # Errors
    /// Returns `SerializeError` if deserialization fails.
    fn decode(&self, bytes: &[u8]) -> Result<WalRecord, SerializeError>;
}

/// A codec that uses `bincode` with standard configuration.
///
/// This is the default codec for WAL records.
#[derive(Clone)]
pub struct BincodeCodec;

impl WalRecordCodec for BincodeCodec {
    fn encode(&self, record: &WalRecord) -> Result<Vec<u8>, SerializeError> {
        bincode::encode_to_vec(record, config::standard())
            .map_err(|e| SerializeError::SerializeError(e.to_string()))
    }

    fn decode(&self, bytes: &[u8]) -> Result<WalRecord, SerializeError> {
        let (decoded, _): (WalRecord, _) = bincode::decode_from_slice(bytes, config::standard())
            .map_err(|e| SerializeError::DeserializeError(e.to_string()))?;
        Ok(decoded)
    }
}
