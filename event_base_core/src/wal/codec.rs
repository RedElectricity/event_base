use crate::error::CoreError;
use crate::error::serialize::SerializeError;
use crate::wal::wal::WalRecord;
use bincode::config;

pub trait WalRecordCodec: Send + Sync + Clone + 'static {
    fn encode(&self, record: &WalRecord) -> Result<Vec<u8>, SerializeError>;
    fn decode(&self, bytes: &[u8]) -> Result<WalRecord, SerializeError>;
}

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
