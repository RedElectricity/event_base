use bincode::config;
use crate::error::CoreError;
use crate::wal::wal::WalRecord;

pub trait WalRecordCodec: Send + Sync + Clone + 'static {
    fn encode(&self, record: &WalRecord) -> Result<Vec<u8>, CoreError>;
    fn decode(&self, bytes: &[u8]) -> Result<WalRecord, CoreError>;
}

#[derive(Clone)]
pub struct BincodeCodec;

impl WalRecordCodec for BincodeCodec {
    fn encode(&self, record: &WalRecord) -> Result<Vec<u8>, CoreError> {
        bincode::encode_to_vec(record, config::standard()).map_err(|e| CoreError::SerializeError(e.to_string()))
    }

    fn decode(&self, bytes: &[u8]) -> Result<WalRecord, CoreError> {
        let (decoded, _): (WalRecord, _)= bincode::decode_from_slice(bytes, config::standard()).map_err(|e| CoreError::DeserializeError(e.to_string()))?;
        Ok(decoded)
    }
}