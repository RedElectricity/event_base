use crate::error::CoreError;
use crate::message::EMessage;

pub trait Codec: Send + Sync {
    fn encode(&self, msg: &EMessage) -> Result<Vec<u8>, CoreError>;
    fn decode(&self, data: &[u8]) -> Result<EMessage, CoreError>;
}
