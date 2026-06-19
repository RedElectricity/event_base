use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, Encode, Decode)]
pub struct EMessage {
    pub id: String, // Restrict to uuid
    pub topic: MessageTopic,
    pub payload: MessagePayload,
    pub metadata: MessageMetadata,
    pub attempts: u32,
    pub delivery_mode: DeliveryMode,
    pub consumed_count: u32,
    pub deliver_at: Option<SystemTime>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Encode, Decode, PartialEq, Eq)]
pub enum DeliveryMode {
    Standard,
    Repeated(u32), // 需要 N 个不同 Worker 消费
    Broadcast,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode, Default)]
pub struct MessageTopic(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Encode, Decode, Default)]
pub struct MessagePayload(pub Vec<u8>);

#[derive(Clone, Debug, Serialize, Deserialize, Encode, Decode)]
pub struct MessageMetadata {
    pub created_at: SystemTime,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub source: Option<String>,
}

impl EMessage {
    pub fn new(topic: MessageTopic, payload: MessagePayload, delivery_mode: DeliveryMode) -> Self {
        EMessage {
            id: Uuid::new_v4().to_string(),
            topic,
            payload,
            metadata: MessageMetadata {
                created_at: SystemTime::now(),
                trace_id: None,
                correlation_id: None,
                causation_id: None,
                source: None,
            },
            attempts: 0,
            delivery_mode,
            consumed_count: 0,
            deliver_at: None,
        }
    }

    fn increment_attempts(&mut self) {
        self.attempts += 1;
    }
}

impl Default for EMessage {
    fn default() -> Self {
        Self {
            id: "".to_string(),
            topic: Default::default(),
            payload: Default::default(),
            metadata: MessageMetadata {
                created_at: SystemTime::now(),
                trace_id: None,
                correlation_id: None,
                causation_id: None,
                source: None,
            },
            attempts: 0,
            delivery_mode: DeliveryMode::Standard,
            consumed_count: 0,
            deliver_at: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_creation() {
        let new_topic = MessageTopic("topic".to_owned());
        let new_payload = MessagePayload("text".as_bytes().to_vec());
        let mut msg = EMessage::new(new_topic.clone(), new_payload, DeliveryMode::Standard);
        assert_eq!(msg.topic, new_topic.clone());
        assert_eq!(msg.attempts, 0);

        msg.increment_attempts();
        assert_eq!(msg.attempts, 1);
    }
}
