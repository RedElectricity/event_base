//! Core message types used throughout the system.
//!
//! Defines the structure of an envelope (`EMessage`) including metadata,
//! delivery modes, topics, and payload.

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use uuid::Uuid;

/// The main message envelope carrying payload, routing, and metadata.
#[derive(Clone, Debug, Serialize, Deserialize, Encode, Decode)]
pub struct EMessage {
    /// Unique message identifier (should be a UUID string).
    pub id: String,
    /// Topic to which this message belongs.
    pub topic: MessageTopic,
    /// Actual payload as a byte vector.
    pub payload: MessagePayload,
    /// Metadata about the message (creation time, tracing, etc.).
    pub metadata: MessageMetadata,
    /// Number of processing attempts so far.
    pub attempts: u32,
    /// Delivery mode: standard, repeated (with count), or broadcast.
    pub delivery_mode: DeliveryMode,
    /// Number of times this message has been consumed (for repeated mode).
    pub consumed_count: u32,
    /// Optional timestamp after which the message should be delivered (for delayed delivery).
    pub deliver_at: Option<SystemTime>,
    /// Optional specific worker to route this message to.
    pub to_worker: Option<String>,
    /// Version of the message schema.
    pub version: u32,
}

/// Delivery mode for a message.
#[derive(Clone, Debug, Serialize, Deserialize, Encode, Decode, PartialEq, Eq)]
pub enum DeliveryMode {
    /// Standard single delivery.
    Standard,
    /// Repeated delivery exactly `u32` times.
    Repeated(u32),
    /// Broadcast to all workers subscribed to the topic.
    Broadcast,
}

/// A topic identifier (wrapper over `String`).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode, Default)]
pub struct MessageTopic(pub String);

impl From<&str> for MessageTopic {
    fn from(s: &str) -> Self {
        MessageTopic(s.to_string())
    }
}

impl From<String> for MessageTopic {
    fn from(s: String) -> Self {
        MessageTopic(s)
    }
}

/// Opaque payload as a byte vector.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Encode, Decode, Default)]
pub struct MessagePayload(pub Vec<u8>);

/// Metadata associated with a message.
#[derive(Clone, Debug, Serialize, Deserialize, Encode, Decode)]
pub struct MessageMetadata {
    /// Timestamp when the message was created.
    pub created_at: SystemTime,
    /// Distributed tracing trace ID.
    pub trace_id: Option<String>,
    /// Correlation ID for linking related messages.
    pub correlation_id: Option<String>,
    /// Causation ID for identifying the message that caused this one.
    pub causation_id: Option<String>,
    /// Source system or component that produced the message.
    pub source: Option<String>,
}

impl EMessage {
    /// Creates a new message with a generated UUID and current timestamp.
    ///
    /// # Arguments
    /// * `topic` - The topic for this message.
    /// * `payload` - The payload data.
    /// * `delivery_mode` - How the message should be delivered.
    /// * `to_worker` - Optional specific worker to target.
    pub fn new(
        topic: MessageTopic,
        payload: MessagePayload,
        delivery_mode: DeliveryMode,
        to_worker: Option<String>,
    ) -> Self {
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
            to_worker,
            version: 0,
        }
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
            to_worker: None,
            version: 0,
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
        let msg = EMessage::new(new_topic.clone(), new_payload, DeliveryMode::Standard, None);
        assert_eq!(msg.topic, new_topic.clone());
        assert_eq!(msg.attempts, 0);
    }
}
