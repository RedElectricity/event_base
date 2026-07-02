//! Handlers for topic discovery and synchronization.
//!
//! The [`TopicDiscovery`] handler responds to discovery requests by sending
//! the current list of topics back to the requester. The [`TopicSync`] handler
//! synchronizes a worker's topic list with the host's list.

use crate::NodeType::Host;
use crate::constant::SYSTEM_TOPIC_TOPIC_SYNC;
use crate::get_node_type;
use crate::handler::{Ack, EHandler};
use crate::message::DeliveryMode::Standard;
use crate::message::{EMessage, MessagePayload, MessageTopic};
use crate::topic::TopicRouter;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Message sent by a worker to discover topics from the host.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct TopicDiscoveryMessage {
    /// The list of topics the sender already knows about.
    pub has_topics: Vec<String>,
}

/// Message used to synchronize the full topic list.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct TopicSyncMessage {
    /// The full list of topics to sync.
    pub topics: Vec<String>,
}

/// Handler for topic discovery requests.
///
/// It receives a `TopicDiscoveryMessage`, merges any new topics into the local
/// router, and responds with a `TopicSyncMessage` containing the full topic list.
pub struct TopicDiscovery {}

#[async_trait]
impl EHandler for TopicDiscovery {
    async fn handler(&self, msg: &EMessage) -> Ack {
        let topics: TopicDiscoveryMessage = match bincode::decode_from_slice(&msg.payload.0, bincode::config::standard()) {
            Ok((r, _)) => r,
            Err(e) => {
                tracing::error!("Failed to deserialize topic discovery message: {}", e);
                return Ack::Ack;
            }
        };

        let topic_router = TopicRouter::global();
        let topic_list = topic_router.list_topics().await;

        for item in topics.has_topics {
            if !topic_list.contains(&item) {
                topic_router.register_topic(&item).await;
            }
        }

        let topics_sync_msg = EMessage::new(
            MessageTopic(SYSTEM_TOPIC_TOPIC_SYNC.to_string()),
            MessagePayload(
                bincode::encode_to_vec(&TopicSyncMessage {
                    topics: topic_router.list_topics().await,
                }, bincode::config::standard())
                .unwrap_or_default(),
            ),
            Standard,
            None,
        );

        if let Err(_) = topic_router
            .send(SYSTEM_TOPIC_TOPIC_SYNC, topics_sync_msg, None, None)
            .await
        {
            eprintln!("[TOPIC DISCOVERY] Failed to send topic sync message")
        }

        Ack::Ack
    }
}

/// Handler for topic synchronization.
///
/// On a worker node, it receives a `TopicSyncMessage` and registers any topics
/// that are not already present in the local router. On a host node, it does
/// nothing.
pub struct TopicSync {}

#[async_trait]
impl EHandler for TopicSync {
    async fn handler(&self, msg: &EMessage) -> Ack {
        if get_node_type() == Arc::from(Host) {
            return Ack::Ack;
        }
        let topics: TopicSyncMessage = match bincode::decode_from_slice(&msg.payload.0, bincode::config::standard()) {
            Ok((r, _)) => r,
            Err(e) => {
                tracing::error!("Failed to deserialize topic sync message: {}", e);
                return Ack::Ack;
            }
        };

        let topic_router = TopicRouter::global();
        let topic_list = topic_router.list_topics().await;

        for item in topics.topics {
            if !topic_list.contains(&item) {
                topic_router.register_topic(&item).await;
            }
        }

        Ack::Ack
    }
}
