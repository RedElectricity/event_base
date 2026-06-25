use crate::NodeType::Host;
use crate::constant::SYSTEM_TOPIC_TOPIC_SYNC;
use crate::get_node_type;
use crate::handler::{Ack, EHandler};
use crate::message::DeliveryMode::Standard;
use crate::message::{EMessage, MessagePayload, MessageTopic};
use crate::topic::TopicRouter;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicDiscoveryMessage {
    pub has_topics: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicSyncMessage {
    pub topics: Vec<String>,
}

pub struct TopicDiscovery {}

#[async_trait]
impl EHandler for TopicDiscovery {
    async fn handle(&self, msg: &EMessage) -> Ack {
        let topics: TopicDiscoveryMessage = match serde_json::from_slice(&msg.payload.0) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed to deserialize audit record: {}", e);
                return Ack::Ack;
            }
        };

        let topic_router = TopicRouter::global();
        let topic_list = topic_router.list_topics().await;

        for item in topics.has_topics {
            if !topic_list.contains(&item) {
                topic_router.register_topic(&*item).await;
            }
        }

        let topics_sync_msg = EMessage::new(
            MessageTopic(SYSTEM_TOPIC_TOPIC_SYNC.parse().unwrap()),
            MessagePayload(
                serde_json::to_vec(&TopicSyncMessage {
                    topics: topic_router.list_topics().await,
                })
                .unwrap(),
            ),
            Standard,
            None,
        );

        topic_router
            .send(SYSTEM_TOPIC_TOPIC_SYNC, topics_sync_msg, None, None)
            .await
            .expect("[TOPIC DISCOVERY] Failed to send topic sync message");

        Ack::Ack
    }
}

pub struct TopicSync {}

#[async_trait]
impl EHandler for TopicSync {
    async fn handle(&self, msg: &EMessage) -> Ack {
        if get_node_type() == Arc::from(Host) {
            return Ack::Ack;
        }
        let topics: TopicSyncMessage = match serde_json::from_slice(&msg.payload.0) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed to deserialize audit record: {}", e);
                return Ack::Ack;
            }
        };

        let topic_router = TopicRouter::global();
        let topic_list = topic_router.list_topics().await;

        for item in topics.topics {
            if !topic_list.contains(&item) {
                topic_router.register_topic(&*item).await;
            }
        }

        Ack::Ack
    }
}
