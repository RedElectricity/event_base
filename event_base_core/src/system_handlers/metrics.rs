use crate::handler::{Ack, EHandler};
use crate::message::EMessage;
use crate::metrics::node::NodeMetrics;
use crate::metrics::node_store::MetricsStore;
use async_trait::async_trait;

pub struct MetricsHandler {}

#[async_trait]
impl EHandler for MetricsHandler {
    async fn handler(&self, msg: &EMessage) -> Ack {
        let info: NodeMetrics =
            match serde_json::from_slice::<NodeMetrics>(msg.payload.0.as_slice()) {
                Ok(msg) => msg,
                Err(e) => {
                    eprintln!("[METRICS]Failed to deserialize NodeMetrics: {}", e);
                    return Ack::Ack;
                }
            };

        MetricsStore::global().update(info.clone()).await;

        Ack::Ack
    }
}
