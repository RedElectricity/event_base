use async_trait::async_trait;
use event_base_core::handler::Ack;
use event_base_core::message::EMessage;
use event_base_core::middleware::{Middleware, Next};

pub struct LoggerMiddleware;

#[async_trait]
impl Middleware for LoggerMiddleware {
    async fn handle(&self, msg: &mut EMessage, next: Next<'_>) -> Ack {
        let start = std::time::Instant::now();
        tracing::debug!("[{}] Processing message: {}", msg.topic.0, msg.id);
        let ack = next.run(msg).await;
        tracing::debug!("[{}] Done in {:?}: {:?}", msg.topic.0, start.elapsed(), ack);
        ack
    }
}
