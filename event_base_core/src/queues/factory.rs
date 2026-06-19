use crate::error::CoreError;
use crate::queues::EProducer;
use crate::queues::consumer_factory::ConsumerFactory;
use std::sync::Arc;

#[async_trait::async_trait]
pub trait QueueFactory: Send + Sync {
    fn create_queue(
        &self,
        topic: &str,
    ) -> Result<(Arc<dyn EProducer>, Arc<dyn ConsumerFactory>), CoreError>;

    fn name(&self) -> &'static str;

    async fn health_check(&self) -> Result<(), CoreError> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), CoreError> {
        Ok(())
    }
}
