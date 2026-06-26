use crate::error::CoreError;
use crate::queues::consumer_factory::ConsumerFactory;
use crate::queues::{EConsumer, EProducer};
use std::sync::Arc;
use tokio::sync::Mutex;

#[async_trait::async_trait]
pub trait QueueFactory: Send + Sync {
    fn create_queue(
        &self,
        topic: &str,
    ) -> Result<(Arc<dyn EProducer>, Arc<dyn ConsumerFactory>), CoreError>;

    fn create_global_producer(&self) -> Result<Arc<dyn EProducer>, CoreError>;
    fn create_main_consumer(&self) -> Result<Arc<Mutex<dyn EConsumer>>, CoreError>;

    fn name(&self) -> &'static str;

    async fn health_check(&self) -> Result<(), CoreError> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), CoreError> {
        Ok(())
    }
}
