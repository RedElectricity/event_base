use std::sync::Arc;
use crate::queues::EConsumer;

pub trait ConsumerFactory: Send + Sync {
    fn create_consumer(&self) -> Box<dyn EConsumer>;

    fn clone_factory(&self) -> Arc<dyn ConsumerFactory>;
}