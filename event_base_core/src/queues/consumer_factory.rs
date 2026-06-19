use crate::queues::EConsumer;
use std::sync::Arc;

pub trait ConsumerFactory: Send + Sync {
    fn create_consumer(&self) -> Box<dyn EConsumer>;

    fn clone_factory(&self) -> Arc<dyn ConsumerFactory>;
}
