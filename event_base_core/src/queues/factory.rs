//! Queue factory abstraction for creating producers, consumers, and topics.
//!
//! The [`QueueFactory`] trait provides a pluggable way to create queue-related
//! resources for a given topic, including a producer, a consumer factory, and
//! global producer and consumer instances.

use crate::error::CoreError;
use crate::queues::consumer_factory::ConsumerFactory;
use crate::queues::{EConsumer, EProducer};
use std::sync::Arc;
use tokio::sync::Mutex;

/// A factory that creates producers and consumers for message queues.
///
/// Implementations are responsible for setting up the underlying messaging
/// infrastructure (e.g., Redis Streams, Kafka, in‑memory) and returning
/// appropriate producer/consumer instances.
#[async_trait::async_trait]
pub trait QueueFactory: Send + Sync {
    /// Creates a producer and a consumer factory for a given topic.
    ///
    /// The producer can be used to send messages to this topic, while the
    /// consumer factory creates consumers that pull messages from the topic.
    ///
    /// # Arguments
    /// * `topic` - The name of the topic/queue.
    ///
    /// # Returns
    /// A tuple of `(Arc<dyn EProducer>, Arc<dyn ConsumerFactory>)`.
    ///
    /// # Errors
    /// Returns `CoreError` if the topic cannot be created or configured.
    fn create_queue(
        &self,
        topic: &str,
    ) -> Result<(Arc<dyn EProducer>, Arc<dyn ConsumerFactory>), CoreError>;

    /// Creates a global producer that can send messages to any topic.
    ///
    /// This is typically used for system‑wide messages (e.g., audits, traces).
    ///
    /// # Errors
    /// Returns `CoreError` if the producer cannot be created.
    fn create_global_producer(&self) -> Result<Arc<dyn EProducer>, CoreError>;

    /// Creates the main consumer that listens for messages from all topics.
    ///
    /// This consumer is used by the [`ConsumerRouter`](consumer_router::ConsumerRouter)
    /// to claim messages and dispatch them to workers.
    ///
    /// # Errors
    /// Returns `CoreError` if the consumer cannot be created.
    fn create_main_consumer(&self) -> Result<Arc<Mutex<dyn EConsumer>>, CoreError>;

    /// Returns the name of this factory (e.g., "redis", "memory").
    fn name(&self) -> &'static str;

    /// Performs a health check on the underlying messaging system.
    ///
    /// The default implementation always returns `Ok(())`.
    ///
    /// # Errors
    /// Returns `CoreError` if the system is unhealthy.
    async fn health_check(&self) -> Result<(), CoreError> {
        Ok(())
    }

    /// Gracefully shuts down the factory and releases resources.
    ///
    /// The default implementation does nothing.
    ///
    /// # Errors
    /// Returns `CoreError` if shutdown fails.
    async fn shutdown(&self) -> Result<(), CoreError> {
        Ok(())
    }
}
