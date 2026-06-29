//! Factory trait for creating consumers.
//!
//! This module defines the [`ConsumerFactory`] trait, which is used to create
//! instances of [`EConsumer`] for a specific topic or queue.

use crate::queues::EConsumer;
use std::sync::Arc;

/// A factory that creates `EConsumer` instances.
///
/// This trait allows the system to create new consumers on demand, typically
/// for each worker that needs to consume from a particular topic.
pub trait ConsumerFactory: Send + Sync {
    /// Creates a new boxed consumer.
    ///
    /// The returned consumer should be configured for the topic that this factory
    /// was created for.
    fn create_consumer(&self) -> Box<dyn EConsumer>;

    /// Clones the factory as an `Arc<dyn ConsumerFactory>`.
    ///
    /// This is a convenience method to avoid requiring `Clone` bounds on the
    /// concrete type.
    fn clone_factory(&self) -> Arc<dyn ConsumerFactory>;
}
