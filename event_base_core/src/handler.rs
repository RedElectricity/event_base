//! Handler trait and acknowledgment types for message processing.
//!
//! This module defines the core `EHandler` trait and the `Ack` enumeration
//! used to signal how a message should be handled after processing.

pub use crate::dead_letter::DeadReason;
use crate::message::EMessage;
use async_trait::async_trait;
use std::time::Duration;

/// The acknowledgment result returned by a message handler.
///
/// It indicates whether the message was successfully processed, should be
/// retried later, or moved to the dead letter queue.
#[derive(Debug)]
pub enum Ack {
    /// The message was successfully processed and can be acknowledged
    Ack,

    /// The message could not be processed but may succeed later.
    ///
    /// It will be re-queued for a retry according to the specified parameters.
    NoAck {
        /// Optional delay before the next retry attempt.
        ///
        /// If `None`, the system may use a default backoff strategy.
        retry_after: Option<Duration>,
        /// Maximum number of retry attempts allowed.
        ///
        /// If the retry count exceeds this value, the message may be moved
        /// to the dead letter queue automatically.
        max_retries: u32,
    },
    /// The message cannot be processed and should be moved to the dead letter
    /// queue immediately.
    ///
    /// This is typically used for irrecoverable errors or invalid messages.
    Dead {
        /// The reason for moving the message to the dead letter queue.
        dead_reason: DeadReason,
    },
}

/// A trait for asynchronous message handlers.
///
/// Types implementing `EHandler` can process messages of type [`EMessage`]
/// and return an [`Ack`] to indicate the outcome.
///
/// # Examples
///
/// ```
/// # use event_base::core::{EHandler, EMessage, Ack, DeadReason};
/// # use async_trait::async_trait;
/// # use std::time::Duration;
/// struct MyHandler;
///
/// #[async_trait]
/// impl EHandler for MyHandler {
///     async fn handler(&self, msg: &EMessage) -> Ack {
///         // Process the message...
///         if msg.payload().is_empty() {
///             Ack::Dead { dead_reason: DeadReason::InvalidPayload }
///         } else {
///             Ack::Ack
///         }
///     }
/// }
/// ```
#[async_trait]
pub trait EHandler: Send + Sync {
    /// Processes a single message asynchronously and returns an acknowledgment.
    ///
    /// # Parameters
    /// - `msg`: A reference to the incoming [`EMessage`] to be processed.
    ///
    /// # Returns
    /// An [`Ack`] variant indicating whether the message is acknowledged,
    /// should be retried, or moved to the dead letter queue.
    async fn handler(&self, msg: &EMessage) -> Ack;
}

/// Automatic implementation of [`EHandler`] for `Box<dyn EHandler>`.
///
/// This allows a boxed handler to be used wherever a trait object is expected,
/// delegating the call to the inner handler.
#[async_trait]
impl EHandler for Box<dyn EHandler> {
    async fn handler(&self, msg: &EMessage) -> Ack {
        (**self).handler(msg).await
    }
}
