//! Queue abstraction layer providing producers, consumers, and a router.
//!
//! This module defines the core traits [`EProducer`] and [`EConsumer`] for
//! interacting with message queues, along with the [`ClaimedMessage`] structure
//! used for message claiming. The [`consumer_router`] submodule provides a
//! routing layer that dispatches messages to local workers.

pub mod consumer_factory;
pub mod consumer_router;
pub mod factory;

use crate::error::CoreError;
use crate::message::EMessage;
use async_trait::async_trait;
use std::time::{Duration, SystemTime};

/// A producer that can send messages to a queue.
///
/// Implementations provide various send strategies: async send, try-send (non‑blocking),
/// and send with a timeout.
#[async_trait]
pub trait EProducer: Send + Sync {
    /// Asynchronously sends a message to the queue.
    ///
    /// # Errors
    /// Returns `CoreError` if the queue is full, unavailable, or an I/O error occurs.
    async fn send(&self, msg: EMessage) -> Result<(), CoreError>;

    /// Attempts to send a message without blocking.
    ///
    /// If the queue is full or the operation would block, it returns an error immediately.
    ///
    /// # Errors
    /// Returns `CoreError` if the queue is full or the operation cannot be performed
    /// without blocking.
    async fn try_send(&self, msg: EMessage) -> Result<(), CoreError>;

    /// Sends a message with a timeout.
    ///
    /// If to send cannot complete within the specified duration, it returns an error.
    ///
    /// # Errors
    /// Returns `CoreError` on timeout or other send failures.
    async fn send_timeout(&self, msg: EMessage, timeout: Duration) -> Result<(), CoreError>;
}

/// A consumer that can receive and claim messages from a queue.
///
/// Consumers can receive raw messages or claim them with a unique claim ID for
/// explicit acknowledgment (ack) or negative acknowledgment (nack).
#[async_trait]
pub trait EConsumer: Send + Sync {
    /// Receives the next message from the queue, blocking until one is available.
    ///
    /// This is a simplified reception method that does not support claiming/acking.
    /// For finer control, use [`claim`](Self::claim).
    async fn receive(&mut self) -> Option<EMessage>;

    /// Claims the next available message, returning a [`ClaimedMessage`] with a
    /// unique claim ID.
    ///
    /// If no message is immediately available, it returns `Ok(None)`.
    ///
    /// # Errors
    /// Returns `CoreError` if the underlying queue operation fails.
    async fn claim(&mut self) -> Result<Option<ClaimedMessage>, CoreError>;

    /// Claims up to `max` messages in a single batch, reducing lock contention
    /// on the internal pending map. The default implementation calls [`claim`](Self::claim)
    /// in a loop; queue backends SHOULD override with an optimized version that
    /// acquires the pending lock once for the entire batch.
    ///
    /// Returns however many messages were available (0 ..= `max`), never an error
    /// for an empty queue.
    async fn claim_batch(&mut self, max: usize) -> Result<Vec<ClaimedMessage>, CoreError> {
        let mut batch = Vec::with_capacity(max);
        for _ in 0..max {
            match self.claim().await {
                Ok(Some(msg)) => batch.push(msg),
                Ok(None) => break,
                Err(e) => return Err(e),
            }
        }
        Ok(batch)
    }

    /// Acknowledges the successful processing of a claimed message.
    ///
    /// This removes the message from the queue or marks it as processed.
    ///
    /// # Errors
    /// Returns `CoreError` if the claim ID is invalid or the operation fails.
    async fn ack(&mut self, claim_id: &str) -> Result<(), CoreError>;

    /// Negatively acknowledges a claimed message, indicating it should be retried
    /// or returned to the queue.
    ///
    /// # Errors
    /// Returns `CoreError` if the claim ID is invalid or the operation fails.
    async fn nack(&mut self, claim_id: &str) -> Result<(), CoreError>;

    /// Re‑claims entries that were delivered to (claimed by) some consumer but
    /// never acknowledged within `min_idle` — typically because the claiming
    /// consumer or its whole node crashed mid‑flight.
    ///
    /// Only durable shared backends can do this (Redis: `XAUTOCLAIM` over the
    /// consumer‑group PEL; the reclaimed entries are moved to *this* consumer).
    /// In‑memory backends keep the default no‑op: their pending state lives in
    /// the process, so a crash loses it anyway and the WAL/replay is the
    /// recovery path.
    ///
    /// The [`ConsumerRouter`](super::consumer_router::ConsumerRouter) dispatch
    /// loop calls this periodically, so a dead node's half‑processed messages
    /// get re‑driven by the fleet instead of rotting in the pending list.
    ///
    /// # Errors
    /// Returns `CoreError` if the backend operation fails.
    async fn claim_stale(&mut self, _min_idle: Duration) -> Result<Vec<ClaimedMessage>, CoreError> {
        Ok(Vec::new())
    }
}

/// A message that has been claimed from a queue, along with its claim identifier
/// and timestamp.
#[derive(Debug, Clone)]
pub struct ClaimedMessage {
    /// The actual message payload.
    pub message: EMessage,
    /// Unique identifier for this claim, used for ack/nack.
    pub claim_id: String,
    /// Timestamp when the message was claimed.
    pub claimed_at: SystemTime,
}
