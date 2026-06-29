//! Core Write‑Ahead Log (WAL) trait and record types.
//!
//! This module defines the [`Wal`] trait for durable storage of message states,
//! along with the [`WalRecord`] structure and its [`WalRecordState`] enumeration.
//! It also provides methods for appending, updating, replaying, and scheduling
//! records, as well as persisting the worker registry.

use crate::dead_letter::DeadReason;
use crate::error::CoreError;
use crate::message::EMessage;
use crate::worker_registry::WorkerInfo;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

/// The main WAL trait for durable message persistence.
///
/// Implementations are responsible for storing and retrieving `WalRecord` s,
/// updating their states, replaying pending records, and managing scheduled
/// (delayed) messages. They also persist the worker registry.
#[async_trait]
pub trait Wal: Send + Sync {
    /// Appends a new record to the WAL.
    ///
    /// The record is stored in the `Pending` state.
    ///
    /// # Errors
    /// Returns `CoreError` if the append operation fails.
    async fn append(&mut self, record: WalRecord) -> Result<(), CoreError>;

    /// Updates the state of a message by its ID.
    ///
    /// # Errors
    /// Returns `CoreError` if the message does not exist or the update fails.
    async fn update_state(
        &mut self,
        message_id: &str,
        status: WalRecordState,
    ) -> Result<(), CoreError>;

    /// Retrieves all pending records for replay after a restart.
    ///
    /// # Errors
    /// Returns `CoreError` if reading fails.
    async fn replay_pending(&mut self) -> Result<Vec<WalRecord>, CoreError>;

    /// Forces a flush of any buffered data to persistent storage.
    ///
    /// # Errors
    /// Returns `CoreError` if the flush fails.
    async fn flush(&mut self) -> Result<(), CoreError>;

    /// Schedules a record for future delivery.
    ///
    /// The record is stored separately and will be returned by [`fetch_ready`](Self::fetch_ready)
    /// when its delivery time arrives.
    ///
    /// # Errors
    /// Returns `CoreError` if scheduling fails.
    async fn schedule(&self, record: WalRecord) -> Result<(), CoreError>;

    /// Fetches all scheduled records that are ready to be delivered
    /// (i.e., whose `deliver_at` time has passed).
    ///
    /// # Errors
    /// Returns `CoreError` if fetching fails.
    async fn fetch_ready(&self) -> Result<Vec<WalRecord>, CoreError>;

    /// Removes a scheduled record by its message ID.
    ///
    /// # Errors
    /// Returns `CoreError` if removal fails.
    async fn remove_scheduled(&self, msg_id: &str) -> Result<(), CoreError>;

    /// Saves the entire worker registry to persistent storage.
    ///
    /// # Errors
    /// Returns `CoreError` if saving fails.
    async fn save_worker_registry(
        &self,
        registry: HashMap<String, WorkerInfo>,
    ) -> Result<(), CoreError>;

    /// Loads the worker registry from persistent storage.
    ///
    /// # Errors
    /// Returns `CoreError` if loading fails.
    async fn load_worker_registry(&self) -> Result<HashMap<String, WorkerInfo>, CoreError>;
}

/// A single record in the WAL representing a message and its current state.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct WalRecord {
    /// Unique ID of this record (usually auto‑incremented).
    pub record_id: u64,
    /// The message payload and metadata.
    pub message: EMessage,
    /// Current state of the message.
    pub status: WalRecordState,
    /// Timestamp of the last processing attempt (if any).
    pub last_attempt_at: Option<SystemTime>,
    /// Whether this message has been dead‑lettered.
    pub is_dead_letter: bool,
    /// The reason for dead‑lettering, if applicable.
    pub dead_reason: Option<DeadReason>,
}

impl WalRecord {
    /// Creates a new `WalRecord` from a message, with default state `Pending`.
    ///
    /// The `record_id` is set to 0 and will be assigned by the WAL implementation
    /// upon append.
    pub fn from_msg(msg: EMessage) -> Self {
        Self {
            record_id: 0,
            message: msg,
            status: WalRecordState::Pending,
            last_attempt_at: None,
            is_dead_letter: false,
            dead_reason: None,
        }
    }
}

/// The state of a message in the WAL.
#[repr(u8)]
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Copy, Encode, Decode)]
pub enum WalRecordState {
    /// Message is pending and not yet processed.
    Pending = 0,
    /// Message is currently being processed by a worker.
    Processing = 1,
    /// Message has been successfully processed.
    Complete = 2,
    /// Message processing failed (dead‑lettered).
    Failed = 3,
}
