//! Registry of active workers, their topics, and heartbeats.
//!
//! The registry persists worker information via the WAL and provides methods
//! for registration, heartbeat updates, and cleanup of stale workers.

use crate::error::CoreError;
use crate::wal::wal::Wal;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

static WORKER_REGISTRY: OnceLock<RwLock<WorkerRegistry>> = OnceLock::new();

/// Information about a registered worker.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct WorkerInfo {
    /// Unique name of the worker.
    pub worker_name: String,
    /// Topic this worker handles.
    pub topic: String,
    /// Timestamp of the last heartbeat.
    pub last_heartbeat: SystemTime,
}

/// Message format for worker heartbeat.
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct WorkerHeartbeatMessage {
    pub worker_name: String,
    pub timestamp: SystemTime,
}

/// Message format for worker discovery (registration).
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct WorkerDiscoveryMessage {
    pub worker_name: String,
    pub topic: String,
    pub started_at: SystemTime,
}

/// The global worker registry.
pub struct WorkerRegistry {
    workers: RwLock<HashMap<String, WorkerInfo>>,
    wal: Option<Arc<RwLock<Box<dyn Wal>>>>,
}

impl WorkerRegistry {
    /// Initializes the global registry, loading persisted data from the WAL.
    ///
    /// # Errors
    /// Returns `CoreError` if loading fails or if already initialized.
    pub async fn init(wal: Option<Arc<RwLock<Box<dyn Wal>>>>) -> Result<(), CoreError> {
        let wr = wal
            .clone()
            .unwrap() // PANIC SAFETY: WAL is critical for data integrity. Failing fast is preferred.
            .read()
            .await
            .load_worker_registry()
            .await?;
        let registry = WorkerRegistry {
            wal,
            workers: RwLock::from(wr),
        };
        WORKER_REGISTRY
            .set(RwLock::new(registry))
            .map_err(|_| CoreError::AlreadyInitialized)?;
        Ok(())
    }

    /// Returns a reference to the global worker registry.
    ///
    /// # Panics
    /// Panics if the registry is not initialized.
    pub fn global() -> &'static RwLock<WorkerRegistry> {
        WORKER_REGISTRY
            .get()
            .expect("WorkerRegistry is not initialized")
    }

    /// Returns a reference to the global WAL, if one was provided during init.
    pub fn wal(&self) -> Option<Arc<RwLock<Box<dyn Wal>>>> {
        self.wal.clone()
    }

    /// Registers a new worker or updates an existing one.
    ///
    /// # Errors
    /// Returns `CoreError` if persistence fails.
    pub async fn register(&self, info: WorkerInfo) -> Result<(), CoreError> {
        let mut workers = self.workers.write().await;
        workers.insert(info.clone().worker_name, info);
        drop(workers);
        self.save_worker_registry().await?;
        Ok(())
    }

    /// Removes a worker by name.
    ///
    /// # Errors
    /// Returns `CoreError` if persistence fails.
    pub async fn unregister(&self, worker_id: &str) -> Result<(), CoreError> {
        let mut workers = self.workers.write().await;
        workers.remove(worker_id);
        drop(workers);
        self.save_worker_registry().await?;
        Ok(())
    }

    /// Updates the heartbeat timestamp for a given worker.
    ///
    /// # Errors
    /// Returns `CoreError` if the worker does not exist or persistence fails.
    pub async fn heartbeat(&self, worker_id: &str) -> Result<(), CoreError> {
        let mut workers = self.workers.write().await;
        if let Some(info) = workers.get_mut(worker_id) {
            info.last_heartbeat = SystemTime::now();
        }
        drop(workers);

        self.save_worker_registry().await?;

        Ok(())
    }

    /// Returns all workers subscribed to a given topic.
    ///
    /// # Errors
    /// Returns `CoreError` on lock failure (unlikely).
    pub async fn get_workers(&self, topic: &str) -> Result<Vec<WorkerInfo>, CoreError> {
        let workers = self.workers.read().await;
        let list_workers = workers
            .values()
            .filter(|info| info.topic == topic)
            .cloned()
            .collect();
        Ok(list_workers)
    }

    /// Returns all registered workers.
    ///
    /// # Errors
    /// Returns `CoreError` on lock failure.
    pub async fn get_all_workers(&self) -> Result<Vec<WorkerInfo>, CoreError> {
        let workers = self.workers.read().await;
        Ok(workers.values().cloned().collect())
    }

    /// Removes workers whose last heartbeat is older than the given timeout.
    ///
    /// Returns the list of removed worker IDs.
    ///
    /// # Errors
    /// Returns `CoreError` if persistence fails.
    pub async fn cleanup_stale(&self, timeout: Duration) -> Result<Vec<String>, CoreError> {
        let now = SystemTime::now();
        let mut workers = self.workers.write().await;

        let stale: Vec<String> = workers
            .iter()
            .filter(|(_, info)| {
                let elapsed = now
                    .duration_since(info.last_heartbeat)
                    .unwrap_or(Duration::MAX);

                elapsed > timeout
            })
            .map(|(id, _)| id.clone())
            .collect();

        for worker_id in &stale {
            workers.remove(worker_id);
        }

        drop(workers);

        self.save_worker_registry().await?;

        Ok(stale)
    }

    async fn save_worker_registry(&self) -> Result<(), CoreError> {
        let workers = self.workers.read().await;
        self.wal
            .clone()
            .unwrap()
            .write()
            .await
            .save_worker_registry(workers.clone())
            .await
    }
}
