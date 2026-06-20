use crate::error::CoreError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

static WORKER_REGISTRY: OnceLock<Arc<WorkerRegistry>> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub worker_name: String,
    pub topic: String,
    pub last_heartbeat: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerHeartbeatMessage {
    pub worker_name: String,
    pub timestamp: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerDiscoveryMessage {
    pub worker_name: String,
    pub topic: String,
    pub started_at: SystemTime,
}

pub struct WorkerRegistry {
    workers: RwLock<HashMap<String, WorkerInfo>>,
}

impl WorkerRegistry {
    pub fn init() -> Result<(), CoreError> {
        let registry = Arc::new(WorkerRegistry {
            workers: RwLock::new(HashMap::new()),
        });
        WORKER_REGISTRY
            .set(registry)
            .map_err(|_| CoreError::AlreadyInitialized)?;
        Ok(())
    }

    pub fn global() -> Arc<WorkerRegistry> {
        WORKER_REGISTRY
            .get()
            .expect("WorkerRegistry is not initialized")
            .clone()
    }

    pub async fn register(&self, info: WorkerInfo) -> Result<(), CoreError> {
        let mut workers = self.workers.write().await;
        workers.insert(info.clone().worker_name, info);
        Ok(())
    }

    pub async fn unregister(&self, worker_id: &str) -> Result<(), CoreError> {
        let mut workers = self.workers.write().await;
        workers.remove(worker_id);
        Ok(())
    }

    pub async fn heartbeat(&self, worker_id: &str) -> Result<(), CoreError> {
        let mut workers = self.workers.write().await;
        if let Some(info) = workers.get_mut(worker_id) {
            info.last_heartbeat = SystemTime::now();
        }
        Ok(())
    }

    pub async fn get_workers(&self, topic: &str) -> Result<Vec<WorkerInfo>, CoreError> {
        let workers = self.workers.read().await;
        let list_workers = workers
            .values()
            .filter(|info| info.topic == topic)
            .cloned()
            .collect();
        Ok(list_workers)
    }

    pub async fn get_all_workers(&self) -> Result<Vec<WorkerInfo>, CoreError> {
        let workers = self.workers.read().await;
        Ok(workers.values().cloned().collect())
    }

    pub async fn cleanup_stale(&self, timeout: Duration) -> Result<Vec<String>, CoreError> {
        let now = SystemTime::now();
        let mut workers = self.workers.write().await;

        let stable: Vec<String> = workers
            .iter()
            .filter(|(_, info)| {
                let elapsed = now
                    .duration_since(info.last_heartbeat)
                    .unwrap_or(Duration::MAX);

                elapsed > timeout
            })
            .map(|(id, _)| id.clone())
            .collect();

        for worker_id in &stable {
            workers.remove(worker_id);
        }

        Ok(stable)
    }
}
