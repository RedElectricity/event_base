use event_base_core::error::CoreError;
use event_base_core::error::wal::WalError;
use event_base_core::wal::wal::{Wal, WalRecord, WalRecordState};
use event_base_core::worker_registry::WorkerInfo;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{Mutex, RwLock};

#[derive(Default, Clone)]
pub struct MemoryWal {
    records: Arc<RwLock<HashMap<String, WalRecord>>>,
    delays: Arc<RwLock<HashMap<String, WalRecord>>>,
    worker_registry: Arc<RwLock<HashMap<String, WorkerInfo>>>,
    id_counter: Arc<Mutex<u64>>,
}

impl MemoryWal {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl Wal for MemoryWal {
    async fn append(&mut self, mut record: WalRecord) -> Result<(), CoreError> {
        let mut counter = self.id_counter.lock().await;
        *counter += 1;
        record.record_id = *counter;
        let mut records = self.records.write().await;
        records.insert(record.message.id.clone(), record);
        Ok(())
    }

    async fn update_state(
        &mut self,
        message_id: &str,
        status: WalRecordState,
    ) -> Result<(), CoreError> {
        let mut records = self.records.write().await;
        if let Some(record) = records.get_mut(message_id) {
            record.status = status;
            Ok(())
        } else {
            Err(CoreError::from(WalError::RecordNotFound(
                message_id.to_string(),
            )))
        }
    }

    async fn replay_pending(&mut self) -> Result<Vec<WalRecord>, CoreError> {
        let records = self.records.read().await;
        let pendings = records
            .values()
            .filter(|x| x.status == WalRecordState::Pending)
            .cloned()
            .collect();
        Ok(pendings)
    }

    async fn flush(&mut self) -> Result<(), CoreError> {
        Ok(())
    }

    async fn schedule(&self, mut record: WalRecord) -> Result<(), CoreError> {
        let mut counter = self.id_counter.lock().await;
        *counter += 1;
        record.record_id = *counter;
        let mut records = self.delays.write().await;
        records.insert(record.message.id.clone(), record);
        Ok(())
    }

    async fn fetch_ready(&self) -> Result<Vec<WalRecord>, CoreError> {
        let mut delays = self.delays.write().await;
        let now = SystemTime::now();

        let mut ready = Vec::new();

        let mut to_remove = Vec::new();
        for (msg_id, delayed) in delays.iter() {
            match &delayed.message.deliver_at {
                Some(deliver_at) if deliver_at <= &now => {
                    ready.push(delayed.clone());
                    to_remove.push(msg_id.clone());
                }
                _ => {}
            }
        }

        for id in to_remove {
            delays.remove(&id);
        }
        Ok(ready)
    }

    async fn remove_scheduled(&self, msg_id: &str) -> Result<(), CoreError> {
        let mut store = self.delays.write().await;
        store.remove(msg_id);
        Ok(())
    }

    async fn save_worker_registry(
        &self,
        registry: HashMap<String, WorkerInfo>,
    ) -> Result<(), CoreError> {
        let mut store = self.worker_registry.write().await;
        *store = registry;
        Ok(())
    }

    async fn load_worker_registry(&self) -> Result<HashMap<String, WorkerInfo>, CoreError> {
        let store = self.worker_registry.write().await;
        Ok(store.clone())
    }
}
