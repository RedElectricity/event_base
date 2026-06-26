use bincode::{Decode, Encode, config};
use event_base_core::error::CoreError;
use event_base_core::error::serialize::SerializeError::SerializeError;
use event_base_core::error::wal::WalError;
use event_base_core::error::wal::WalError::RecordNotFound;
use event_base_core::wal::wal::{Wal, WalRecord, WalRecordState};
use event_base_core::worker_registry::WorkerInfo;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::fs;
use tokio::sync::{Mutex, RwLock};

#[derive(Serialize, Deserialize, Encode, Decode)]
struct WalStore {
    records: HashMap<String, WalRecord>,
    delays: HashMap<String, WalRecord>,
    worker_registry: HashMap<String, WorkerInfo>,
    id_counter: u64,
}

#[derive(Default, Clone)]
pub struct PersistentWal {
    records: Arc<RwLock<HashMap<String, WalRecord>>>,
    delays: Arc<RwLock<HashMap<String, WalRecord>>>,
    worker_registry: Arc<RwLock<HashMap<String, WorkerInfo>>>,
    id_counter: Arc<Mutex<u64>>,
    file_path: String,
}

impl PersistentWal {
    pub async fn new(file_path: String) -> Result<Self, CoreError> {
        if !tokio::fs::try_exists(&file_path).await.unwrap_or(false) {
            // 文件不存在，初始化空存储
            return Ok(Self {
                file_path,
                records: Arc::new(Default::default()),
                delays: Arc::new(Default::default()),
                worker_registry: Arc::new(Default::default()),
                id_counter: Arc::new(Default::default()),
            });
        }

        // 1. 安全读取文件，处理IO错误
        let data = fs::read(&file_path).await.map_err(|e| {
            WalError::Backend(format!(
                "Fail to read the file: {}，path:{:?}",
                e, file_path
            ))
        })?;

        // 2. 安全反序列化，处理二进制损坏
        let (store, _): (WalStore, _) = bincode::decode_from_slice(&data, config::standard())
            .map_err(|e| WalError::Backend(format!("Fail to decode the file:{}", e)))?;

        Ok(Self {
            records: Arc::new(RwLock::new(store.records)),
            delays: Arc::new(RwLock::new(store.delays)),
            worker_registry: Arc::new(RwLock::new(store.worker_registry)),
            id_counter: Arc::new(Mutex::new(store.id_counter)),
            file_path,
        })
    }
}

#[async_trait::async_trait]
impl Wal for PersistentWal {
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
            Err(CoreError::from(RecordNotFound(message_id.to_string())))
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
        let store = WalStore {
            records: self.records.read().await.clone(),
            delays: self.delays.read().await.clone(),
            worker_registry: self.worker_registry.read().await.clone(),
            id_counter: *self.id_counter.clone().lock().await,
        };
        let bytes = bincode::encode_to_vec(&store, config::standard())
            .map_err(|e| SerializeError(e.to_string()))?;
        let temp_path = PathBuf::from(&self.file_path).with_extension("tmp");
        fs::write(&temp_path, bytes).await?;
        fs::rename(&temp_path, &self.file_path).await?;
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
        let store = self.worker_registry.read().await;
        Ok(store.clone())
    }
}
