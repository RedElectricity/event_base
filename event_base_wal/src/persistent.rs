use bincode::{Decode, Encode, config};
use event_base_core::error::CoreError;
use event_base_core::error::serialize::SerializeError;
use event_base_core::error::wal::WalError;
use event_base_core::wal::wal::{Wal, WalRecord, WalRecordState};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::fs;

#[derive(Serialize, Deserialize, Encode, Decode)]
struct WalStore {
    records: Vec<WalRecord>,
    delays: Vec<WalRecord>,
    id_counter: u64,
}

#[derive(Default, Clone, Encode, Decode)]
pub struct PersistentWal {
    records: Arc<Mutex<HashMap<String, WalRecord>>>,
    delays: Arc<Mutex<HashMap<String, WalRecord>>>,
    id_counter: Arc<Mutex<u64>>,
    file_path: String,
}

impl PersistentWal {
    pub async fn new(file_path: String) -> Self {
        let path = PathBuf::from(file_path.clone());
        if path.exists() {
            let data = fs::read(&path).await;
            let (store, _): (WalStore, _) =
                bincode::decode_from_slice(&data.unwrap().as_slice(), config::standard()).unwrap();
            Self {
                records: Arc::new(Mutex::new(
                    store
                        .records
                        .into_iter()
                        .map(|r| (r.message.id.clone(), r))
                        .collect(),
                )),
                delays: Arc::new(Mutex::new(Default::default())),
                id_counter: Arc::new(Mutex::new(store.id_counter)),
                file_path: file_path.clone(),
            }
        } else {
            Self::default()
        }
    }
}

#[async_trait::async_trait]
impl Wal for PersistentWal {
    async fn append(&mut self, mut record: WalRecord) -> Result<(), CoreError> {
        let mut counter = self.id_counter.lock().unwrap();
        *counter += 1;
        record.record_id = *counter;
        let mut records = self.records.lock().unwrap();
        records.insert(record.message.id.clone(), record);
        Ok(())
    }

    async fn update_state(
        &mut self,
        message_id: &str,
        status: WalRecordState,
    ) -> Result<(), CoreError> {
        let mut records = self.records.lock().unwrap();
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
        let records = self.records.lock().unwrap();
        let pendings = records
            .values()
            .filter(|x| x.status == WalRecordState::Pending)
            .cloned()
            .collect();
        Ok(pendings)
    }

    async fn flush(&mut self) -> Result<(), CoreError> {
        let store = WalStore {
            records: self
                .records
                .clone()
                .lock()
                .unwrap()
                .values()
                .cloned()
                .collect(),
            delays: self
                .delays
                .clone()
                .lock()
                .unwrap()
                .values()
                .cloned()
                .collect(),
            id_counter: self.id_counter.clone().lock().unwrap().clone(),
        };
        let bytes = bincode::encode_to_vec(&store, config::standard())
            .map_err(|e| SerializeError::SerializeError(e.to_string()))?;
        let temp_path = PathBuf::from(&self.file_path).with_extension("tmp");
        fs::write(&temp_path, bytes).await?;
        fs::rename(&temp_path, &self.file_path).await?;
        Ok(())
    }

    async fn schedule(&self, mut record: WalRecord) -> Result<(), CoreError> {
        let mut counter = self.id_counter.lock().unwrap();
        *counter += 1;
        record.record_id = *counter;
        let mut records = self.delays.lock().unwrap();
        records.insert(record.message.id.clone(), record);
        Ok(())
    }

    async fn fetch_ready(&self, now: SystemTime) -> Result<Vec<WalRecord>, CoreError> {
        let mut delays = self.delays.lock().unwrap();
        let mut ready = Vec::new();

        for (msg_id, delayed) in delays.clone().drain() {
            if delayed.message.deliver_at <= Option::from(SystemTime::now()) {
                ready.push(delayed.clone());
                delays.remove(&msg_id);
            }
        }
        Ok(ready)
    }

    async fn remove_scheduled(&self, msg_id: &str) -> Result<(), CoreError> {
        let mut store = self.delays.lock().unwrap();
        store.remove(msg_id);
        Ok(())
    }
}
