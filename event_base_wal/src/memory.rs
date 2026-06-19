use event_base_core::error::CoreError;
use event_base_core::wal::wal::{Wal, WalRecord, WalRecordState};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use event_base_core::error::wal::WalError;

#[derive(Default, Clone)]
pub struct MemoryWal {
    records: Arc<Mutex<HashMap<String, WalRecord>>>,
    delays: Arc<Mutex<HashMap<String, WalRecord>>>,
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
            Err(CoreError::from(WalError::RecordNotFound(message_id.to_string())))
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

    async fn fetch_ready(&self, _now: SystemTime) -> Result<Vec<WalRecord>, CoreError> {
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
