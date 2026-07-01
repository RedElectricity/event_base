use async_trait::async_trait;
use event_base_core::error::CoreError;
use event_base_core::error::wal::WalError;
use event_base_core::message::EMessage;
use event_base_core::wal::wal::{Wal, WalRecord, WalRecordState};
use event_base_core::worker_registry::WorkerInfo;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, RwLock};

#[derive(Clone)]
pub struct RecordingProducer {
    pub sent: Arc<Mutex<Vec<EMessage>>>,
    pub try_sent: Arc<Mutex<Vec<EMessage>>>,
    pub timeout_sent: Arc<Mutex<Vec<(EMessage, Duration)>>>,
}

impl RecordingProducer {
    pub fn new() -> Self {
        Self {
            sent: Arc::new(Mutex::new(Vec::new())),
            try_sent: Arc::new(Mutex::new(Vec::new())),
            timeout_sent: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Default for RecordingProducer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl event_base_core::queues::EProducer for RecordingProducer {
    async fn send(&self, msg: EMessage) -> Result<(), CoreError> {
        self.sent.lock().await.push(msg);
        Ok(())
    }

    async fn try_send(&self, msg: EMessage) -> Result<(), CoreError> {
        self.try_sent.lock().await.push(msg);
        Ok(())
    }

    async fn send_timeout(&self, msg: EMessage, timeout: Duration) -> Result<(), CoreError> {
        self.timeout_sent.lock().await.push((msg, timeout));
        Ok(())
    }
}

#[derive(Clone)]
pub struct RecordingWal {
    pending: Arc<RwLock<HashMap<String, WalRecord>>>,
    scheduled: Arc<RwLock<HashMap<String, WalRecord>>>,
    workers: Arc<RwLock<HashMap<String, WorkerInfo>>>,
}

impl RecordingWal {
    pub fn new() -> Self {
        Self {
            pending: Arc::new(RwLock::new(HashMap::new())),
            scheduled: Arc::new(RwLock::new(HashMap::new())),
            workers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn seed_pending(&self, record: WalRecord) {
        self.pending
            .write()
            .await
            .insert(record.message.id.clone(), record);
    }

    pub async fn seed_worker_registry(&self, registry: HashMap<String, WorkerInfo>) {
        *self.workers.write().await = registry;
    }

    pub async fn pending_records(&self) -> Vec<WalRecord> {
        self.pending.read().await.values().cloned().collect()
    }

    pub async fn scheduled_records(&self) -> Vec<WalRecord> {
        self.scheduled.read().await.values().cloned().collect()
    }
}

impl Default for RecordingWal {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Wal for RecordingWal {
    async fn append(&mut self, mut record: WalRecord) -> Result<(), CoreError> {
        let mut pending = self.pending.write().await;
        record.record_id = pending.len() as u64 + 1;
        pending.insert(record.message.id.clone(), record);
        Ok(())
    }

    async fn update_state(
        &mut self,
        message_id: &str,
        status: WalRecordState,
    ) -> Result<(), CoreError> {
        let mut pending = self.pending.write().await;
        match pending.get_mut(message_id) {
            Some(record) => {
                record.status = status;
                Ok(())
            }
            None => Err(CoreError::from(WalError::RecordNotFound(
                message_id.to_string(),
            ))),
        }
    }

    async fn replay_pending(&mut self) -> Result<Vec<WalRecord>, CoreError> {
        Ok(self
            .pending
            .read()
            .await
            .values()
            .filter(|record| record.status == WalRecordState::Pending)
            .cloned()
            .collect())
    }

    async fn flush(&mut self) -> Result<(), CoreError> {
        Ok(())
    }

    async fn schedule(&self, mut record: WalRecord) -> Result<(), CoreError> {
        let mut scheduled = self.scheduled.write().await;
        record.record_id = scheduled.len() as u64 + 1;
        scheduled.insert(record.message.id.clone(), record);
        Ok(())
    }

    async fn fetch_ready(&self) -> Result<Vec<WalRecord>, CoreError> {
        let mut scheduled = self.scheduled.write().await;
        let now = SystemTime::now();
        let ready_ids: Vec<String> = scheduled
            .iter()
            .filter_map(|(id, record)| match record.message.deliver_at {
                Some(deliver_at) if deliver_at <= now => Some(id.clone()),
                _ => None,
            })
            .collect();

        let mut ready = Vec::new();
        for id in ready_ids {
            if let Some(record) = scheduled.remove(&id) {
                ready.push(record);
            }
        }

        Ok(ready)
    }

    async fn remove_scheduled(&self, msg_id: &str) -> Result<(), CoreError> {
        self.scheduled.write().await.remove(msg_id);
        Ok(())
    }

    async fn save_worker_registry(
        &self,
        registry: HashMap<String, WorkerInfo>,
    ) -> Result<(), CoreError> {
        *self.workers.write().await = registry;
        Ok(())
    }

    async fn load_worker_registry(&self) -> Result<HashMap<String, WorkerInfo>, CoreError> {
        Ok(self.workers.read().await.clone())
    }
}
