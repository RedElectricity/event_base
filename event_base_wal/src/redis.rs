//! Redis WAL backend.
//!
//! [`RedisWal`] implements [`Wal`] on top of plain Redis keys, so the pending
//! set and the delay index survive process restarts and can be shared by
//! multiple nodes (see the prefix caveat below).
//!
//! # Key layout (`{prefix}` defaults to `event_base`)
//!
//! * `{prefix}:wal:records` — hash: `msg_id` → bincode(`WalRecord`)
//! * `{prefix}:wal:delays` — hash: `msg_id` → bincode(`WalRecord`)
//! * `{prefix}:wal:delay_index` — zset: `msg_id` → `deliver_at` (epoch ms,
//!   `i64::MAX` for records without `deliver_at`, which then never fire)
//! * `{prefix}:wal:registry` — string: bincode(worker registry map)
//! * `{prefix}:wal:id_counter` — integer, `INCR`‑assigned `record_id`s
//!
//! # Semantics vs the other backends
//!
//! * Every write goes straight to Redis, so [`flush`](Wal::flush) is a no‑op.
//! * [`update_state`](Wal::update_state) errors with `RecordNotFound` for
//!   unknown ids (mirrors [`PersistentWal`](crate::persistent::PersistentWal);
//!   [`MemoryWal`](crate::memory::MemoryWal) inserts a placeholder instead).
//!   The status change is a Redis‑side read‑modify‑write, so two nodes racing
//!   on the same record last‑write‑wins; use a node‑unique `prefix` unless you
//!   deliberately share a WAL.
//! * [`fetch_ready`](Wal::fetch_ready) claims due entries with `ZREM` before
//!   reading them, so concurrent consumers never deliver the same record twice.
//!
//! ```no_run
//! # async fn demo() -> Result<(), event_base_core::error::CoreError> {
//! use event_base_core::wal::wal::Wal;
//! use event_base_wal::redis::RedisWal;
//! let wal = RedisWal::new("redis://127.0.0.1:6379").await?;
//! # Ok(())
//! # }
//! ```

use ::redis::aio::ConnectionManager;
use event_base_core::error::CoreError;
use event_base_core::error::wal::WalError;
use event_base_core::wal::wal::{Wal, WalRecord, WalRecordState};
use event_base_core::worker_registry::WorkerInfo;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

fn backend_err(e: ::redis::RedisError) -> CoreError {
    CoreError::from(WalError::Backend(e.to_string()))
}

fn corrupted_err(context: &str, e: bincode::error::DecodeError) -> CoreError {
    CoreError::from(WalError::Corrupted(format!("{context}: {e}")))
}

fn encode<T: bincode::Encode>(value: &T) -> Result<Vec<u8>, CoreError> {
    bincode::encode_to_vec(value, bincode::config::standard())
        .map_err(|e| CoreError::from(WalError::Write(format!("encode failed: {e}"))))
}

fn decode<T: bincode::Decode<()>>(bytes: &[u8], context: &str) -> Result<T, CoreError> {
    let (value, _) = bincode::decode_from_slice(bytes, bincode::config::standard())
        .map_err(|e| corrupted_err(context, e))?;
    Ok(value)
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn deliver_at_millis(record: &WalRecord) -> i64 {
    match record.message.deliver_at {
        Some(at) => at
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0),
        // Records without an explicit deadline never become ready through
        // the index; they stay until remove_scheduled or a manual send.
        None => i64::MAX,
    }
}

/// A [`Wal`] implementation backed by Redis hashes and a sorted set.
///
/// Cheap to clone (shares one multiplexed [`ConnectionManager`]); all trait
/// methods only need `&self`, matching [`MemoryWal`](crate::memory::MemoryWal).
#[derive(Clone)]
pub struct RedisWal {
    conn: ConnectionManager,
    prefix: String,
}

impl RedisWal {
    /// Connects to Redis at `url` using the default `event_base` prefix.
    pub async fn new(url: impl Into<String>) -> Result<Self, CoreError> {
        Self::with_prefix(url, "event_base").await
    }

    /// Connects to Redis at `url` with an explicit key prefix.
    pub async fn with_prefix(
        url: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Self, CoreError> {
        let client = ::redis::Client::open(url.into()).map_err(backend_err)?;
        let conn = client.get_connection_manager().await.map_err(backend_err)?;
        Ok(Self::from_connection_manager(conn, prefix))
    }

    /// Builds a WAL from an existing connection manager.
    pub fn from_connection_manager(conn: ConnectionManager, prefix: impl Into<String>) -> Self {
        Self {
            conn,
            prefix: prefix.into(),
        }
    }

    fn records_key(&self) -> String {
        format!("{}:wal:records", self.prefix)
    }

    fn delays_key(&self) -> String {
        format!("{}:wal:delays", self.prefix)
    }

    fn delay_index_key(&self) -> String {
        format!("{}:wal:delay_index", self.prefix)
    }

    fn registry_key(&self) -> String {
        format!("{}:wal:registry", self.prefix)
    }

    fn counter_key(&self) -> String {
        format!("{}:wal:id_counter", self.prefix)
    }

    async fn next_record_id(&self) -> Result<u64, CoreError> {
        let mut conn = self.conn.clone();
        let id: i64 = ::redis::cmd("INCR")
            .arg(self.counter_key())
            .query_async(&mut conn)
            .await
            .map_err(backend_err)?;
        Ok(id.max(0) as u64)
    }
}

#[async_trait::async_trait]
impl Wal for RedisWal {
    async fn append(&mut self, mut record: WalRecord) -> Result<(), CoreError> {
        record.record_id = self.next_record_id().await?;
        let blob = encode(&record)?;
        let mut conn = self.conn.clone();
        let _: i64 = ::redis::cmd("HSET")
            .arg(self.records_key())
            .arg(&record.message.id)
            .arg(&blob)
            .query_async(&mut conn)
            .await
            .map_err(backend_err)?;
        Ok(())
    }

    async fn update_state(
        &mut self,
        message_id: &str,
        status: WalRecordState,
    ) -> Result<(), CoreError> {
        let mut conn = self.conn.clone();
        let existing: Option<Vec<u8>> = ::redis::cmd("HGET")
            .arg(self.records_key())
            .arg(message_id)
            .query_async(&mut conn)
            .await
            .map_err(backend_err)?;
        let Some(bytes) = existing else {
            return Err(CoreError::from(WalError::RecordNotFound(
                message_id.to_string(),
            )));
        };
        let mut record: WalRecord = decode(&bytes, "update_state")?;
        record.status = status;
        let blob = encode(&record)?;
        let _: i64 = ::redis::cmd("HSET")
            .arg(self.records_key())
            .arg(message_id)
            .arg(&blob)
            .query_async(&mut conn)
            .await
            .map_err(backend_err)?;
        Ok(())
    }

    async fn replay_pending(&mut self) -> Result<Vec<WalRecord>, CoreError> {
        let mut conn = self.conn.clone();
        let all: HashMap<Vec<u8>, Vec<u8>> = ::redis::cmd("HGETALL")
            .arg(self.records_key())
            .query_async(&mut conn)
            .await
            .map_err(backend_err)?;
        let mut pending = Vec::new();
        for (_id, bytes) in all {
            let record: WalRecord = decode(&bytes, "replay_pending")?;
            if record.status == WalRecordState::Pending {
                pending.push(record);
            }
        }
        pending.sort_by_key(|r| r.record_id);
        Ok(pending)
    }

    /// Redis writes are already durable, so this is a no‑op.
    async fn flush(&mut self) -> Result<(), CoreError> {
        Ok(())
    }

    async fn schedule(&self, mut record: WalRecord) -> Result<(), CoreError> {
        record.record_id = self.next_record_id().await?;
        let blob = encode(&record)?;
        let score = deliver_at_millis(&record);
        let mut conn = self.conn.clone();
        // HSET (payload) + ZADD (due index) run in ONE server‑side script, so
        // they commit atomically. Split across two round trips, a crash between
        // them strands the delay either as an indexed record with no payload
        // (fetch_ready loses it) or a payload with no index (never becomes
        // due). Redis guarantees EVAL runs without interleaving other clients'
        // commands, which the WATCH‑transaction API cannot promise here because
        // the id is minted by INCR *outside* the transaction.
        const SCHEDULE_LUA: &str = "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]) \
                                    redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1]) \
                                    return 1";
        let _: i64 = ::redis::cmd("EVAL")
            .arg(SCHEDULE_LUA)
            .arg(2)
            .arg(self.delays_key())
            .arg(self.delay_index_key())
            .arg(&record.message.id)
            .arg(&blob)
            .arg(score)
            .query_async(&mut conn)
            .await
            .map_err(backend_err)?;
        Ok(())
    }

    /// Pops every record whose `deliver_at` has passed.
    ///
    /// Each candidate is claimed with `ZREM` (exactly one concurrent caller
    /// wins) before its payload is read and deleted.
    async fn fetch_ready(&self) -> Result<Vec<WalRecord>, CoreError> {
        let mut conn = self.conn.clone();
        let due: Vec<Vec<u8>> = ::redis::cmd("ZRANGEBYSCORE")
            .arg(self.delay_index_key())
            .arg("-inf")
            .arg(now_millis())
            .query_async(&mut conn)
            .await
            .map_err(backend_err)?;
        let mut ready = Vec::new();
        for msg_id in due {
            let claimed: i64 = ::redis::cmd("ZREM")
                .arg(self.delay_index_key())
                .arg(&msg_id)
                .query_async(&mut conn)
                .await
                .map_err(backend_err)?;
            if claimed == 0 {
                // Another concurrent consumer already took this one.
                continue;
            }
            let bytes: Option<Vec<u8>> = ::redis::cmd("HGET")
                .arg(self.delays_key())
                .arg(&msg_id)
                .query_async(&mut conn)
                .await
                .map_err(backend_err)?;
            let _: i64 = ::redis::cmd("HDEL")
                .arg(self.delays_key())
                .arg(&msg_id)
                .query_async(&mut conn)
                .await
                .map_err(backend_err)?;
            if let Some(bytes) = bytes {
                let record: WalRecord = decode(&bytes, "fetch_ready")?;
                ready.push(record);
            }
        }
        Ok(ready)
    }

    async fn remove_scheduled(&self, msg_id: &str) -> Result<(), CoreError> {
        let mut conn = self.conn.clone();
        let _: i64 = ::redis::cmd("ZREM")
            .arg(self.delay_index_key())
            .arg(msg_id)
            .query_async(&mut conn)
            .await
            .map_err(backend_err)?;
        let _: i64 = ::redis::cmd("HDEL")
            .arg(self.delays_key())
            .arg(msg_id)
            .query_async(&mut conn)
            .await
            .map_err(backend_err)?;
        Ok(())
    }

    /// Replaces the stored worker registry with a single blob value.
    async fn save_worker_registry(
        &self,
        registry: HashMap<String, WorkerInfo>,
    ) -> Result<(), CoreError> {
        let blob = encode(&registry)?;
        let mut conn = self.conn.clone();
        let _: () = ::redis::cmd("SET")
            .arg(self.registry_key())
            .arg(&blob)
            .query_async(&mut conn)
            .await
            .map_err(backend_err)?;
        Ok(())
    }

    async fn load_worker_registry(&self) -> Result<HashMap<String, WorkerInfo>, CoreError> {
        let mut conn = self.conn.clone();
        let blob: Option<Vec<u8>> = ::redis::cmd("GET")
            .arg(self.registry_key())
            .query_async(&mut conn)
            .await
            .map_err(backend_err)?;
        match blob {
            Some(bytes) => decode(&bytes, "load_worker_registry"),
            None => Ok(HashMap::new()),
        }
    }
}
