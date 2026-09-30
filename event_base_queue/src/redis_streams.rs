//! Redis Streams queue backend.
//!
//! [`RedisStreamQueueFactory`] implements [`QueueFactory`] on top of Redis
//! Streams (consumer groups + PEL), enabling multiple processes/nodes to share
//! one message fabric — unlike the in-memory backends, which are single-process
//! only.
//!
//! # Semantics vs the memory backend
//!
//! * One stream per topic: `{prefix}:{topic}`, with a single payload field
//!   (`"m"`, bincode‑encoded [`EMessage`]).
//! * `receive()` (used by the core topic dispatcher) XREADGROUPs a message and
//!   XACKs it immediately — at‑most‑once, mirroring the memory pop. Durable
//!   *at‑least‑once* delivery belongs to the WAL layer, not the queue.
//! * `claim()`/`ack()`/`nack()` use the stream consumer group PEL: `ack` =
//!   XACK, `nack` = XACK + XADD a fresh copy (re‑injection, same as crossfire).
//! * The global producer does **not** error on unknown topics (a Redis stream
//!   is created implicitly by `XADD`). This is intentional: in a distributed
//!   setup a node publishes to topics whose queue is only registered on other
//!   nodes — including coordination‑plane `_system.*` topics whose consumer
//!   lives on a *different* node (e.g. worker discovery from a Worker node to
//!   the Host). Consider setting [`maxlen`](RedisQueueConfig::maxlen) so
//!   unconsumed system streams stay bounded.
//! * **Delivery modes per topic class.** Business topics share one consumer
//!   group (competing consumers → load balancing). Coordination topics that
//!   *every* node must see (`_system.shutdown`, `_system.topic_sync`,
//!   `_system.metrics`) get a **per‑node group** (`{group}@{node}`), turning
//!   the stream into a fan‑out channel; their groups are created at `$` so a
//!   joining node never replays stale control traffic. All other groups are
//!   created with id `0` (messages sent before the first consumer attaches are
//!   not lost — bounded‑channel buffer parity).
//!
//! # Configuration
//!
//! See [`RedisQueueConfig`]: url/prefix/group/block_ms/maxlen/node.

use async_trait::async_trait;
use event_base_core::error::CoreError;
use event_base_core::error::queue::QueueError;
use event_base_core::message::EMessage;
use event_base_core::queues::consumer_factory::ConsumerFactory;
use event_base_core::queues::factory::QueueFactory;
use event_base_core::queues::{ClaimedMessage, EConsumer, EProducer};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use redis::streams::StreamReadOptions;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;
use uuid::Uuid;

/// Payload field name used inside every stream entry.
const MSG_FIELD: &str = "m";

/// Connection/backend options for the Redis queue factory.
#[derive(Clone, Debug)]
pub struct RedisQueueConfig {
    /// Redis connection URL, e.g. `redis://127.0.0.1:6379`.
    pub url: String,
    /// Key namespace; streams are named `{prefix}:{topic}`.
    pub prefix: String,
    /// Redis consumer‑group name shared by all event_base consumers.
    pub group: String,
    /// `BLOCK` timeout (ms) for `XREADGROUP`. Lower = fewer idle round trips,
    /// higher = less polling on the dispatcher loops.
    pub block_ms: usize,
    /// Optional `XADD MAXLEN ~ n` approximate trim for every topic stream.
    /// `None` (default) leaves streams unbounded — producers must not outrun
    /// consumers forever, or Redis memory grows.
    pub maxlen: Option<usize>,
    /// This process's node identity, used to name fan‑out consumer groups
    /// (`{group}@{node}`). `None` falls back to
    /// [`try_get_node_name`](event_base_core::try_get_node_name) at queue
    /// creation; if neither is set, coordination topics degrade to a shared
    /// competing group (single‑node semantics).
    pub node: Option<String>,
}

/// Coordination topics every node must receive a copy of (fan‑out per‑node
/// consumer groups). Everything else — business topics and Host‑only system
/// topics — stays on the shared competing group for load‑balanced claiming.
///
pub const FANOUT_SYSTEM_TOPICS: &[&str] = &[
    event_base_core::constant::SYSTEM_TOPIC_SHUTDOWN,
    event_base_core::constant::SYSTEM_TOPIC_TOPIC_SYNC,
    event_base_core::constant::SYSTEM_TOPIC_METRICS,
];

/// Consumer‑group start id for a topic: fan‑out groups begin at `$` (new
/// entries only — a node that joins later must not act on a stale shutdown
/// command or replay hours of metrics), everything else at `0`.
fn group_start_for(topic: &str) -> &'static str {
    if FANOUT_SYSTEM_TOPICS.contains(&topic) {
        "$"
    } else {
        "0"
    }
}

impl RedisQueueConfig {
    /// Creates a config for `url` with default namespace/group and no trim.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            prefix: "event_base".to_string(),
            group: "event_base".to_string(),
            block_ms: 500,
            maxlen: None,
            node: None,
        }
    }

    /// Sets the key prefix (default `"event_base"`).
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Sets the consumer‑group name (default `"event_base"`).
    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group = group.into();
        self
    }

    /// Sets the `BLOCK` timeout for reads in milliseconds (default 500).
    pub fn with_block_ms(mut self, block_ms: usize) -> Self {
        self.block_ms = block_ms;
        self
    }

    /// Enables approximate stream trimming at `maxlen` entries.
    pub fn with_maxlen(mut self, maxlen: usize) -> Self {
        self.maxlen = Some(maxlen);
        self
    }

    /// Sets this process's node identity explicitly (otherwise fan‑out group
    /// naming falls back to the global node name).
    pub fn with_node(mut self, node: impl Into<String>) -> Self {
        self.node = Some(node.into());
        self
    }

    fn node_or_global(&self) -> Option<String> {
        self.node
            .clone()
            .or_else(event_base_core::try_get_node_name)
    }

    /// Consumer group actually used for `topic`: the shared group for
    /// business/Host‑only topics, `{group}@{node}` for fan‑out coordination
    /// topics when a node identity exists.
    pub fn group_for(&self, topic: &str) -> String {
        if FANOUT_SYSTEM_TOPICS.contains(&topic)
            && let Some(node) = self.node_or_global()
        {
            return format!("{}@{}", self.group, node);
        }
        self.group.clone()
    }

    /// Full Redis key for a topic stream.
    pub fn stream_key(&self, topic: &str) -> String {
        format!("{}:{}", self.prefix, topic)
    }
}

fn encode_msg(msg: &EMessage) -> Result<Vec<u8>, CoreError> {
    bincode::encode_to_vec(msg, bincode::config::standard())
        .map_err(|e| CoreError::from(QueueError::Send(format!("encode failed: {e}"))))
}

fn decode_msg(bytes: &[u8]) -> Result<EMessage, CoreError> {
    let (msg, _) = bincode::decode_from_slice(bytes, bincode::config::standard())
        .map_err(|e| CoreError::from(QueueError::Receive(format!("decode failed: {e}"))))?;
    Ok(msg)
}

fn send_err(e: redis::RedisError) -> CoreError {
    CoreError::from(QueueError::Send(e.to_string()))
}

fn recv_err(e: redis::RedisError) -> CoreError {
    CoreError::from(QueueError::Receive(e.to_string()))
}

/// XADDs an encoded message onto `key`, honoring the optional maxlen trim.
async fn xadd(
    conn: &mut ConnectionManager,
    key: &str,
    msg: &EMessage,
    maxlen: Option<usize>,
) -> Result<(), CoreError> {
    let bytes = encode_msg(msg)?;
    let mut cmd = redis::cmd("XADD");
    cmd.arg(key);
    if let Some(n) = maxlen {
        cmd.arg("MAXLEN").arg("~").arg(n);
    }
    cmd.arg("*").arg(MSG_FIELD).arg(&bytes[..]);
    let _: String = cmd.query_async(conn).await.map_err(send_err)?;
    Ok(())
}

/// XACKs one entry of `key` within the group.
async fn xack(
    conn: &mut ConnectionManager,
    key: &str,
    group: &str,
    entry_id: &str,
) -> Result<(), CoreError> {
    let _: i64 = redis::cmd("XACK")
        .arg(key)
        .arg(group)
        .arg(entry_id)
        .query_async(conn)
        .await
        .map_err(send_err)?;
    Ok(())
}

// ── Producer ────────────────────────────────────────────────────────────────

/// Producer that appends messages to a single topic stream.
#[derive(Clone)]
pub struct RedisStreamProducer {
    conn: ConnectionManager,
    key: String,
    maxlen: Option<usize>,
}

#[async_trait]
impl EProducer for RedisStreamProducer {
    async fn send(&self, msg: EMessage) -> Result<(), CoreError> {
        let mut conn = self.conn.clone();
        xadd(&mut conn, &self.key, &msg, self.maxlen).await
    }

    /// XADD never blocks, so `try_send` is an alias for [`send`](Self::send).
    async fn try_send(&self, msg: EMessage) -> Result<(), CoreError> {
        self.send(msg).await
    }

    async fn send_timeout(&self, msg: EMessage, timeout: Duration) -> Result<(), CoreError> {
        tokio::time::timeout(timeout, self.send(msg))
            .await
            .unwrap_or_else(|_elapsed| Err(CoreError::from(QueueError::Timeout)))
    }
}

// ── Consumer ────────────────────────────────────────────────────────────────

/// Consumer that reads one topic stream through a Redis consumer group.
///
/// Each consumer owns a **dedicated** Redis connection: `XREADGROUP BLOCK`
/// parks its connection until data arrives, and sharing one multiplexed
/// connection between blocking reads and the node's other traffic would stall
/// every command behind the blocked read.
pub struct RedisStreamConsumer {
    client: redis::Client,
    conn: Option<ConnectionManager>,
    key: String,
    group: String,
    /// Group creation id: `"0"` (full replay — business/host topics) or
    /// `"$"` (new entries only — fan‑out coordination groups).
    group_start: &'static str,
    consumer: String,
    block_ms: usize,
    maxlen: Option<usize>,
    /// claim_id → (stream entry id, message) for in‑flight claims.
    pending: Arc<Mutex<HashMap<String, (String, EMessage)>>>,
    /// XGROUP CREATE is issued lazily once per consumer instance.
    group_ready: AtomicBool,
}

impl RedisStreamConsumer {
    fn new(
        client: redis::Client,
        key: String,
        group: String,
        group_start: &'static str,
        block_ms: usize,
        maxlen: Option<usize>,
    ) -> Self {
        Self {
            client,
            conn: None,
            key,
            group,
            group_start,
            consumer: Uuid::new_v4().to_string(),
            block_ms,
            maxlen,
            pending: Arc::new(Mutex::new(HashMap::new())),
            group_ready: AtomicBool::new(false),
        }
    }

    /// Returns the consumer's private connection, connecting lazily.
    async fn conn(&mut self) -> Result<ConnectionManager, CoreError> {
        if self.conn.is_none() {
            let manager = self
                .client
                .get_connection_manager()
                .await
                .map_err(recv_err)?;
            self.conn = Some(manager);
        }
        Ok(self.conn.clone().expect("connection just set"))
    }

    /// Connection plus a guaranteed consumer group, ready for reading.
    async fn ready_conn(&mut self) -> Result<ConnectionManager, CoreError> {
        let mut conn = self.conn().await?;
        self.ensure_group(&mut conn).await?;
        Ok(conn)
    }

    async fn ensure_group(&self, conn: &mut ConnectionManager) -> Result<(), CoreError> {
        if self.group_ready.load(Ordering::Acquire) {
            return Ok(());
        }
        let result = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.key)
            .arg(&self.group)
            .arg(self.group_start)
            .arg("MKSTREAM")
            .query_async::<()>(conn)
            .await;
        match result {
            Ok(()) => {
                self.group_ready.store(true, Ordering::Release);
                Ok(())
            }
            // Group already exists (another node/consumer created it).
            Err(e) if e.to_string().contains("BUSYGROUP") => {
                self.group_ready.store(true, Ordering::Release);
                Ok(())
            }
            Err(e) => Err(recv_err(e)),
        }
    }

    /// XREADGROUP up to `count` new entries, decoding them into messages with
    /// their stream entry ids. Returns `Ok(vec![])` when the block timeout
    /// elapses with no messages.
    async fn read_group(&mut self, count: usize) -> Result<Vec<(String, EMessage)>, CoreError> {
        let mut conn = self.ready_conn().await?;
        let options = StreamReadOptions::default()
            .group(&self.group, &self.consumer)
            .block(self.block_ms)
            .count(count);
        let key = self.key.clone();
        let reply: Option<redis::streams::StreamReadReply> = conn
            .xread_options(&[key.as_str()][..], &[">"], &options)
            .await
            .map_err(recv_err)?;
        let mut out = Vec::new();
        if let Some(reply) = reply {
            for key in reply.keys {
                for id in key.ids {
                    match id.get::<Vec<u8>>(MSG_FIELD) {
                        Some(bytes) => {
                            let msg = decode_msg(&bytes)?;
                            out.push((id.id, msg));
                        }
                        None => {
                            // Foreign entry on our stream: ack it so it does
                            // not pile up in the PEL.
                            let _ = xack(&mut conn, &self.key, &self.group, &id.id).await;
                        }
                    }
                }
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl EConsumer for RedisStreamConsumer {
    /// Reads the next message and XACKs it immediately (at‑most‑once, parity
    /// with the memory backend's pop).
    ///
    /// Loops over the `BLOCK` timeout until a message arrives, honoring the
    /// [`EConsumer::receive`] contract; `None` signals an unrecoverable
    /// backend error (the memory backend returns `None` when closed).
    async fn receive(&mut self) -> Option<EMessage> {
        loop {
            match self.read_group(1).await {
                Ok(mut entries) => {
                    if let Some((entry_id, msg)) = entries.pop() {
                        let mut conn = self.conn().await.ok()?;
                        // Ack failure is not fatal: the message is already
                        // dispatched; worst case the PEL keeps a ghost entry.
                        let _ = xack(&mut conn, &self.key, &self.group, &entry_id).await;
                        return Some(msg);
                    }
                    // Block timeout elapsed with no traffic — keep waiting.
                }
                Err(_) => return None,
            }
        }
    }

    async fn claim(&mut self) -> Result<Option<ClaimedMessage>, CoreError> {
        let mut batch = self.claim_batch(1).await?;
        Ok(batch.pop())
    }

    async fn claim_batch(&mut self, max: usize) -> Result<Vec<ClaimedMessage>, CoreError> {
        if max == 0 {
            return Ok(Vec::new());
        }
        let entries = self.read_group(max).await?;
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let now = SystemTime::now();
        let mut batch = Vec::with_capacity(entries.len());
        let mut pending = self.pending.lock().await;
        for (entry_id, msg) in entries {
            let claim_id = Uuid::new_v4().to_string();
            pending.insert(claim_id.clone(), (entry_id, msg.clone()));
            batch.push(ClaimedMessage {
                message: msg,
                claim_id,
                claimed_at: now,
            });
        }
        Ok(batch)
    }

    async fn ack(&mut self, claim_id: &str) -> Result<(), CoreError> {
        let entry = self.pending.lock().await.remove(claim_id);
        match entry {
            Some((entry_id, _)) => {
                let mut conn = self.conn().await?;
                xack(&mut conn, &self.key, &self.group, &entry_id).await
            }
            None => Err(CoreError::from(QueueError::InvalidClaimId(
                claim_id.to_string(),
            ))),
        }
    }

    /// Re‑injects the message as a fresh stream entry and releases the original
    /// (same requeue semantics as the crossfire backend).
    async fn nack(&mut self, claim_id: &str) -> Result<(), CoreError> {
        let entry = self.pending.lock().await.remove(claim_id);
        match entry {
            Some((entry_id, msg)) => {
                let mut conn = self.conn().await?;
                xadd(&mut conn, &self.key, &msg, self.maxlen).await?;
                xack(&mut conn, &self.key, &self.group, &entry_id).await
            }
            None => Err(CoreError::from(QueueError::InvalidClaimId(
                claim_id.to_string(),
            ))),
        }
    }
}

// ── Consumer factory ────────────────────────────────────────────────────────

/// Creates [`RedisStreamConsumer`]s for one topic stream.
pub struct RedisStreamConsumerFactory {
    client: redis::Client,
    key: String,
    group: String,
    group_start: &'static str,
    block_ms: usize,
    maxlen: Option<usize>,
}

impl ConsumerFactory for RedisStreamConsumerFactory {
    fn create_consumer(&self) -> Box<dyn EConsumer> {
        Box::new(RedisStreamConsumer::new(
            self.client.clone(),
            self.key.clone(),
            self.group.clone(),
            self.group_start,
            self.block_ms,
            self.maxlen,
        ))
    }

    fn clone_factory(&self) -> Arc<dyn ConsumerFactory> {
        Arc::new(RedisStreamConsumerFactory {
            client: self.client.clone(),
            key: self.key.clone(),
            group: self.group.clone(),
            group_start: self.group_start,
            block_ms: self.block_ms,
            maxlen: self.maxlen,
        })
    }
}

// ── Routing (global) producer ───────────────────────────────────────────────

/// Global producer used by [`TopicRouter`](event_base_core::topic::TopicRouter):
/// appends each message to the stream of its own topic.
///
/// Unlike the memory `RoutingProducer`, unknown topics are always accepted —
/// including `_system.*` topics whose only consumer is registered on another
/// node (worker discovery/heartbeat to the Host, or the other direction). The
/// target node's consumer group picks them up; with no consumer anywhere,
/// entries accumulate in the stream, so prefer [`maxlen`] on multi‑node
/// deployments.
///
/// [`maxlen`]: RedisQueueConfig::maxlen
pub struct RedisRoutingProducer {
    conn: ConnectionManager,
    prefix: String,
    maxlen: Option<usize>,
}

#[async_trait]
impl EProducer for RedisRoutingProducer {
    async fn send(&self, msg: EMessage) -> Result<(), CoreError> {
        let key = format!("{}:{}", self.prefix, msg.topic.0);
        let mut conn = self.conn.clone();
        xadd(&mut conn, &key, &msg, self.maxlen).await
    }

    async fn try_send(&self, msg: EMessage) -> Result<(), CoreError> {
        self.send(msg).await
    }

    async fn send_timeout(&self, msg: EMessage, timeout: Duration) -> Result<(), CoreError> {
        tokio::time::timeout(timeout, self.send(msg))
            .await
            .unwrap_or_else(|_elapsed| Err(CoreError::from(QueueError::Timeout)))
    }
}

// ── Queue factory ───────────────────────────────────────────────────────────

/// [`QueueFactory`] backed by Redis Streams.
///
/// Every event_base process keeps its own consumer name per topic, so multiple
/// nodes share consumption of the same topic stream through the group PEL.
pub struct RedisStreamQueueFactory {
    /// Shared (auto‑reconnecting) connection for non‑blocking commands:
    /// `XADD` from producers and the `PING` health check.
    conn: ConnectionManager,
    /// Used to open each consumer's dedicated connection (blocking reads must
    /// not park the shared one).
    client: redis::Client,
    config: RedisQueueConfig,
    main_consumer: Arc<Mutex<RedisStreamConsumer>>,
}

impl RedisStreamQueueFactory {
    /// Connects to Redis using `url` (defaults applied).
    pub async fn new(url: impl Into<String>) -> Result<Self, CoreError> {
        Self::with_config(RedisQueueConfig::new(url)).await
    }

    /// Connects to Redis with an explicit configuration.
    pub async fn with_config(config: RedisQueueConfig) -> Result<Self, CoreError> {
        let client = redis::Client::open(config.url.as_str()).map_err(send_err)?;
        let conn = client.get_connection_manager().await.map_err(send_err)?;
        Ok(Self::assemble(conn, client, config))
    }

    /// Builds a factory from an existing connection manager. Consumers still
    /// open their own connections from `config.url`, so it must point at the
    /// same Redis the manager is connected to.
    pub fn from_connection_manager(
        conn: ConnectionManager,
        config: RedisQueueConfig,
    ) -> Result<Self, CoreError> {
        let client = redis::Client::open(config.url.as_str()).map_err(send_err)?;
        Ok(Self::assemble(conn, client, config))
    }

    fn assemble(conn: ConnectionManager, client: redis::Client, config: RedisQueueConfig) -> Self {
        let main_key = config.stream_key("_default");
        let main_consumer = Arc::new(Mutex::new(RedisStreamConsumer::new(
            client.clone(),
            main_key,
            config.group.clone(),
            "0",
            config.block_ms,
            config.maxlen,
        )));
        Self {
            conn,
            client,
            config,
            main_consumer,
        }
    }

    /// The configuration this factory was built with.
    pub fn config(&self) -> &RedisQueueConfig {
        &self.config
    }
}

#[async_trait]
impl QueueFactory for RedisStreamQueueFactory {
    fn create_queue(
        &self,
        topic: &str,
    ) -> Result<(Arc<dyn EProducer>, Arc<dyn ConsumerFactory>), CoreError> {
        let key = self.config.stream_key(topic);
        let producer = Arc::new(RedisStreamProducer {
            conn: self.conn.clone(),
            key: key.clone(),
            maxlen: self.config.maxlen,
        });
        // Coordination fan‑out topics get a per‑node group (every node sees
        // every message, started at `$`); everything else shares the group
        // with full replay for competing consumption.
        let consumer_factory = Arc::new(RedisStreamConsumerFactory {
            client: self.client.clone(),
            key,
            group: self.config.group_for(topic),
            group_start: group_start_for(topic),
            block_ms: self.config.block_ms,
            maxlen: self.config.maxlen,
        });
        Ok((producer, consumer_factory))
    }

    fn create_global_producer(&self) -> Result<Arc<dyn EProducer>, CoreError> {
        Ok(Arc::new(RedisRoutingProducer {
            conn: self.conn.clone(),
            prefix: self.config.prefix.clone(),
            maxlen: self.config.maxlen,
        }))
    }

    fn create_main_consumer(&self) -> Result<Arc<Mutex<dyn EConsumer>>, CoreError> {
        Ok(self.main_consumer.clone())
    }

    fn name(&self) -> &'static str {
        "redis"
    }

    async fn health_check(&self) -> Result<(), CoreError> {
        let mut conn = self.conn.clone();
        let pong: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(recv_err)?;
        if pong == "PONG" {
            Ok(())
        } else {
            Err(CoreError::from(QueueError::Receive(format!(
                "unexpected PING reply: {pong}"
            ))))
        }
    }
}
