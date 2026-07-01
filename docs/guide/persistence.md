# Persistence & WAL

`event_base` uses a Write-Ahead Log (WAL) to guarantee message durability. Every message is persisted **before** it is enqueued, ensuring no data loss on crash.

---

## WAL principle

The WAL follows the standard write-ahead pattern:

```
1. Message arrives
2. Append to WAL (state: Pending)
3. Enqueue the message for processing
4. Worker completes processing
5. Update WAL state: Complete (or Failed)
```

If the process crashes between steps 2 and 5, the message remains in `Pending` state and is **replayed** on the next startup.

### Record lifecycle

```
Pending ──► Processing ──► Complete
  │                           │
  └──► Failed (Dead Letter)
```

| State | Meaning |
|---|---|
| `Pending` | Written to WAL, not yet processed |
| `Processing` | Currently being handled by a worker |
| `Complete` | Successfully processed (Ack::Ack) |
| `Failed` | Moved to Dead Letter Queue |

---

## WAL implementations

### `MemoryWal` (in-memory)

Stores all records in RAM. Fast, but data is lost on restart.

```rust
use event_base::memory_wal::MemoryWal;

let wal = MemoryWal::new();
```

**When to use**: Development, testing, or when persistence is not required.

### `PersistentWal` (file-based)

Persists records to disk using `bincode` serialization. Data survives restarts.

```rust
use event_base::persistent::PersistentWal;

let wal = PersistentWal::new("/path/to/wal.bin".into()).await?;
```

The file stores:

- All records with their current state
- Scheduled (delayed) records
- Worker registry data

**When to use**: Production deployments requiring crash recovery.

---

## The `Wal` trait

Both implementations implement the `Wal` trait:

```rust
#[async_trait]
pub trait Wal: Send + Sync {
    async fn append(&mut self, record: WalRecord) -> Result<(), CoreError>;
    async fn update_state(&mut self, message_id: &str, status: WalRecordState) -> Result<(), CoreError>;
    async fn replay_pending(&mut self) -> Result<Vec<WalRecord>, CoreError>;
    async fn flush(&mut self) -> Result<(), CoreError>;
    async fn schedule(&self, record: WalRecord) -> Result<(), CoreError>;
    async fn fetch_ready(&self) -> Result<Vec<WalRecord>, CoreError>;
    async fn remove_scheduled(&self, msg_id: &str) -> Result<(), CoreError>;
    async fn save_worker_registry(&self, registry: HashMap<String, WorkerInfo>) -> Result<(), CoreError>;
    async fn load_worker_registry(&self) -> Result<HashMap<String, WorkerInfo>, CoreError>;
}
```

### WalRecord

```rust
pub struct WalRecord {
    pub record_id: u64,
    pub message: EMessage,
    pub status: WalRecordState,
    pub last_attempt_at: Option<SystemTime>,
    pub is_dead_letter: bool,
    pub dead_reason: Option<DeadReason>,
}
```

---

## Crash recovery

On system restart, `TopicRouter::replay()` is called (automatically by `start_queue_system!`).

### Recovery flow

```
System starts
    │
    ▼
TopicRouter::replay()
    │
    ├── 1. Load all Pending records from WAL
    │
    ├── 2. For each record:
    │       ├── If deliver_at is in the future → re-schedule
    │       └── Otherwise → re-send the message
    │
    └── 3. Return ReplaySummary
```

```rust
pub struct ReplaySummary {
    pub recovered: usize,   // Messages successfully re-sent
    pub delayed: usize,     // Messages re-scheduled for future delivery
    pub errors: Vec<(String, CoreError)>,  // Per-message errors
}
```

### Filtered replay

You can replay specific topics:

```rust
let summary = TopicRouter::global()
    .replay(Some(&["orders", "payments"]))
    .await?;

println!("Recovered: {}, Delayed: {}, Errors: {}",
    summary.recovered, summary.delayed, summary.errors.len());
```

---

## WAL sync (distributed mode)

In distributed mode, workers send WAL state updates to the Host via the `_system.wal_sync` topic. The `WalClient` (used by each worker) sends state transitions to the host's WAL.

```rust
// Inside Worker::process_msg:
self.wal.mark_processing(msg.id.as_str(), msg.topic.0.as_str()).await?;
// ... process ...
self.wal.mark_complete(msg.id.as_str(), msg.topic.0.as_str()).await?;
```

The host receives these sync messages and updates its local WAL accordingly.

---

## Codec

WAL records are serialized using `bincode`. The `BincodeCodec` is the default codec:

```rust
use event_base::core::wal::codec::{BincodeCodec, WalRecordCodec};

let codec = BincodeCodec;
let bytes = codec.encode(&record)?;
let decoded = codec.decode(&bytes)?;
```

### Performance

Serialization benchmarks:

| Operation | Throughput |
|---|---|
| Encode (256B payload) | ~1µs per record |
| Decode (256B payload) | ~800ns per record |

---

## Configuration

When starting the system, pass the WAL to `start_queue_system!`:

```rust
// In-memory (no persistence)
start_queue_system! {
    factory: MemoryQueueFactory::new(1000),
    wal: Some(MemoryWal::new()),
}

// Persistent (file-backed)
let wal = PersistentWal::new("./event_base_wal.bin".into()).await?;
start_queue_system! {
    factory: MemoryQueueFactory::new(1000),
    wal: Some(wal),
}
```

Pass `wal: None` to disable WAL entirely (not recommended for production).

---

## Best practices

1. **Always use a WAL in production** — even `MemoryWal` provides crash detection via replay.
2. **Use `PersistentWal` for critical data** — financial transactions, order processing, etc.
3. **Monitor replay summary** — log the `ReplaySummary` after startup to detect issues.
4. **Regular WAL checkpointing** — The WAL grows over time. Implement periodic compaction (future feature).
5. **Set `deliver_at` for delayed delivery** — The WAL handles scheduling reliably across restarts.

---

## Next steps

- [Shutdown Strategies](shutdown.md) — Graceful and forceful shutdown
- [Distributed Mode](distributed.md) — Host/Worker node model
