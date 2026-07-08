//! Queue implementation benchmarks — exercises all three queue backends
//! (flume, mpmc, crossfire) plus the end‑to‑end system pipeline.
//!
//! Queue benchmarks (per impl):
//! - `queue_send`: raw send throughput (uncontended)
//! - `queue_recv`: raw receive throughput (pre‑loaded queue)
//! - `queue_claim_ack`: claim + ack round‑trip throughput
//!
//! System benchmarks (kept from original):
//! - `system_send`: TopicRouter::send pipeline (WAL + producer)
//! - `system_process`: Worker::test_process_msg pipeline (handler + middleware + WAL + audit)

use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use event_base_core::audit::AuditManager;
use event_base_core::handler::{Ack, EHandler};
use event_base_core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use event_base_core::middleware::{Middleware, Next, Pipeline};
use event_base_core::queues::consumer_factory::ConsumerFactory;
use event_base_core::queues::consumer_router::ConsumerRouter;
use event_base_core::queues::factory::QueueFactory;
use event_base_core::queues::{ClaimedMessage, EConsumer, EProducer};
use event_base_core::topic::TopicRouter;
use event_base_core::wal::wal::{Wal, WalRecord};
use event_base_core::worker::Worker;
use event_base_core::worker_registry::WorkerRegistry;
use event_base_core::{set_node_name, set_node_type, NodeType};
use event_base_queue::{crossfire, flume, mpmc};
use event_base_test::support::{RecordingProducer, RecordingWal};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

// ── Config ───────────────────────────────────────────────────────────────────

const SEND_COUNT: u64 = 50_000;
const RECV_COUNT: u64 = 50_000;
const CLAIM_COUNT: u64 = 10_000;
const PROCESS_COUNT: u64 = 10_000;

static SYSTEM_INIT: std::sync::Once = std::sync::Once::new();

fn bench_topic() -> &'static str {
    "bench-sys"
}

/// Create a single message with the given payload.
fn msg(topic: &str, payload: &[u8]) -> EMessage {
    EMessage::new(
        MessageTopic(topic.into()),
        MessagePayload(payload.to_vec()),
        DeliveryMode::Standard,
        None,
    )
}

/// Pre‑create N messages with empty payload (avoid UUID overhead in loop).
fn pre_create(n: u64) -> Vec<EMessage> {
    (0..n).map(|_| msg(bench_topic(), &[])).collect()
}

// ── Minimal infra (for system benchmarks) ────────────────────────────────────

struct NoopConsumer;
#[async_trait]
impl EConsumer for NoopConsumer {
    async fn receive(&mut self) -> Option<EMessage> {
        None
    }
    async fn claim(&mut self) -> Result<Option<ClaimedMessage>, event_base_core::error::CoreError> {
        Ok(None)
    }
    async fn ack(&mut self, _: &str) -> Result<(), event_base_core::error::CoreError> {
        Ok(())
    }
    async fn nack(&mut self, _: &str) -> Result<(), event_base_core::error::CoreError> {
        Ok(())
    }
}

struct NoopCF;
impl ConsumerFactory for NoopCF {
    fn create_consumer(&self) -> Box<dyn EConsumer> {
        Box::new(NoopConsumer)
    }
    fn clone_factory(&self) -> Arc<dyn ConsumerFactory> {
        Arc::new(NoopCF)
    }
}

struct NoopQF {
    p: Arc<dyn EProducer>,
}
#[async_trait]
impl QueueFactory for NoopQF {
    fn create_queue(
        &self,
        _: &str,
    ) -> Result<(Arc<dyn EProducer>, Arc<dyn ConsumerFactory>), event_base_core::error::CoreError>
    {
        Ok((self.p.clone(), Arc::new(NoopCF)))
    }
    fn create_global_producer(
        &self,
    ) -> Result<Arc<dyn EProducer>, event_base_core::error::CoreError> {
        Ok(self.p.clone())
    }
    fn create_main_consumer(
        &self,
    ) -> Result<Arc<Mutex<dyn EConsumer>>, event_base_core::error::CoreError> {
        Ok(Arc::new(Mutex::new(NoopConsumer)))
    }
    fn name(&self) -> &'static str {
        "bench"
    }
}

// ── Lock-free producer for benchmark (drops messages to avoid Mutex contention) ──

struct BenchProducer;
#[async_trait]
impl EProducer for BenchProducer {
    async fn send(&self, _msg: EMessage) -> Result<(), event_base_core::error::CoreError> {
        Ok(())
    }
    async fn try_send(&self, _msg: EMessage) -> Result<(), event_base_core::error::CoreError> {
        Ok(())
    }
    async fn send_timeout(
        &self,
        _msg: EMessage,
        _timeout: Duration,
    ) -> Result<(), event_base_core::error::CoreError> {
        Ok(())
    }
}

// ── System setup (once per process) ──────────────────────────────────────────

static BENCH_PRODUCER: std::sync::OnceLock<std::sync::RwLock<Arc<dyn EProducer>>> = std::sync::OnceLock::new();

fn system_init() {
    SYSTEM_INIT.call_once(|| {
        let _ = set_node_name("bench-node".to_string());
        let _ = set_node_type(NodeType::Host);

        let fake_wal = RecordingWal::new();
        let wal: Arc<tokio::sync::RwLock<Box<dyn Wal>>> =
            Arc::new(tokio::sync::RwLock::new(Box::new(fake_wal)));

        let bp = Arc::new(BenchProducer);

        let qf = Arc::new(event_base_queue::flume::MemoryQueueFactory::new(1_000_000));
        let producer = qf.create_global_producer().expect("global producer");
        let main_consumer = qf.create_main_consumer().expect("main consumer");

        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let _ = WorkerRegistry::init(Some(wal.clone())).await;
            let _ = TopicRouter::init(bp.clone());
            let _ = AuditManager::init(1024);
            let _ = event_base_core::metrics::manager::MetricsManager::init();
            let _ = event_base_core::metrics::node_store::MetricsStore::init();
            ConsumerRouter::init(main_consumer, qf, None).expect("CR init");
        });

        BENCH_PRODUCER.set(std::sync::RwLock::new(producer)).ok();
    });
}

// ── Handlers ─────────────────────────────────────────────────────────────────

struct AckHandler;
#[async_trait]
impl EHandler for AckHandler {
    async fn handler(&self, _msg: &EMessage) -> Ack {
        Ack::Ack
    }
}

struct CpuHandler;
#[async_trait]
impl EHandler for CpuHandler {
    async fn handler(&self, _msg: &EMessage) -> Ack {
        let mut s: u64 = 0;
        for i in 0..100 {
            s = s.wrapping_add(i);
        }
        std::hint::black_box(s);
        Ack::Ack
    }
}

// ── Middleware ───────────────────────────────────────────────────────────────

struct LoggingMiddleware;
#[async_trait]
impl Middleware for LoggingMiddleware {
    async fn handle(&self, msg: &mut EMessage, next: Next<'_>) -> Ack {
        msg.payload.0.push(0);
        next.run(msg).await
    }
}

// ── Queue benchmarks ─────────────────────────────────────────────────────────

/// Helper: benchmark raw `send()` throughput for a single queue impl.
/// Creates a fresh queue inside each `b.iter()` iteration so the bounded
/// channel never fills up across repeated measurements.
macro_rules! bench_send_one {
    ($group:ident, $label:expr, $module:ident, $rt:expr, $msgs:expr) => {{
        let cap = $msgs.len() + 1024;
        let msgs = &$msgs;
        $group.bench_function(BenchmarkId::new($label, $msgs.len()), |b| {
            b.iter(|| {
                $rt.block_on(async {
                    let (p, _c) = $module::memory_queue(cap);
                    for m in msgs {
                        p.send(m.clone()).await.expect("send");
                    }
                });
            });
        });
    }};
}

/// Helper: benchmark raw `receive()` throughput — pre‑load the queue outside
/// the measured region via `iter_custom`.
macro_rules! bench_recv_one {
    ($group:ident, $label:expr, $module:ident, $rt:expr, $msgs:expr) => {{
        $group.bench_function(BenchmarkId::new($label, $msgs.len()), |b| {
            b.iter_custom(|iters| {
                let msgs = $msgs.clone();
                $rt.block_on(async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let cap = msgs.len() + 1024;
                        let (p, mut c) = $module::memory_queue(cap);
                        // pre‑load
                        for m in &msgs {
                            p.send(m.clone()).await.expect("send");
                        }
                        let start = std::time::Instant::now();
                        for _ in &msgs {
                            let _ = c.receive().await.expect("recv");
                        }
                        total += start.elapsed();
                    }
                    total
                })
            });
        });
    }};
}

/// Helper: benchmark `claim()` + `ack()` throughput.
macro_rules! bench_claim_ack_one {
    ($group:ident, $label:expr, $module:ident, $rt:expr, $msgs:expr) => {{
        $group.bench_function(BenchmarkId::new($label, $msgs.len()), |b| {
            b.iter_custom(|iters| {
                let msgs = $msgs.clone();
                $rt.block_on(async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let cap = msgs.len() + 1024;
                        let (p, mut c) = $module::memory_queue(cap);
                        for m in &msgs {
                            p.send(m.clone()).await.expect("send");
                        }
                        let start = std::time::Instant::now();
                        for _ in &msgs {
                            let claimed = c.claim().await.expect("claim").expect("non‑empty");
                            c.ack(&claimed.claim_id).await.expect("ack");
                        }
                        total += start.elapsed();
                    }
                    total
                })
            });
        });
    }};
}

fn bench_queue_send(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("queue_send");
    group.throughput(Throughput::Elements(SEND_COUNT));

    let msgs = pre_create(SEND_COUNT);

    bench_send_one!(group, "flume", flume, rt, msgs);
    bench_send_one!(group, "mpmc", mpmc, rt, msgs);
    bench_send_one!(group, "crossfire", crossfire, rt, msgs);

    group.finish();
}

fn bench_queue_recv(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("queue_recv");
    group.throughput(Throughput::Elements(RECV_COUNT));

    let msgs = pre_create(RECV_COUNT);

    bench_recv_one!(group, "flume", flume, rt, msgs);
    bench_recv_one!(group, "mpmc", mpmc, rt, msgs);
    bench_recv_one!(group, "crossfire", crossfire, rt, msgs);

    group.finish();
}

fn bench_queue_claim_ack(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("queue_claim_ack");
    group.throughput(Throughput::Elements(CLAIM_COUNT));

    let msgs = pre_create(CLAIM_COUNT);

    bench_claim_ack_one!(group, "flume", flume, rt, msgs);
    bench_claim_ack_one!(group, "mpmc", mpmc, rt, msgs);
    bench_claim_ack_one!(group, "crossfire", crossfire, rt, msgs);

    group.finish();
}

// ── System benchmarks ────────────────────────────────────────────────────────

/// TopicRouter::send() — WAL append + producer send (uses RecordingProducer mock).
fn bench_topic_send(c: &mut Criterion) {
    system_init();
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        TopicRouter::global().write().await.register_topic(bench_topic()).await;
    });

    let mut group = c.benchmark_group("system_send");
    group.throughput(Throughput::Elements(SEND_COUNT));

    let msgs = pre_create(SEND_COUNT);

    group.bench_function("TopicRouter::send", |b| {
        b.iter(|| {
            rt.block_on(async {
                for m in &msgs {
                    router
                        .send(bench_topic(), m.clone(), None, None)
                        .await
                        .expect("send");
                }
            });
        });
    });
    group.finish();
}

/// Helper: benchmark WAL append + real queue producer.send() — same operations
/// as `TopicRouter::send()` but with a concrete queue backend instead of the
/// global mock producer.  Avoids the single‑init constraint of TopicRouter.
macro_rules! bench_system_send_one {
    ($group:ident, $label:expr, $module:ident, $rt:expr, $msgs:expr) => {{
        let cap = $msgs.len() + 1024;
        let msgs = &$msgs;
        $group.bench_function(BenchmarkId::new($label, $msgs.len()), |b| {
            b.iter(|| {
                $rt.block_on(async {
                    let wal: Arc<tokio::sync::RwLock<Box<dyn Wal>>> =
                        Arc::new(tokio::sync::RwLock::new(Box::new(RecordingWal::new())));
                    let (raw_p, _c) = $module::memory_queue(cap);
                    let p: Arc<dyn EProducer> = Arc::new(raw_p);
                    for m in msgs {
                        let record = WalRecord::from_msg(m.clone());
                        let mut guard = wal.write().await;
                        guard.append(record).await.expect("wal append");
                        drop(guard);
                        p.send(m.clone()).await.expect("send");
                    }
                });
            });
        });
    }};
}

/// WAL append + real queue producer.send() — per queue impl.
fn bench_system_send_queue(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("system_send_queue");
    group.throughput(Throughput::Elements(SEND_COUNT));

    let msgs = pre_create(SEND_COUNT);

    bench_system_send_one!(group, "flume", flume, rt, msgs);
    bench_system_send_one!(group, "mpmc", mpmc, rt, msgs);
    bench_system_send_one!(group, "crossfire", crossfire, rt, msgs);

    group.finish();
}

/// Worker::test_process_msg — full pipeline: handler + middleware + WAL + audit.
///
/// Note: `Ack` handler never calls `self.producer.send()`, so the output
/// producer has no effect here.  The consumer is unused by `process_msg`.
fn bench_worker_process(c: &mut Criterion, label: &str, pipeline: Arc<Pipeline>) {
    system_init();
    let rt = Runtime::new().unwrap();
    let t = bench_topic();

    rt.block_on(async {
        ConsumerRouter::global().write().await
            .register(t, Arc::new(AckHandler))
            .await
            .expect("register");
    });

    let msgs = pre_create(PROCESS_COUNT);

    let mut group = c.benchmark_group("system_process");
    group.throughput(Throughput::Elements(PROCESS_COUNT));

    let wp = Arc::new(RecordingProducer::default());
    let w = Worker::new(
        t.into(),
        Box::new(NoopConsumer),
        pipeline,
        wp,
        None,
        Duration::from_millis(50),
        Some(Duration::from_millis(500)),
    );

    group.bench_function(BenchmarkId::new(label, PROCESS_COUNT), |b| {
        b.iter(|| {
            rt.block_on(async {
                for m in &msgs {
                    w.test_process_msg(m.clone()).await.expect("process");
                }
            });
        });
    });
    group.finish();

    rt.block_on(async {
        cr.del_workers(t).await.ok();
    });
}

/// Pipeline::run — multi‑worker parallel handler throughput (no WAL / audit).
///
/// Partitions `PROCESS_COUNT` messages across `worker_count` workers and
/// measures wall‑clock time for all to finish.
///
/// Note: this uses `Pipeline::run` directly rather than `Worker::test_process_msg`
/// to avoid global WAL + AuditManager lock contention, which artificially limits
/// parallel throughput.  Single‑worker WAL + audit overhead is measured separately
/// by `system_process/*`.
fn bench_worker_process_parallel(
    c: &mut Criterion,
    label: &str,
    pipeline: Arc<Pipeline>,
    worker_count: usize,
) {
    let rt = Runtime::new().unwrap();
    let msgs = pre_create(PROCESS_COUNT);

    let mut group = c.benchmark_group("system_process_parallel");
    group.throughput(Throughput::Elements(PROCESS_COUNT));

    group.bench_function(BenchmarkId::new(label, PROCESS_COUNT), |b| {
        b.iter_custom(|iters| {
            let msgs = msgs.clone();
            let pipeline = pipeline.clone();
            rt.block_on(async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let chunk_size = msgs.len() / worker_count;
                    let mut handles = vec![];

                    for i in 0..worker_count {
                        let start = i * chunk_size;
                        let end = if i == worker_count - 1 {
                            msgs.len()
                        } else {
                            start + chunk_size
                        };
                        let chunk: Vec<EMessage> = msgs[start..end].to_vec();

                        let pipeline = pipeline.clone();
                        handles.push(tokio::spawn(async move {
                            for mut m in chunk {
                                pipeline.run(&mut m).await;
                            }
                        }));
                    }

                    let start = std::time::Instant::now();
                    for h in handles {
                        h.await.unwrap();
                    }
                    total += start.elapsed();
                }
                total
            })
        });
    });
    group.finish();
}

// ── Full pipeline through real ConsumerRouter ──

use std::sync::atomic::{AtomicU64, Ordering};

struct CountingHandler(Arc<AtomicU64>);
#[async_trait]
impl EHandler for CountingHandler {
    async fn handler(&self, _msg: &EMessage) -> Ack {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ack::Ack
    }
}

/// Benchmarks the real CR dispatch path using flume MemoryQueueFactory.
/// CR is already initialized in `system_init()`.
fn bench_full_pipeline_cr(c: &mut Criterion, worker_count: usize) {
    system_init();
    let rt = Runtime::new().unwrap();
    let total = PROCESS_COUNT as usize;
    let count = Arc::new(AtomicU64::new(0));

    let topic = format!("bench-cr-{}w", worker_count);
    rt.block_on(async {
        let cr = ConsumerRouter::global().write().await;
        // Register + create workers only if not already done (OnceLock pattern)
        cr.register(&topic, Arc::new(CountingHandler(count.clone()))).await
            .or_else(|e| {
                if matches!(&e, event_base_core::error::CoreError::Topic(event_base_core::error::topic::TopicError::AlreadyExists(_))) {
                    Ok(())
                } else {
                    Err(e)
                }
            }).expect("register topic");
        // Check if workers already exist for this topic
        let existing = cr.get_workers(&topic).await;
        if existing.is_empty() {
            for _ in 0..worker_count {
                cr.create_worker(&topic, Arc::new(Pipeline::new(Box::new(CountingHandler(count.clone())))), None, None, None)
                    .await
                    .expect("create worker");
            }
        }
        drop(cr);
    });

    // Spawn CR's recv loop in background
    // Spawn CR's recv loop only once
    static CR_RECV_SPAWNED: std::sync::Once = std::sync::Once::new();
    CR_RECV_SPAWNED.call_once(|| {
        std::thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let _ = ConsumerRouter::global().read().await.recv().await;
            });
        });
    });

    // Pre‑create messages
    let all_msgs = pre_create(total as u64);
    let producer = BENCH_PRODUCER.get().expect("BENCH_PRODUCER not set").read().expect("BENCH_PRODUCER poisoned").clone();

    let mut group = c.benchmark_group("system_full_pipeline_cr");
    group.throughput(Throughput::Elements(total as u64));

    group.bench_function(BenchmarkId::new(format!("{}w", worker_count), total), |b| {
        b.iter_custom(|iters| {
            let count = count.clone();
            let msgs = all_msgs.clone();
            let producer = producer.clone();
            rt.block_on(async move {
                let mut total_dur = Duration::ZERO;
                for _ in 0..iters {
                    count.store(0, Ordering::Relaxed);
                    let start = std::time::Instant::now();
                    for m in &msgs {
                        producer.send(m.clone()).await.expect("send");
                    }
                    while count.load(Ordering::Relaxed) < total as u64 {
                        tokio::time::sleep(Duration::from_micros(100)).await;
                    }
                    total_dur += start.elapsed();
                }
                total_dur
            })
        });
    });
    group.finish();
}

// ── Full pipeline — all three backends (not via global CR singleton) ──

macro_rules! bench_full_pipeline_backend_one {
    ($group:ident, $label:expr, $module:ident, $rt:expr, $worker_count:expr) => {{
        let total = PROCESS_COUNT as usize;
        let n_per_worker = total / $worker_count;
        let all_msgs = pre_create(total as u64);
        let pipeline = Arc::new(Pipeline::new(Box::new(AckHandler)));

        let all_msgs = all_msgs; // move out of macro capture
        $group.bench_function(BenchmarkId::new($label, total), |b| {
            b.iter_custom(|iters| {
                let pipeline = pipeline.clone();
                let all_msgs = all_msgs.clone();
                $rt.block_on(async move {
                    let mut total_dur = Duration::ZERO;
                    for _ in 0..iters {
                        // Setup: create per-worker queues
                        let mut worker_rxs = Vec::with_capacity($worker_count);
                        let mut worker_txs = Vec::with_capacity($worker_count);
                        for _ in 0..$worker_count {
                            let (tx, rx) = $module::memory_queue(n_per_worker + 1024);
                            worker_txs.push(tx);
                            worker_rxs.push(rx);
                        }

                        // Dispatch: distribute round‑robin
                        for (i, m) in all_msgs.iter().enumerate() {
                            worker_txs[i % $worker_count]
                                .send(m.clone())
                                .await
                                .expect("dispatch");
                        }

                        // Timed: workers receive + pipeline.run
                        let start = std::time::Instant::now();
                        let mut handles = Vec::with_capacity($worker_count);
                        for mut rx in worker_rxs {
                            let pipeline = pipeline.clone();
                            handles.push(tokio::spawn(async move {
                                for _ in 0..n_per_worker {
                                    let mut msg = rx.receive().await.expect("receive");
                                    pipeline.run(&mut msg).await;
                                }
                            }));
                        }
                        for h in handles {
                            h.await.unwrap();
                        }
                        total_dur += start.elapsed();
                    }
                    total_dur
                })
            });
        });
    }};
}

fn bench_full_pipeline_backends(c: &mut Criterion, worker_count: usize) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group(format!("system_full_pipeline_backends_{}w", worker_count));
    group.throughput(Throughput::Elements(PROCESS_COUNT));

    bench_full_pipeline_backend_one!(group, "flume", flume, rt, worker_count);
    bench_full_pipeline_backend_one!(group, "mpmc", mpmc, rt, worker_count);
    bench_full_pipeline_backend_one!(group, "crossfire", crossfire, rt, worker_count);

    group.finish();
}

// ── Entry point ──────────────────────────────────────────────────────────────

fn benchmarks(c: &mut Criterion) {
    // ── Queue impl benchmarks ──
    bench_queue_send(c);
    bench_queue_recv(c);
    bench_queue_claim_ack(c);

    // ── System pipeline benchmarks ──
    bench_system_send_queue(c);
    bench_topic_send(c);

    bench_worker_process(
        c,
        "handler-only",
        Arc::new(Pipeline::new(Box::new(AckHandler))),
    );

    bench_worker_process(
        c,
        "handler+cpu",
        Arc::new(Pipeline::new(Box::new(CpuHandler))),
    );

    bench_worker_process(
        c,
        "handler+1mw",
        Arc::new(Pipeline::new(Box::new(AckHandler)).with(LoggingMiddleware)),
    );

    // ── Multi‑worker parallel benchmarks (4 workers) ──
    bench_worker_process_parallel(
        c,
        "handler-only-4w",
        Arc::new(Pipeline::new(Box::new(AckHandler))),
        4,
    );

    bench_worker_process_parallel(
        c,
        "handler+cpu-4w",
        Arc::new(Pipeline::new(Box::new(CpuHandler))),
        4,
    );

    bench_worker_process_parallel(
        c,
        "handler+1mw-4w",
        Arc::new(Pipeline::new(Box::new(AckHandler)).with(LoggingMiddleware)),
        4,
    );

    // ── Multi‑worker parallel benchmarks (8 workers) ──
    bench_worker_process_parallel(
        c,
        "handler-only-8w",
        Arc::new(Pipeline::new(Box::new(AckHandler))),
        8,
    );

    bench_worker_process_parallel(
        c,
        "handler+cpu-8w",
        Arc::new(Pipeline::new(Box::new(CpuHandler))),
        8,
    );

    bench_worker_process_parallel(
        c,
        "handler+1mw-8w",
        Arc::new(Pipeline::new(Box::new(AckHandler)).with(LoggingMiddleware)),
        8,
    );

    // ── CR dispatch: real ConsumerRouter with CountingHandler ──
    bench_full_pipeline_cr(c, 4);
    bench_full_pipeline_cr(c, 8);

    // ── Full pipeline — all three backends (not via global CR singleton) ──
    bench_full_pipeline_backends(c, 4);
    bench_full_pipeline_backends(c, 8);
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
