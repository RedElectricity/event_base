//! End‑to‑end system benchmarks — exercises the full message pipeline:
//! handler, middleware, WAL sync, audit, TopicRouter send.
//!
//! - `system_send`: 100K msgs through TopicRouter::send (WAL + producer)
//! - `system_process`: 50K msgs through Worker::test_process_msg (handler + pipeline + WAL + audit)

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
use event_base_core::wal::wal::Wal;
use event_base_core::worker::Worker;
use event_base_core::worker_registry::WorkerRegistry;
use event_base_core::{set_node_name, set_node_type, NodeType};
use event_base_test::support::{RecordingProducer, RecordingWal};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

// ── Config ───────────────────────────────────────────────────────────────────

const SEND_COUNT: u64 = 50_000;
const PROCESS_COUNT: u64 = 10_000;
static SYSTEM_INIT: std::sync::Once = std::sync::Once::new();

fn bench_topic() -> &'static str {
    "bench-sys"
}

/// Pre‑create N messages with empty payload (avoid UUID overhead in loop).
fn pre_create(n: u64) -> Vec<EMessage> {
    (0..n)
        .map(|_| {
            EMessage::new(
                MessageTopic(bench_topic().into()),
                MessagePayload(Vec::new()),
                DeliveryMode::Standard,
                None,
            )
        })
        .collect()
}

// ── Minimal infra ────────────────────────────────────────────────────────────

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

// ── System setup (once per process) ──────────────────────────────────────────

fn system_init() {
    SYSTEM_INIT.call_once(|| {
        let _ = set_node_name("bench-node".to_string());
        let _ = set_node_type(NodeType::Host);

        let fake_wal = RecordingWal::new();
        let wal: Arc<tokio::sync::RwLock<Box<dyn Wal>>> =
            Arc::new(tokio::sync::RwLock::new(Box::new(fake_wal)));

        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let _ = WorkerRegistry::init(Some(wal.clone())).await;
            let gp = Arc::new(RecordingProducer::default());
            let _ = TopicRouter::init(wal, gp.clone());
            let _ = AuditManager::init(1024);
            let _ = event_base_core::metrics::manager::MetricsManager::init();
            let _ = event_base_core::metrics::node_store::MetricsStore::init();
            let f = Arc::new(NoopQF { p: gp });
            let mc = f.create_main_consumer().unwrap();
            let _ = ConsumerRouter::init(mc, f);
        });
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
        criterion::black_box(s);
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

// ── Benchmarks ───────────────────────────────────────────────────────────────

/// TopicRouter::send() — WAL append + producer send.
fn bench_topic_send(c: &mut Criterion) {
    system_init();
    let rt = Runtime::new().unwrap();
    let router = TopicRouter::global();
    rt.block_on(async {
        router.register_topic(bench_topic()).await;
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

/// Worker::test_process_msg — full pipeline: handler + middleware + WAL + audit.
fn bench_worker_process(c: &mut Criterion, label: &str, pipeline: Arc<Pipeline>) {
    system_init();
    let rt = Runtime::new().unwrap();
    let cr = ConsumerRouter::global();
    let t = bench_topic();

    rt.block_on(async {
        cr.register(t, Arc::new(AckHandler))
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

// ── Entry point ──────────────────────────────────────────────────────────────

fn benchmarks(c: &mut Criterion) {
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
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
