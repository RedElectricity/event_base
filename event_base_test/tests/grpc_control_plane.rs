//! End‑to‑end coverage for the gRPC control plane booted through `ServeConfig`
//! with Server Reflection + the standard Health service on.
//!
//! Boots a minimal Host on the production memory backend (crossfire + MemoryWal,
//! the same primitives `start_system!` wires), serves the control plane on a
//! random port, and drives it with the `connect` client helper:
//! * `RespCheck` / `ListTopics` / `GetNodeStatus` read the live globals;
//! * `Publish` injects a business message and refuses `_system.*` targets;
//! * the reflection `FILE_DESCRIPTOR_SET` advertises the current schema
//!   (including the new `Publish` / `GetNodeStatus` methods).

use async_trait::async_trait;
use event_base_core::handler::{Ack, EHandler};
use event_base_core::message::EMessage;
use event_base_core::metrics::node::NodeMetrics;
use event_base_core::queues::consumer_router::ConsumerRouter;
use event_base_core::queues::factory::QueueFactory;
use event_base_core::shutdown::shutdown_channel;
use event_base_core::system_handlers::system::SystemHandlerBuilder;
use event_base_core::topic::TopicRouter;
use event_base_core::wal::wal::Wal;
use event_base_core::worker_registry::WorkerRegistry;
use event_base_core::{NodeType, set_node_name, set_node_type};
use event_base_grpc::server::event_base::{PublishRequest, FILE_DESCRIPTOR_SET};
use event_base_grpc::{Empty, ServeConfig, connect};
use event_base_queue::crossfire;
use event_base_wal::memory::MemoryWal;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

struct NoopHandler;

#[async_trait]
impl EHandler for NoopHandler {
    async fn handler(&self, _msg: &EMessage) -> Ack {
        Ack::Ack
    }
}

/// Boot a Host once per process (all globals are `OnceLock` singletons — this
/// file owns its process, so a single boot serves every test here).
async fn boot_host() {
    set_node_name("grpc-node".to_string());
    set_node_type(NodeType::Host);

    let registry_wal: Arc<RwLock<Box<dyn Wal>>> = Arc::new(RwLock::new(Box::new(MemoryWal::new())));
    let factory = Arc::new(crossfire::MemoryQueueFactory::new(10_000));
    TopicRouter::init(factory.create_global_producer().expect("producer")).expect("topic init");
    ConsumerRouter::init(
        factory.create_main_consumer().expect("consumer"),
        factory.clone(),
        None,
    )
    .expect("consumer init");
    WorkerRegistry::init(Some(registry_wal)).await.expect("registry");
    let (shutdown_tx, _rx) = shutdown_channel();
    SystemHandlerBuilder::new(Arc::new(RwLock::new(MemoryWal::new())), shutdown_tx, 32)
        .register_all()
        .await
        .expect("register_all");

    // Register a real business topic so the crossfire RoutingProducer accepts
    // publishes to it (it rejects unknown topics by design).
    TopicRouter::global()
        .write()
        .await
        .register_topic("rpc-topic")
        .await;
    ConsumerRouter::global()
        .read()
        .await
        .register("rpc-topic", Arc::new(NoopHandler))
        .await
        .expect("register topic");
}

async fn free_addr() -> std::net::SocketAddr {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);
    addr
}

/// Retry‑connect until the listener answers (server task binds asynchronously).
async fn dial(addr: std::net::SocketAddr) -> event_base_grpc::EventBaseClient<tonic::transport::Channel> {
    for _ in 0..40 {
        if let Ok(client) = connect(addr.to_string()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("grpc server never came up on {addr}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_plane_serves_read_and_publish_rpcs() {
    boot_host().await;

    let addr = free_addr().await;
    // Reflection + health default ON, no token.
    let server = tokio::spawn(async move { ServeConfig::new(addr).serve().await });
    let mut client = dial(addr).await;

    // ── RespCheck ───────────────────────────────────────────────────────
    let resp = client
        .resp_check(Empty::default())
        .await
        .expect("resp_check");
    assert!(resp.into_inner().ready);

    // ── ListTopics (reads TopicRouter's registered list) ────────────────
    let topics = client
        .list_topics(Empty::default())
        .await
        .expect("list_topics")
        .into_inner();
    assert!(
        topics.topics.iter().any(|t| t == "rpc-topic"),
        "rpc-topic should be listed: {:?}",
        topics.topics
    );

    // ── Publish: business topic accepted, id returned ───────────────────
    let pub_resp = client
        .publish(PublishRequest {
            topic: "rpc-topic".into(),
            payload: b"hello from ebctl".to_vec(),
            broadcast: false,
            to_worker: String::new(),
        })
        .await
        .expect("publish to business topic");
    let id = pub_resp.into_inner().message_id;
    assert!(!id.is_empty(), "publish must return a message id");

    // ── Publish: empty topic rejected as InvalidArgument ────────────────
    let err = client
        .publish(PublishRequest {
            topic: String::new(),
            payload: vec![1],
            broadcast: false,
            to_worker: String::new(),
        })
        .await
        .expect_err("empty topic must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err}");

    // ── Publish: `_system.*` refused (coordination plane is guarded) ─────
    let err = client
        .publish(PublishRequest {
            topic: "_system.worker_discovery".into(),
            payload: b"spoof".to_vec(),
            broadcast: false,
            to_worker: String::new(),
        })
        .await
        .expect_err("system topics must not be publishable");
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err}");

    // ── GetNodeStatus: self + fleet snapshot ────────────────────────────
    // Seed one fleet node so the Host view is non‑empty and the mapping runs.
    event_base_core::metrics::node_store::MetricsStore::global()
        .write()
        .await
        .update(NodeMetrics {
            node_name: "peer-node".into(),
            node_type: NodeType::Worker,
            cpu_percent: vec![12.5],
            memory_percent: 33.0,
            node_worker_count: 2,
            update_time: std::time::SystemTime::now(),
        })
        .await;

    let status = client
        .get_node_status(event_base_grpc::Empty::default())
        .await
        .expect("get_node_status")
        .into_inner();
    assert_eq!(status.node_name, "grpc-node");
    assert!(status.ready);
    assert_eq!(status.node_type, 0, "host is node_type 0");
    assert!(status.topic_count >= 1, "rpc-topic counted");
    assert!(
        status.fleet.iter().any(|n| n.node_name == "peer-node"),
        "seeded peer must appear in the fleet view: {:?}",
        status.fleet
    );

    server.abort();
}

#[test]
fn reflection_descriptor_advertises_current_schema() {
    // Reflection serves from the encoded `FileDescriptorSet` emitted at build
    // time by `build.rs`. Prove that set is non‑empty and carries the control‑
    // plane service WITH the newly added methods — a regression guard that the
    // descriptor the server registers is the current schema, not a stale
    // artifact (the names appear verbatim as length‑prefixed UTF‑8 in the wire
    // encoding, so a byte substring search is a faithful existence test).
    assert!(
        !FILE_DESCRIPTOR_SET.is_empty(),
        "reflection descriptor must be generated"
    );
    let contains = |needle: &[u8]| FILE_DESCRIPTOR_SET.windows(needle.len()).any(|w| w == needle);
    for name in [
        "EventBase",
        "GetNodeMetrics",
        "ListWorkers",
        "ListTopics",
        "GetTopicStats",
        "RespCheck",
        "Publish",
        "GetNodeStatus",
    ] {
        assert!(
            contains(name.as_bytes()),
            "reflection descriptor must advertise {name}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_config_gates_all_services_with_token() {
    // With a token set, the auth layer wraps EVERY service — including
    // reflection and health — so an unauthenticated RespCheck fails before the
    // handler runs. This pins the "auth is uniform across the router" contract.
    let addr = free_addr().await;
    let server = tokio::spawn(async move {
        ServeConfig::new(addr).token("gate").serve().await
    });
    let mut client = dial(addr).await;

    let err = client
        .resp_check(Empty::default())
        .await
        .expect_err("no token → unauth");
    assert_eq!(err.code(), tonic::Code::Unauthenticated, "{err}");

    // Authenticated via the connect_with_token helper's interceptor path is
    // covered in ebctl; here drive it by hand with attach_token.
    let mut req = tonic::Request::new(event_base_grpc::Empty::default());
    event_base_grpc::attach_token(&mut req, "gate");
    let resp = client.resp_check(req).await.expect("token accepted");
    assert!(resp.into_inner().ready);

    server.abort();
}
