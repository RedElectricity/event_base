//! End‑to‑end proof of the two config‑feature deliverables in the umbrella
//! crate: typed `eb.toml` parsing, and a `Bootstrap` that boots a Host and
//! dispatches to a `#[handler]`‑registered worker — the "copy‑paste quick‑start
//! that actually compiles and runs" the crate previously lacked.

use event_base::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

static HITS: AtomicUsize = AtomicUsize::new(0);

/// Registered into the linkme slice at compile time; wired by `Bootstrap::start`
/// → `register_all_handlers`.
#[handler(topic = "echo", workers = 1)]
async fn echo(_msg: &EMessage) -> Ack {
    HITS.fetch_add(1, Ordering::SeqCst);
    Ack::Ack
}

// This file owns the process‑wide OnceLock singletons; exactly one boot test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_host_boots_and_dispatches() {
    let running = Bootstrap::host("boot-node").start().await.expect("bootstrap start");
    assert_eq!(running.node_name(), "boot-node");

    let msg = EMessage::new(
        MessageTopic("echo".into()),
        MessagePayload(b"ping".to_vec()),
        DeliveryMode::Standard,
        None,
    );
    event_base::core::topic::TopicRouter::global()
        .read()
        .await
        .send("echo", msg, None, None)
        .await
        .expect("send must route to the handler‑created topic");

    // Dispatch runs on the spawned consumer loop; poll for the handler hit.
    for _ in 0..200 {
        if HITS.load(Ordering::SeqCst) >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        HITS.load(Ordering::SeqCst) >= 1,
        "the #[handler] worker should have processed the published message"
    );

    // shutdown() must reach the SAME channel the built‑in handlers use — the
    // pre‑0.8 boot minted a second one so this returned 0 (nobody listening).
    let receivers = running.shutdown();
    assert!(
        receivers >= 1,
        "shutdown must fan out to live receivers (got {receivers})"
    );
}

#[test]
fn node_config_parses_and_rejects_typos() {
    let toml = r#"
        [node]
        name = "orders"
        role = "host"

        [queue]
        backend = "memory"
        capacity = 5000

        [wal]
        backend = "memory"

        [grpc]
        addr = "127.0.0.1:50051"
        token = "sekret"
        reflection = false
    "#;
    let cfg = NodeConfig::from_toml_str(toml).expect("valid eb.toml parses");
    assert_eq!(cfg.node.name, "orders");
    assert_eq!(cfg.queue.capacity, 5000);
    assert_eq!(cfg.audit_capacity(), 1024, "0 defaults to 1024");
    let grpc = cfg.grpc.expect("grpc section present");
    assert_eq!(grpc.token, "sekret");
    assert!(!grpc.reflection, "explicit false is honored");
    assert!(grpc.health, "unset health defaults to true");

    // A typo'd key must be a hard error, not a silent ignore.
    let bad = r#"
        [node]
        name = "x"
        roly = "host"
    "#;
    let err = NodeConfig::from_toml_str(bad).expect_err("unknown key must be rejected");
    assert!(err.contains("roly"), "error names the offending key: {err}");
}
