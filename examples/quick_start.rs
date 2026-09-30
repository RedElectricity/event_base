//! The canonical `event_base` starter — this file is compiled by CI
//! (`cargo test --workspace` builds examples) so the documented quick‑start is
//! guaranteed to build. It is the same program the `template/` directory
//! scaffolds for `cargo generate`.
//!
//! Run it:
//! ```text
//! cargo run --example quick_start
//! ```
//!
//! It boots a Host via [`Bootstrap`], registers one `#[handler]`, publishes a
//! message through the typed control plane, waits for the worker to see it, and
//! shuts down — the whole lifecycle in ~30 lines.

use event_base::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Counts how many times the handler ran (proof the message was dispatched).
static SEEN: AtomicUsize = AtomicUsize::new(0);

/// A handler on the `greeting` topic. The `#[handler]` attribute registers it
/// at compile time; `Bootstrap::start` wires its queue + workers.
#[handler(topic = "greeting", workers = 2)]
async fn greet(msg: &EMessage) -> Ack {
    let text = String::from_utf8_lossy(&msg.payload.0);
    println!("[{}] received: {text}", msg.id);
    SEEN.fetch_add(1, Ordering::SeqCst);
    Ack::Ack
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Boot a single in‑memory Host. Swap `host()` for
    //    `Bootstrap::from_file("eb.toml")?` to drive role/backend/gRPC from
    //    config instead of code.
    let node = Bootstrap::host("demo-node").start().await?;
    println!("node {} is up", node.node_name());

    // 2. Publish a message on the `greeting` topic.
    let msg = EMessage::new(
        MessageTopic("greeting".into()),
        MessagePayload(b"hello from quick_start".to_vec()),
        DeliveryMode::Standard,
        None,
    );
    node.shutdown_sender(); // (illustrative: hand this to a signal handler)
    event_base::core::topic::TopicRouter::global()
        .read()
        .await
        .send("greeting", msg, None, None)
        .await?;

    // 3. Give the worker loop a moment to process, then shut down cleanly.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    println!("handler saw {} message(s)", SEEN.load(Ordering::SeqCst));

    node.shutdown();
    // node.wait().await;  // in a real daemon you'd await ctrl‑C/shutdown here.
    Ok(())
}
