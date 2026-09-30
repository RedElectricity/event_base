//! Starter `event_base` application. Boots from `eb.toml`, registers one
//! handler, and blocks until shutdown (Ctrl‑C or a `Shutdown` control‑plane
//! command). Edit the handler + topic and you have an app skeleton.

use event_base::prelude::*;

/// Replace `jobs` with your first topic. Handlers are registered at compile
/// time; `Bootstrap::start` wires their queue + workers.
#[handler(topic = "jobs", workers = 4)]
async fn handle_job(msg: &EMessage) -> Ack {
    let body = String::from_utf8_lossy(&msg.payload.0);
    println!("[{}] processing: {body}", msg.id);
    // Return Ack::Ack on success, Ack::NoAck { .. } to retry, or
    // Ack::Dead { .. } to send straight to the dead‑letter queue.
    Ack::Ack
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Boot from eb.toml (role, queue/WAL backends, optional gRPC control plane).
    // Falls back to an in‑memory Host if no file is present.
    let node = match std::fs::File::open("eb.toml") {
        Ok(_) => Bootstrap::from_file("eb.toml")?.start().await?,
        Err(_) => Bootstrap::host("my-app").start().await?,
    };
    println!("{} running — Ctrl‑C to stop", node.node_name());

    // Emit one message so a fresh checkout shows the handler firing.
    let msg = EMessage::new(
        MessageTopic("jobs".into()),
        MessagePayload(b"first job".to_vec()),
        DeliveryMode::Standard,
        None,
    );
    event_base::core::topic::TopicRouter::global()
        .read()
        .await
        .send("jobs", msg, None, None)
        .await?;

    // Block until Ctrl‑C or a shutdown signal, then drain the gRPC task.
    node.wait().await;
    Ok(())
}
