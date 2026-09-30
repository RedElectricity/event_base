//! `ebctl` — the operator / debugging CLI for an `event_base` node's gRPC
//! control plane.
//!
//! It speaks the same protocol the framework serves, so anything a running
//! node exposes (`status`, topics, workers, metrics) can be inspected and
//! driven from a shell — the ergonomic layer the raw control plane was missing.
//!
//! ```text
//! ebctl -a 127.0.0.1:50051 status
//! ebctl -t "$EB_TOKEN" publish -T orders -p '{"id":7}'
//! ebctl shutdown --force
//! ```
//!
//! All commands take `--addr` (default `127.0.0.1:50051`) and `--token` (also
//! readable from `EBCTL_TOKEN`/`EBCTL_ADDR`), and exit non‑zero with the server's
//! gRPC status on failure.

use clap::{Parser, Subcommand};
use event_base_grpc::server::event_base::shutdown_request::{
    Batched, Force, Graceful, StateBasedIdle, Strategy, Timeout, TwoStage,
};
use event_base_grpc::server::event_base::{
    Empty, ListNodeMetricsRequest, ListWorkersRequest, PublishRequest, ShutdownRequest,
};
use std::error::Error;

#[derive(Parser)]
#[command(name = "ebctl", version, about = "event_base control‑plane CLI")]
struct Cli {
    /// Control‑plane address (host:port).
    #[arg(
        short = 'a',
        long,
        default_value = "127.0.0.1:50051",
        env = "EBCTL_ADDR"
    )]
    addr: String,

    /// Bearer token, if the node requires one.
    #[arg(short = 't', long, env = "EBCTL_TOKEN", default_value = "")]
    token: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Liveness check (RespCheck).
    Ping,
    /// Node + fleet status snapshot (GetNodeStatus).
    Status,
    /// List registered topics (ListTopics).
    Topics,
    /// List workers, optionally for one topic (ListWorkers).
    Workers {
        #[arg(short = 'T', long)]
        topic: Option<String>,
    },
    /// Per‑topic throughput / latency (GetTopicStats).
    Metrics,
    /// Metrics for one node by name (GetNodeMetrics).
    NodeMetrics {
        #[arg(short = 'n', long)]
        node: String,
    },
    /// Publish a message onto a business topic (Publish).
    Publish {
        /// Target topic (must be registered on the node).
        #[arg(short = 'T', long)]
        topic: String,
        /// Payload text. Pass `-` to read all of stdin.
        #[arg(short = 'p', long, default_value = "")]
        payload: String,
        /// Deliver as Broadcast (fan‑out to every worker on the topic).
        #[arg(long)]
        broadcast: bool,
        /// Target one specific worker by name.
        #[arg(long)]
        to_worker: Option<String>,
    },
    /// Request a fleet shutdown (Shutdown).
    Shutdown {
        #[command(subcommand)]
        strategy: ShutdownStrategy,
    },
}

#[derive(Subcommand)]
enum ShutdownStrategy {
    /// Stop immediately.
    Force,
    /// Wait for a named worker to finish in‑flight work.
    Graceful {
        #[arg(short = 'w', long)]
        worker: String,
        /// Poll interval in ms.
        #[arg(long, default_value_t = 1000)]
        poll_interval_ms: u64,
    },
    /// Stop once the system is idle.
    Idle,
    /// Two‑stage: wait, then force after a timeout.
    TwoStage {
        #[arg(long, default_value_t = 1000)]
        poll_interval_ms: u64,
        #[arg(long, default_value_t = 10)]
        force_timeout_secs: u64,
    },
    /// Force after an overall timeout.
    Timeout {
        #[arg(long, default_value_t = 30)]
        total_timeout_secs: u64,
    },
    /// Shut workers down in batches.
    Batched {
        #[arg(long, default_value_t = 4)]
        batch_size: u64,
        #[arg(long, default_value_t = 500)]
        interval_ms: u64,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    // One client type for every case: connect_with_token with an empty token
    // sends no `authorization` header, so it works against unauthenticated
    // servers too (AuthInterceptor skips the empty token).
    let mut client = event_base_grpc::connect_with_token(&cli.addr, &cli.token).await?;

    match cli.command {
        Command::Ping => {
            let r = client.resp_check(Empty::default()).await?.into_inner();
            println!("ready={} timestamp={}", r.ready, r.timestamp);
        }
        Command::Status => {
            let s = client.get_node_status(Empty::default()).await?.into_inner();
            let role = if s.node_type == 0 { "Host" } else { "Worker" };
            println!("node  : {} ({role})", s.node_name);
            println!("ready : {}", s.ready);
            println!("topics: {}", s.topic_count);
            println!("workers: {}", s.worker_count);
            if !s.fleet.is_empty() {
                println!("fleet ({} known nodes):", s.fleet.len());
                for n in s.fleet {
                    let cpu = if n.cpu_percent.is_empty() {
                        "-".to_string()
                    } else {
                        n.cpu_percent
                            .iter()
                            .map(|c| format!("{c:.1}"))
                            .collect::<Vec<_>>()
                            .join(",")
                    };
                    println!(
                        "  {:<20} type={} workers={} mem={:.1}% cpu={}",
                        n.node_name, n.node_type, n.node_worker_count, n.memory_percent, cpu
                    );
                }
            }
        }
        Command::Topics => {
            let t = client.list_topics(Empty::default()).await?.into_inner();
            println!("{} topic(s):", t.total);
            for topic in t.topics {
                println!("  {topic}");
            }
        }
        Command::Workers { topic } => {
            let topic = match topic {
                Some(t) => t,
                None => {
                    // No --topic: enumerate topics, then workers per topic.
                    let topics = client.list_topics(Empty::default()).await?.into_inner();
                    for topic in topics.topics {
                        let w = client
                            .list_workers(ListWorkersRequest {
                                topic: topic.clone(),
                            })
                            .await?
                            .into_inner();
                        if !w.workers.is_empty() {
                            println!("[{topic}] {} worker(s)", w.total);
                            for worker in w.workers {
                                println!("  {} (hb={})", worker.worker_name, worker.last_heartbeat);
                            }
                        }
                    }
                    return Ok(());
                }
            };
            let w = client
                .list_workers(ListWorkersRequest {
                    topic: topic.clone(),
                })
                .await?
                .into_inner();
            println!("[{topic}] {} worker(s)", w.total);
            for worker in w.workers {
                println!("  {} (hb={})", worker.worker_name, worker.last_heartbeat);
            }
        }
        Command::Metrics => {
            let m = client.get_topic_stats(Empty::default()).await?.into_inner();
            match m.info {
                None => println!("(no metrics collected yet)"),
                Some(info) => {
                    let keys = collect_keys(&[
                        &info.enqueued,
                        &info.completed,
                        &info.failed,
                        &info.retried,
                    ]);
                    if keys.is_empty() {
                        println!("(empty stats)");
                    }
                    for topic in keys {
                        let g = |map: &std::collections::HashMap<String, u64>| {
                            map.get(topic.as_str()).copied().unwrap_or(0)
                        };
                        println!(
                            "  {topic:<20} enq={} done={} fail={} retry={}",
                            g(&info.enqueued),
                            g(&info.completed),
                            g(&info.failed),
                            g(&info.retried),
                        );
                    }
                }
            }
        }
        Command::NodeMetrics { node } => {
            let n = client
                .get_node_metrics(ListNodeMetricsRequest {
                    node_name: node.clone(),
                })
                .await?
                .into_inner();
            println!(
                "node {}: type={} workers={} mem={:.1}%",
                n.node_name, n.node_type, n.node_worker_count, n.memory_percent
            );
        }
        Command::Publish {
            topic,
            payload,
            broadcast,
            to_worker,
        } => {
            let bytes = if payload == "-" {
                use std::io::Read;
                let mut buf = Vec::new();
                std::io::stdin().read_to_end(&mut buf)?;
                buf
            } else {
                payload.into_bytes()
            };
            let r = client
                .publish(PublishRequest {
                    topic: topic.clone(),
                    payload: bytes,
                    broadcast,
                    to_worker: to_worker.unwrap_or_default(),
                })
                .await?
                .into_inner();
            println!("published to {topic}: message_id={}", r.message_id);
        }
        Command::Shutdown { strategy } => {
            let strategy = match strategy {
                ShutdownStrategy::Force => Strategy::Force(Force {}),
                ShutdownStrategy::Idle => Strategy::StateBasedIdle(StateBasedIdle {}),
                ShutdownStrategy::Graceful {
                    worker,
                    poll_interval_ms,
                } => Strategy::Graceful(Graceful {
                    worker_name: worker,
                    poll_interval_ms,
                }),
                ShutdownStrategy::TwoStage {
                    poll_interval_ms,
                    force_timeout_secs,
                } => Strategy::TwoStage(TwoStage {
                    poll_interval_ms,
                    force_timeout_secs,
                }),
                ShutdownStrategy::Timeout { total_timeout_secs } => {
                    Strategy::Timeout(Timeout { total_timeout_secs })
                }
                ShutdownStrategy::Batched {
                    batch_size,
                    interval_ms,
                } => Strategy::Batched(Batched {
                    batch_size,
                    interval_ms,
                }),
            };
            let r = client
                .shutdown(ShutdownRequest {
                    strategy: Some(strategy),
                })
                .await?
                .into_inner();
            println!("shutdown accepted={}", r.success);
        }
    }

    Ok(())
}

/// Union of the map keys across several topic→count maps, sorted, so the
/// metrics table lists every topic seen in any column.
fn collect_keys(maps: &[&std::collections::HashMap<String, u64>]) -> Vec<String> {
    let mut set = std::collections::BTreeSet::new();
    for m in maps {
        for k in m.keys() {
            set.insert(k.clone());
        }
    }
    set.into_iter().collect()
}
