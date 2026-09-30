use crate::server::event_base::event_base_server::EventBase;
use crate::server::event_base::shutdown_request::Strategy;
use crate::server::event_base::{
    Empty, LatencyStats, ListNodeMetricsRequest, ListTopicsResponse, ListWorkersRequest,
    ListWorkersResponse, NodeMetrics, NodeStatusResponse, PublishRequest, PublishResponse,
    RespCheckResponse, ShutdownRequest, ShutdownResponse, TopicInfo, TopicStatsResponse, WorkerInfo,
};
use event_base_core::constant::SYSTEM_TOPIC_SHUTDOWN;
use event_base_core::message::DeliveryMode::{Broadcast, Standard};
use event_base_core::message::{EMessage, MessagePayload, MessageTopic};
use event_base_core::metrics::manager::MetricsManager;
use event_base_core::metrics::node_store::MetricsStore;
use event_base_core::shutdown::messages::ShutdownStrategy::{
    Batched, Force, Graceful, StateBasedIdle,
};
use event_base_core::shutdown::messages::{ShutdownCommand, ShutdownStrategy};
use event_base_core::topic::TopicRouter;
use event_base_core::worker_registry::WorkerRegistry;
use event_base_core::{NodeType, get_node_type};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tonic::{Request, Response, Status};

pub mod event_base {
    use tonic::include_proto;

    include_proto!("event_base");

    /// Encoded `FileDescriptorSet` emitted by `build.rs`, registered with the
    /// gRPC Server Reflection service so `grpcurl`/`buf` can introspect a
    /// running node without the `.proto` source.
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("event_base_descriptor");
}

#[derive(Default)]
pub struct EventBaseService;

#[tonic::async_trait]
impl EventBase for EventBaseService {
    async fn get_node_metrics(
        &self,
        request: Request<ListNodeMetricsRequest>,
    ) -> Result<Response<NodeMetrics>, Status> {
        let node_name = request.into_inner().node_name;
        if let Some(metrics) = MetricsStore::global()
            .read()
            .await
            .get_node(node_name.as_str())
            .await
        {
            return Ok(Response::new(proto_node_metrics(metrics)));
        }
        Err(Status::not_found(format!("No metrics found for node {node_name:?}")))
    }

    async fn list_workers(
        &self,
        request: Request<ListWorkersRequest>,
    ) -> Result<Response<ListWorkersResponse>, Status> {
        if get_node_type() == Arc::from(NodeType::Worker) {
            return Err(Status::failed_precondition(
                "ListWorkers is a Host-only RPC; this node is a Worker",
            ));
        }
        let topic = request.into_inner().topic;
        if let Ok(workers) = WorkerRegistry::global()
            .read()
            .await
            .get_workers(topic.as_str())
            .await
        {
            let mut response = ListWorkersResponse {
                total: workers.len() as u32,
                ..Default::default()
            };
            for worker in workers {
                let info = WorkerInfo {
                    worker_name: worker.worker_name,
                    topic: worker.topic,
                    last_heartbeat: worker
                        .last_heartbeat
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };
                response.workers.push(info);
            }
            return Ok(Response::new(response));
        };
        Err(Status::internal("worker registry read failed"))
    }

    async fn list_topics(&self, _: Request<Empty>) -> Result<Response<ListTopicsResponse>, Status> {
        let topic_list = TopicRouter::global().read().await.list_topics().await;
        Ok(Response::new(ListTopicsResponse {
            topics: topic_list.clone(),
            total: topic_list.len() as u32,
        }))
    }

    async fn get_topic_stats(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<TopicStatsResponse>, Status> {
        let snapshot = MetricsManager::global()
            .read()
            .await
            .snapshot()
            .await
            .business;

        let mut latency_sum_resp: HashMap<String, LatencyStats> = HashMap::new();

        for (topic, lat) in snapshot.latency_sum {
            let lat_resp = LatencyStats {
                count: lat.0,
                sum_duration_nanos: lat.1.as_nanos() as u64,
            };

            latency_sum_resp.insert(topic, lat_resp);
        }

        Ok(Response::new(TopicStatsResponse {
            info: Option::from(TopicInfo {
                enqueued: snapshot.enqueued,
                completed: snapshot.completed,
                failed: snapshot.failed,
                retried: snapshot.retried,
                latency_sum: latency_sum_resp,
            }),
        }))
    }

    async fn shutdown(
        &self,
        request: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, Status> {
        let command = request.into_inner();

        if let Some(strategy) = command.strategy {
            let shutdown_msg = match strategy {
                Strategy::TwoStage(ts) => ShutdownCommand {
                    strategy: ShutdownStrategy::TwoStage {
                        poll_interval_ms: ts.poll_interval_ms,
                        force_timeout_secs: ts.force_timeout_secs,
                    },
                },
                Strategy::Graceful(graceful) => ShutdownCommand {
                    strategy: Graceful {
                        worker_name: graceful.worker_name,
                        poll_interval_ms: graceful.poll_interval_ms,
                    },
                },
                Strategy::Force(..) => ShutdownCommand { strategy: Force },
                Strategy::StateBasedIdle(..) => ShutdownCommand {
                    strategy: StateBasedIdle,
                },
                Strategy::Batched(batched) => ShutdownCommand {
                    strategy: Batched {
                        batch_size: batched.batch_size as usize,
                        interval_ms: batched.interval_ms,
                    },
                },
                _ => {
                    return Err(Status::invalid_argument("Invalid strategy"));
                }
            };
            let msg = EMessage::new(
                MessageTopic(SYSTEM_TOPIC_SHUTDOWN.parse().unwrap()),
                MessagePayload(serde_json::to_vec(&shutdown_msg).unwrap()),
                Standard,
                None,
            );
            let result = TopicRouter::global()
                .read()
                .await
                .send(SYSTEM_TOPIC_SHUTDOWN, msg, None, None)
                .await;
            if let Err(e) = result {
                return Err(Status::internal(format!("[SHUTDOWN]: {}", e)));
            }
            return Ok(Response::new(ShutdownResponse { success: true }));
        }

        Err(Status::invalid_argument("No shutdown requested"))
    }

    async fn resp_check(&self, _: Request<Empty>) -> Result<Response<RespCheckResponse>, Status> {
        Ok(Response::new(RespCheckResponse {
            ready: true,
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }))
    }

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let req = request.into_inner();
        if req.topic.is_empty() {
            return Err(Status::invalid_argument("topic must not be empty"));
        }
        // Publish is the *business* injection door. System topics (`_system.*`)
        // are driven by their own dedicated RPCs (e.g. `Shutdown`) and by the
        // coordination plane; letting arbitrary clients forge discovery/heartbeat
        // metrics frames would corrupt worker state, so refuse them here.
        if req.topic.starts_with('_') {
            return Err(Status::invalid_argument(
                "Publish cannot target system topics (leading '_'); use the dedicated RPCs",
            ));
        }
        let mode = if req.broadcast { Broadcast } else { Standard };
        let to_worker = (!req.to_worker.is_empty()).then_some(req.to_worker.clone());
        let msg = EMessage::new(
            MessageTopic(req.topic.clone()),
            MessagePayload(req.payload),
            mode,
            to_worker,
        );
        let message_id = msg.id.clone();
        TopicRouter::global()
            .read()
            .await
            .send(&req.topic, msg, None, None)
            .await
            .map_err(|e| Status::internal(format!("[PUBLISH]: {e}")))?;
        Ok(Response::new(PublishResponse { message_id }))
    }

    async fn get_node_status(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<NodeStatusResponse>, Status> {
        let node_name = event_base_core::get_node_name();
        let is_host = get_node_type() != Arc::from(NodeType::Worker);
        let node_type = if is_host { 0 } else { 1 };

        let topic_count = TopicRouter::global()
            .read()
            .await
            .list_topics()
            .await
            .len() as u32;
        let worker_count = WorkerRegistry::global()
            .read()
            .await
            .get_all_workers()
            .await
            .map(|w| w.len() as u32)
            .unwrap_or(0);

        // Only a Host aggregates the fleet; a Worker reports just itself.
        let fleet = if is_host {
            MetricsStore::global()
                .read()
                .await
                .get_all_nodes()
                .await
                .into_iter()
                .map(proto_node_metrics)
                .collect()
        } else {
            Vec::new()
        };

        Ok(Response::new(NodeStatusResponse {
            ready: true,
            node_name,
            node_type,
            topic_count,
            worker_count,
            fleet,
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }))
    }
}

/// Converts a core [`NodeMetrics`](event_base_core::metrics::node::NodeMetrics)
/// sample into its protobuf wire form (single source for `GetNodeMetrics` and
/// the fleet list in `GetNodeStatus`).
fn proto_node_metrics(metrics: event_base_core::metrics::node::NodeMetrics) -> NodeMetrics {
    NodeMetrics {
        node_name: metrics.node_name,
        node_type: metrics.node_type as i32,
        cpu_percent: metrics.cpu_percent,
        memory_percent: metrics.memory_percent,
        node_worker_count: metrics.node_worker_count as u64,
        update_time: metrics
            .update_time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    }
}
