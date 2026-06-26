use crate::server::event_base::event_base_server::EventBase;
use crate::server::event_base::shutdown_request::Strategy;
use crate::server::event_base::{
    Empty, LatencyStats, ListNodeMetricsRequest, ListTopicsResponse, ListWorkersRequest,
    ListWorkersResponse, NodeMetrics, RespCheckResponse, ShutdownRequest, ShutdownResponse,
    TopicInfo, TopicStatsResponse, WorkerInfo,
};
use event_base_core::constant::SYSTEM_TOPIC_SHUTDOWN;
use event_base_core::message::DeliveryMode::Standard;
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
        if let Some(metrics) = MetricsStore::global().get_node(node_name.as_str()).await {
            return Ok(Response::new(NodeMetrics {
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
            }));
        }
        Err(Status::invalid_argument("No metrics found for this node"))
    }

    async fn list_workers(
        &self,
        request: Request<ListWorkersRequest>,
    ) -> Result<Response<ListWorkersResponse>, Status> {
        if get_node_type() == Arc::from(NodeType::Worker) {
            return Err(Status::invalid_argument("Worker type is not supported"));
        }
        let topic = request.into_inner().topic;
        if let Ok(workers) = WorkerRegistry::global().get_workers(topic.as_str()).await {
            let mut response: ListWorkersResponse = ListWorkersResponse::default();
            response.total = workers.len() as u32;
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
        Err(Status::invalid_argument("No workers found for this topic"))
    }

    async fn list_topics(&self, _: Request<Empty>) -> Result<Response<ListTopicsResponse>, Status> {
        let topic_list = TopicRouter::global().list_topics().await;
        Ok(Response::new(ListTopicsResponse {
            topics: topic_list.clone(),
            total: topic_list.len() as u32,
        }))
    }

    async fn get_topic_stats(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<TopicStatsResponse>, Status> {
        let snapshot = MetricsManager::global().snapshot().await.business;

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
            let shutdown_msg: ShutdownCommand;
            match strategy {
                Strategy::TwoStage(ts) => {
                    shutdown_msg = ShutdownCommand {
                        strategy: ShutdownStrategy::TwoStage {
                            poll_interval_ms: ts.poll_interval_ms,
                            force_timeout_secs: ts.force_timeout_secs,
                        },
                    };
                }
                Strategy::Graceful(graceful) => {
                    shutdown_msg = ShutdownCommand {
                        strategy: Graceful {
                            worker_name: graceful.worker_name,
                            poll_interval_ms: graceful.poll_interval_ms,
                        },
                    }
                }
                Strategy::Force(..) => shutdown_msg = ShutdownCommand { strategy: Force },
                Strategy::StateBasedIdle(..) => {
                    shutdown_msg = ShutdownCommand {
                        strategy: StateBasedIdle,
                    }
                }
                Strategy::Batched(batched) => {
                    shutdown_msg = ShutdownCommand {
                        strategy: Batched {
                            batch_size: batched.batch_size as usize,
                            interval_ms: batched.interval_ms,
                        },
                    }
                }
                _ => {
                    return Err(Status::invalid_argument("Invalid strategy"));
                }
            }
            let msg = EMessage::new(
                MessageTopic(SYSTEM_TOPIC_SHUTDOWN.parse().unwrap()),
                MessagePayload(serde_json::to_vec(&shutdown_msg).unwrap()),
                Standard,
                None,
            );
            let result = TopicRouter::global()
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
}
