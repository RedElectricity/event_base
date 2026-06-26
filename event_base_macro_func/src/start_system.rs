use event_base_core::constant::SYSTEM_TOPIC_WORKER_DISCOVERY;
use event_base_core::error::CoreError;
use event_base_core::message::DeliveryMode::Standard;
use event_base_core::message::{EMessage, MessagePayload, MessageTopic};
use event_base_core::queues::EProducer;
use event_base_core::shutdown::{ShutdownSender, shutdown_channel};
use event_base_core::system_handlers::system::SystemHandlerBuilder;
use event_base_core::system_handlers::topic::TopicDiscoveryMessage;
use event_base_core::topic::TopicRouter;
use event_base_core::trace_layer::TraceLayer;
use event_base_core::wal::wal::Wal;
use event_base_core::{NodeType, set_node_type};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub async fn start_system_impl(
    node_type: NodeType,
    producer: Arc<dyn EProducer>,
    wal: Box<dyn Wal>,
    system_builder: SystemHandlerBuilder,
) -> Result<ShutdownSender, CoreError> {
    set_node_type(node_type);

    let wal_init = RwLock::new(wal);
    TopicRouter::init(wal_init, producer)?;

    let (shutdown_tx, _) = shutdown_channel();

    system_builder.register_all().await?;

    event_base_core::registry::register_all_handlers(shutdown_tx.clone()).await?;
    let router = TopicRouter::global();

    let producer = router.get_producer();
    let trace_layer = TraceLayer::new(producer);

    Registry::default().with(trace_layer).init();

    let topics_discovery_msg = EMessage::new(
        MessageTopic(SYSTEM_TOPIC_WORKER_DISCOVERY.parse().unwrap()),
        MessagePayload(
            serde_json::to_vec(&TopicDiscoveryMessage {
                has_topics: router.list_topics().await,
            })
            .unwrap(),
        ),
        Standard,
        None,
    );

    router
        .send(
            SYSTEM_TOPIC_WORKER_DISCOVERY,
            topics_discovery_msg,
            None,
            None,
        )
        .await
        .expect("[START UP] Failed to send topic discovery message");

    tokio::spawn(TopicRouter::run_delay_scheduler());

    Ok(shutdown_tx)
}
