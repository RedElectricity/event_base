use event_base_core::constant::SYSTEM_TOPIC_TOPIC_DISCOVERY;
use event_base_core::error::CoreError;
use event_base_core::message::DeliveryMode::Standard;
use event_base_core::message::{EMessage, MessagePayload, MessageTopic};
use event_base_core::queues::consumer_router::ConsumerRouter;
use event_base_core::queues::factory::QueueFactory;
use event_base_core::shutdown::{shutdown_channel, ShutdownSender};
use event_base_core::system_handlers::system::SystemHandlerBuilder;
use event_base_core::system_handlers::topic::TopicDiscoveryMessage;
use event_base_core::topic::TopicRouter;
use event_base_core::trace_layer::TraceLayer;
use event_base_core::wal::wal::Wal;
use event_base_core::worker_registry::WorkerRegistry;
use event_base_core::NodeType::Host;
use event_base_core::{set_node_type, NodeType};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Registry;

pub async fn start_system_impl(
    node_type: NodeType,
    factory: Arc<dyn QueueFactory>,
    wal: Box<dyn Wal>,
    system_builder: SystemHandlerBuilder,
) -> Result<ShutdownSender, CoreError> {
    set_node_type(node_type);

    let wal_init = Arc::new(RwLock::new(wal));
    let global_producer = factory.create_global_producer()?;
    TopicRouter::init(wal_init.clone(), global_producer)?;

    let main_consumer = factory.create_main_consumer()?;
    ConsumerRouter::init(main_consumer, factory)?;

    WorkerRegistry::init(Option::from(wal_init.clone())).await?;

    let (shutdown_tx, _) = shutdown_channel();

    system_builder.register_all().await?;

    event_base_core::registry::register_all_handlers(shutdown_tx.clone()).await?;

    let cr = ConsumerRouter::global();
    tokio::spawn(async move {
        let _ = cr.recv().await;
    });

    let router = TopicRouter::global();

    let producer = router.get_producer();
    let trace_layer = TraceLayer::new(producer);
    Registry::default().with(trace_layer).init();

    let topics_discovery_msg = EMessage::new(
        MessageTopic(SYSTEM_TOPIC_TOPIC_DISCOVERY.to_string()),
        MessagePayload(
            serde_json::to_vec(&TopicDiscoveryMessage {
                has_topics: router.list_topics().await,
            })
            .unwrap_or_default(),
        ),
        Standard,
        None,
    );

    if let Err(_) = router
        .send(
            SYSTEM_TOPIC_TOPIC_DISCOVERY,
            topics_discovery_msg,
            None,
            None,
        )
        .await {
        eprintln!("[START UP] Failed to send topic discovery message")
    }

    if node_type == Host {
        tokio::spawn(TopicRouter::run_delay_scheduler());
    }

    Ok(shutdown_tx)
}
