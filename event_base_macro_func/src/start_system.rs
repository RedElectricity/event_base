//! Implementation of the system startup routine.

use event_base_core::NodeType::Host;
use event_base_core::constant::SYSTEM_TOPIC_TOPIC_DISCOVERY;
use event_base_core::error::CoreError;
use event_base_core::message::DeliveryMode::Standard;
use event_base_core::message::{EMessage, MessagePayload, MessageTopic};
use event_base_core::queues::consumer_router::ConsumerRouter;
use event_base_core::queues::factory::QueueFactory;
use event_base_core::shutdown::{ShutdownSender, shutdown_channel};
use event_base_core::system_handlers::system::SystemHandlerBuilder;
use event_base_core::system_handlers::topic::TopicDiscoveryMessage;
use event_base_core::topic::TopicRouter;
use event_base_core::trace_layer::TraceLayer;
use event_base_core::wal::wal::Wal;
use event_base_core::worker_registry::WorkerRegistry;
use event_base_core::{NodeType, set_node_type};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Initializes all global components and starts the system.
///
/// This function is called by the `start_system!` macro. It performs the
/// following steps:
///
/// 1. Sets the global node type.
/// 2. Wraps the WAL in `Arc<RwLock<...>>` and initializes the `TopicRouter`.
/// 3. Creates a global producer and initializes the `ConsumerRouter`.
/// 4. Initializes the `WorkerRegistry`.
/// 5. Creates a shutdown channel.
/// 6. Registers all system handlers via the `SystemHandlerBuilder`.
/// 7. Registers all user handlers via the static registry.
/// 8. Spawns the main consumer loop.
/// 9. Sets up the tracing layer.
/// 10. Sends a topic discovery message to sync topics.
/// 11. If the node type is `Host`, spawns the delay scheduler.
///
/// # Arguments
/// * `node_type` – `Host` or `Worker`.
/// * `factory` – The queue factory for creating producers/consumers.
/// * `wal` – The WAL implementation.
/// * `system_builder` – The pre‑configured system handler builder.
///
/// # Returns
/// A `ShutdownSender` that can be used to initiate graceful shutdown.
///
/// # Errors
/// Returns `CoreError` if any initialization step fails (e.g., global singletons
/// already set, queue creation fails, etc.).
pub async fn start_system_impl(
    node_type: NodeType,
    factory: Arc<dyn QueueFactory>,
    wal: Box<dyn Wal>,
    system_builder: SystemHandlerBuilder,
) -> Result<ShutdownSender, CoreError> {
    set_node_type(node_type);

    let wal_init = Arc::new(RwLock::new(wal));
    let global_producer = factory.create_global_producer()?;
    TopicRouter::init(global_producer)?;

    let main_consumer = factory.create_main_consumer()?;
    ConsumerRouter::init(main_consumer, factory, None)?;

    WorkerRegistry::init(Option::from(wal_init.clone())).await?;

    let (shutdown_tx, _) = shutdown_channel();

    system_builder.register_all().await?;

    event_base_core::registry::register_all_handlers(shutdown_tx.clone()).await?;

    tokio::spawn(async move {
        let _ = ConsumerRouter::global().read().await.recv().await;
    });

    let router = TopicRouter::global().read().await;
    let producer = router.get_producer();
    let trace_layer = TraceLayer::new(producer);

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stdout)
        .with_target(true)
        .with_level(true);

    // 默认只显示 INFO+，可通过 RUST_LOG 覆盖
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    Registry::default()
        .with(trace_layer)
        .with(filter)
        .with(fmt_layer)
        .init();

    let topics_discovery_msg = EMessage::new(
        MessageTopic(SYSTEM_TOPIC_TOPIC_DISCOVERY.to_string()),
        MessagePayload(
            bincode::encode_to_vec(&TopicDiscoveryMessage {
                has_topics: router.list_topics().await,
            }, bincode::config::standard())
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
        .await
    {
        eprintln!("[START UP] Failed to send topic discovery message")
    }

    if node_type == Host {
        tokio::spawn(TopicRouter::run_delay_scheduler());
    }

    Ok(shutdown_tx)
}
