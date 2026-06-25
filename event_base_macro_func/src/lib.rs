#[macro_export]
macro_rules! send_msg {
    ($topic:expr, $msg:expr) => {{
        let mut msg = $msg;
        let topic_str: String = ($topic).to_string();
        $crate::TopicRouter::global().send(&topic_str, msg).await
    }};
}

// TODO: Refactor this
#[macro_export]
macro_rules! start_system {
    ($factory:expr, $wal:expr, $system_builder:expr, $node_type: expr) => {{
        use event_base_core::shutdown::shutdown_channel;
        use event_base_core::topic::TopicRouter;

        event_base_core::set_node_type($node_type)

        let factory = std::sync::Arc::new($factory);
        let wal = $wal.map(std::sync::Arc::new);
        TopicRouter::init(factory, wal.clone())?;

        let (shutdown_tx, _) = shutdown_channel();

        $system_builder.register_all().await?;

        event_base_core::registry::register_all_handlers(shutdown_tx.clone()).await?;

        if let Some(wal) = wal {
            let router = TopicRouter::global();

            let topics_discovery_msg = EMessage::new(
            MessageTopic(SYSTEM_TOPIC_WORKER_DISCOVERY.parse().unwrap()),
            MessagePayload(serde_json::to_vec(&TopicDiscoveryMessage {
                has_topics: router.list_topics().await
            }).unwrap()),
            Standard,
            None,
        );

            router.send(SYSTEM_TOPIC_WORKER_DISCOVERY, topics_discovery_msg, None, None).await
            .expect("[START UP] Failed to send topic discovery message");

            tokio::spawn(event_base_core::delay::run_scheduler(wal, router));
        }

        Ok(shutdown_tx)
    }};
}
