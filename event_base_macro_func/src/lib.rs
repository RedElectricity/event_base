#[macro_export]
macro_rules! send_msg {
    ($topic:expr, $msg:expr) => {{
        let mut msg = $msg;
        let topic_str: String = ($topic).to_string();
        $crate::TopicRouter::global().send(&topic_str, msg).await
    }};
}

#[macro_export]
macro_rules! start_queue_system {
    ($factory:expr, $wal:expr, $system_builder:expr) => {{
        use event_base_core::topic::TopicRouter;
        use event_base_core::shutdown::shutdown_channel;

        let factory = std::sync::Arc::new($factory);
        let wal = $wal.map(std::sync::Arc::new);
        TopicRouter::init(factory, wal.clone())?;

        let (shutdown_tx, _) = shutdown_channel();

        $system_builder.register_all().await?;

        event_base_core::registry::register_all_handlers(shutdown_tx.clone()).await?;

        if let Some(wal) = wal {
            let router = TopicRouter::global();
            tokio::spawn(event_base_core::delay::run_scheduler(wal, router));
        }

        Ok(shutdown_tx)
    }};
}