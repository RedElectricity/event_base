//! Convenience macros and implementation functions for the event‑base system.
//!
//! This crate provides two macros that wrap common startup and message‑sending
//! operations, reducing boilerplate for end users.
//!
//! - [`send_msg!`] – sends a message via the global `TopicRouter`.
//! - [`start_system!`] – initializes the entire system (WAL, queues, handlers, etc.).
//!
//! The actual implementation is in the `send_msg` and `start_system` submodules.

pub mod send_msg;
pub mod start_system;

/// Sends a message using the global `TopicRouter`.
///
/// This macro is a thin wrapper around [`send_msg_impl`](send_msg::send_msg_impl)
/// that allows you to pass a message and optional parameters for try‑send and
/// timeout behavior.
///
/// # Arguments
/// - `$msg` – an [`EMessage`](event_base_core::message::EMessage) to send.
/// - `$try_send` – an optional `bool` indicating whether to use try‑send
///   (non‑blocking). Pass `None` to use the default (blocking send).
/// - `$time_out` – an optional [`Duration`](std::time::Duration) for send timeout.
///   Pass `None` for no timeout.
///
/// # Returns
/// `Result<(), CoreError>`. The macro expands to an expression that already
/// contains the `.await`, so call sites must **not** await it again.
///
/// # Example
/// ```
/// # use event_base_core::message::{EMessage, MessageTopic, MessagePayload, DeliveryMode};
/// # use event_base_macro_func::send_msg;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let msg = EMessage::new(
///     MessageTopic("my_topic".to_string()),
///     MessagePayload(b"hello".to_vec()),
///     DeliveryMode::Standard,
///     None,
/// );
/// send_msg!(msg, None, None)?;
/// # Ok(())
/// # }
/// ```
#[macro_export]
macro_rules! send_msg {
    ($msg:expr, $try_send:expr, $time_out:expr) => {{
        use $crate::send_msg::send_msg_impl;
        send_msg_impl($msg, $try_send, $time_out).await
    }};
}

/// Starts the entire event‑base system.
///
/// This macro initializes the global `TopicRouter`, `ConsumerRouter`,
/// `WorkerRegistry`, system handlers, and the tracing layer. It also spawns the
/// main consumer loop and, on `Host` nodes, the delay scheduler.
///
/// # Arguments
/// - `$producer` – an `Arc<dyn QueueFactory>` that creates producers and consumers.
/// - `$wal` – a `Box<dyn Wal>` (WAL implementation) to use for durability.
/// - `$system_builder` – a [`SystemHandlerBuilder`](event_base_core::system_handlers::system::SystemHandlerBuilder)
///   already configured with trace collectors and other dependencies.
/// - `$node_type` – a [`NodeType`](event_base_core::NodeType) (`Host` or `Worker`)
///   indicating the role of this node.
///
/// # Returns
/// `Result<ShutdownSender, CoreError>` – the shutdown sender can be used
/// to trigger graceful shutdown of all workers. Like [`send_msg!`], the macro
/// expands to an already‑awaited expression; do not add a `.await` at the
/// call site.
///
/// # Example
/// ```no_run
/// # use event_base_core::NodeType;
/// # use event_base_core::queues::factory::QueueFactory;
/// # use event_base_core::wal::wal::Wal;
/// # use event_base_core::system_handlers::system::SystemHandlerBuilder;
/// # use event_base_core::shutdown::shutdown_channel;
/// # use std::sync::Arc;
/// # use tokio::sync::RwLock;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let factory: Arc<dyn QueueFactory> = unimplemented!();
/// let wal: Box<dyn Wal> = unimplemented!();
/// let (shutdown_tx, _shutdown_rx) = shutdown_channel();
/// let builder = SystemHandlerBuilder::new(
///     Arc::new(RwLock::new(event_base_wal::memory::MemoryWal::new())),
///     shutdown_tx,
///     1024,
/// );
/// let shutdown_tx = event_base_macro_func::start_system!(
///     factory,
///     wal,
///     builder,
///     NodeType::Host
/// )?;
/// # Ok(())
/// # }
/// ```
#[macro_export]
macro_rules! start_system {
    ($producer:expr, $wal:expr, $system_builder:expr, $node_type: expr) => {{
        use $crate::start_system::start_system_impl;
        start_system_impl($node_type, $producer, $wal, $system_builder).await
    }};
}
