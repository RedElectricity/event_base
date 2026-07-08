//! Implementation of message sending via the global `TopicRouter`.

use event_base_core::error::CoreError;
use event_base_core::message::EMessage;
use event_base_core::topic::TopicRouter;
use std::time::Duration;

/// Sends a message using the global `TopicRouter`.
///
/// This is the implementation function called by the `send_msg!` macro.
/// It extracts the topic from the message and forwards the send request
/// to the router.
///
/// # Arguments
/// * `msg` – The message to send.
/// * `try_send` – If `Some(true)`, uses non‑blocking try‑send; if `Some(false)`
///   or `None`, uses blocking send.
/// * `time_out` – If `Some(duration)`, sets a send timeout; `None` means no timeout.
///
/// # Errors
/// Returns `CoreError` if the router is not initialized or the send fails.
///
/// # See Also
/// - [`TopicRouter::send`](event_base_core::topic::TopicRouter::send)
pub async fn send_msg_impl(
    msg: EMessage,
    try_send: Option<bool>,
    time_out: Option<Duration>,
) -> Result<(), CoreError> {
    let topic = msg.clone().topic.0;
    TopicRouter::global()
        .read().await
        .send(&topic, msg, try_send, time_out)
        .await?;
    Ok(())
}
