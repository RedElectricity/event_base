use event_base_core::error::CoreError;
use event_base_core::message::EMessage;
use event_base_core::topic::TopicRouter;
use std::time::Duration;

pub async fn send_msg_impl(
    msg: EMessage,
    try_send: Option<bool>,
    time_out: Option<Duration>,
) -> Result<(), CoreError> {
    let topic = msg.clone().topic.0;
    TopicRouter::global()
        .send(&topic, msg, try_send, time_out)
        .await?;
    Ok(())
}
