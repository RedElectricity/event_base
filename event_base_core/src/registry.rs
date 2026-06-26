use crate::error::CoreError;
use crate::shutdown::ShutdownSender;
use linkme::distributed_slice;
use std::future::Future;
use std::pin::Pin;

pub type RegisterFn = dyn Fn(ShutdownSender) -> Pin<Box<dyn Future<Output = Result<(), CoreError>> + Send>>
    + Send
    + Sync;

#[distributed_slice]
pub static HANDLER_REGISTRY: [HandlerEntry] = [..];

pub struct HandlerEntry {
    pub topic: &'static str,
    pub register_fn: &'static RegisterFn,
}

pub async fn register_all_handlers(shutdown_tx: ShutdownSender) -> Result<(), CoreError> {
    for entry in HANDLER_REGISTRY {
        (entry.register_fn)(shutdown_tx.clone()).await?;
    }
    Ok(())
}
