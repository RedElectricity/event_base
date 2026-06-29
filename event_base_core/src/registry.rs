//! Static registration of message handlers via the `linkme` distributed slice.
//!
//! Handlers are registered at compile time and can be initialized globally.

use crate::error::CoreError;
use crate::shutdown::ShutdownSender;
use linkme::distributed_slice;
use std::future::Future;
use std::pin::Pin;

/// Type alias for a registration function that takes a shutdown sender and
/// returns a future that resolves to `Result<(), CoreError>`.
pub type RegisterFn = dyn Fn(ShutdownSender) -> Pin<Box<dyn Future<Output = Result<(), CoreError>> + Send>>
    + Send
    + Sync;

/// Distributed slice collecting all registered handler entries.
#[distributed_slice]
pub static HANDLER_REGISTRY: [HandlerEntry] = [..];

/// A single entry in the handler registry.
pub struct HandlerEntry {
    /// Topic name this handler processes.
    pub topic: &'static str,
    /// Function that registers the handler (e.g., starts consumers).
    pub register_fn: &'static RegisterFn,
}

/// Iterates over all registered handlers and invokes their registration functions.
///
/// # Errors
/// Returns the first error encountered during registration.
pub async fn register_all_handlers(shutdown_tx: ShutdownSender) -> Result<(), CoreError> {
    for entry in HANDLER_REGISTRY {
        (entry.register_fn)(shutdown_tx.clone()).await?;
    }
    Ok(())
}
