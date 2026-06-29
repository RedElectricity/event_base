//! Shutdown coordination and worker termination strategies.
//!
//! This module provides types and functions for gracefully or forcefully
//! shutting down workers. It defines a broadcast channel for shutdown signals
//! and a set of strategy implementations (two‑stage, graceful, force, timeout,
//! state‑based idle, batched) that can be invoked programmatically or via
//! incoming `ShutdownCommand` messages.

pub mod messages;
pub mod methods;

use tokio::sync::broadcast;

/// A sender for shutdown signals (broadcast channel).
pub type ShutdownSender = broadcast::Sender<()>;

/// A receiver for shutdown signals (broadcast channel).
pub type ShutdownReceiver = broadcast::Receiver<()>;

/// Creates a new broadcast channel for shutdown coordination.
///
/// The channel has a capacity of 1, meaning only the latest signal is retained.
/// This is sufficient for one‑time shutdown notifications.
///
/// # Returns
/// A tuple of `(ShutdownSender, ShutdownReceiver)`.
pub fn shutdown_channel() -> (ShutdownSender, ShutdownReceiver) {
    // Only one signal
    broadcast::channel(1)
}
