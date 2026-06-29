//! A procedural macro for declaring message handlers.
//!
//! This crate provides the `#[handler]` attribute macro, which automatically
//! generates the necessary boilerplate to register a function as a message
//! handler for a specific topic. It creates a handler struct, implements the
//! `EHandler` trait, registers the topic, and creates a configurable number of
//! workers with a middleware pipeline.
//!
//! # Usage
//!
//! Apply the `#[handler]` attribute to an async function that takes a reference
//! to `EMessage` and returns `Ack`. The function becomes the core processing
//! logic for messages on the specified topic.
//!
//! ## Required Parameters
//! - `topic` – The topic string this handler will process (e.g., `topic = "orders"`).
//!
//! ## Optional Parameters
//! - `workers` – Number of worker tasks to spawn (default: `1`).
//! - `timeout` – Maximum processing time in seconds per message (default: `None`).
//! - `shutdown_timeout` – Maximum time in seconds to wait for graceful shutdown
//!   (default: `None`).
//! - `shutdown_check_interval` – Polling interval in milliseconds to check if
//!   the worker is idle during shutdown (default: `None` → `50ms`).
//! - `middleware` – A middleware or array of middlewares to apply (default: `None`).
//!
//! # Generated Code
//!
//! The macro generates:
//! 1. A handler struct named `{FunctionName}Handler`.
//! 2. An `EHandler` implementation that calls the user function.
//! 3. A static entry in the `HANDLER_REGISTRY` via `linkme`.
//! 4. A registration function that:
//!    - Creates the pipeline (with optional middlewares).
//!    - Registers the topic in `TopicRouter`.
//!    - Registers the handler in `ConsumerRouter`.
//!    - Creates the specified number of workers.
//!
//! # Example
//!
//! ```no_run
//! use event_base_core::handler::{Ack, EHandler};
//! use event_base_core::message::EMessage;
//! use event_base_macro::handler;
//!
//! #[handler(
//!     topic = "user.signup",
//!     workers = 3,
//!     timeout = 30,
//!     shutdown_timeout = 10,
//!     shutdown_check_interval = 100,
//!     middleware = [MyMiddleware, AnotherMiddleware]
//! )]
//! async fn handle_signup(msg: &EMessage) -> Ack {
//!     // ... processing logic ...
//!     Ack::Ack
//! }
//! ```

mod handler;

use proc_macro::TokenStream;

/// Attribute macro for declaring a message handler.
///
/// See the module‑level documentation for details on parameters and usage.
#[proc_macro_attribute]
pub fn handler(args: TokenStream, input: TokenStream) -> TokenStream {
    handler::handler_impl(args, input).unwrap_or_else(|e| e.to_compile_error().into())
}
