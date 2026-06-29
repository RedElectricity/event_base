//! Middleware chain and pipeline for processing messages.
//!
//! Middleware can intercept and modify a message before it reaches the final handler,
//! allowing for cross-cutting concerns like logging, validation, or rate limiting.

use crate::handler::{Ack, EHandler};
use crate::message::EMessage;
use async_trait::async_trait;
use std::sync::Arc;

/// A continuation token that represents the next step in the middleware chain.
///
/// It holds the remaining middleware and the final handler.
pub struct Next<'a> {
    pub(crate) next: &'a [Box<dyn Middleware>],
    pub(crate) index: usize,
    handler: &'a dyn EHandler,
}

impl<'a> Next<'a> {
    /// Proceeds to the next middleware or, if none remain, invokes the final handler.
    pub async fn run(&self, msg: &mut EMessage) -> Ack {
        if let Some(mw) = self.next.get(self.index) {
            let next = Next {
                next: self.next,
                index: self.index + 1,
                handler: self.handler,
            };
            mw.handle(msg, next).await
        } else {
            self.handler.handler(msg).await
        }
    }
}

/// A middleware component that can process and optionally transform a message.
#[async_trait]
pub trait Middleware: Send + Sync {
    /// Handles the message and either passes it to the next middleware via `next`
    /// or returns an `Ack` directly.
    async fn handle(&self, msg: &mut EMessage, next: Next<'_>) -> Ack;
}

/// A pipeline that composes a chain of middlewares around a final handler.
pub struct Pipeline {
    middlewares: Vec<Box<dyn Middleware>>,
    handler: Arc<dyn EHandler>,
}

impl Pipeline {
    /// Creates a new pipeline with the given final handler.
    pub fn new(handler: Box<dyn EHandler>) -> Self {
        Self {
            middlewares: Vec::new(),
            handler: Arc::new(handler),
        }
    }

    /// Adds a middleware to the end of the chain.
    pub fn with(mut self, middleware: impl Middleware + 'static) -> Self {
        self.middlewares.push(Box::new(middleware));
        self
    }

    /// Executes the pipeline on the given message, returning the final `Ack`.
    pub async fn run(&self, msg: &mut EMessage) -> Ack {
        let next = Next {
            next: &self.middlewares,
            index: 0,
            handler: &*self.handler,
        };
        next.run(msg).await
    }
}
