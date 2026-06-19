use crate::handler::{Ack, EHandler};
use crate::message::EMessage;
use async_trait::async_trait;
use std::sync::Arc;

pub struct Next<'a> {
    pub(crate) next: &'a [Box<dyn Middleware>],
    pub(crate) index: usize,
    handler: &'a dyn EHandler,
}

impl<'a> Next<'a> {
    pub async fn run(&self, msg: &mut EMessage) -> Ack {
        if let Some(mw) = self.next.get(self.index) {
            let next = Next {
                next: self.next,
                index: self.index + 1,
                handler: self.handler,
            };
            mw.handle(msg, next).await
        } else {
            self.handler.handle(msg).await
        }
    }
}

#[async_trait]
pub trait Middleware: Send + Sync {
    async fn handle(&self, msg: &mut EMessage, next: Next<'_>) -> Ack;
}

pub struct Pipeline {
    middlewares: Vec<Box<dyn Middleware>>,
    handler: Arc<dyn EHandler>,
}

impl Pipeline {
    pub fn new(handler: Box<dyn EHandler>) -> Self {
        Self {
            middlewares: Vec::new(),
            handler: Arc::new(handler),
        }
    }

    pub fn with(mut self, middleware: impl Middleware + 'static) -> Self {
        self.middlewares.push(Box::new(middleware));
        self
    }

    pub async fn run(&self, msg: &mut EMessage) -> Ack {
        let next = Next {
            next: &self.middlewares,
            index: 0,
            handler: &*self.handler,
        };
        next.run(msg).await
    }
}
