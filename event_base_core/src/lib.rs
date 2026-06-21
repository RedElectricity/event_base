use std::sync::{Arc, OnceLock};

pub mod audit;
pub mod constant;
pub mod dead_letter;
pub mod error;
pub mod handler;
pub mod message;
pub mod metrics;
pub mod middleware;
pub mod queues;
pub mod registry;
pub mod shutdown;
pub mod system_handlers;
pub mod topic;
pub mod trace;
pub mod trace_layer;
pub mod traits;
pub mod wal;
pub mod worker;
pub mod worker_registry;

static NODE_NAME: OnceLock<Arc<String>> = OnceLock::new();

pub fn set_node_name(node_name: String) {
    NODE_NAME
        .set(Arc::new(node_name))
        .expect("Node name already set");
}

pub fn get_node_name() -> String {
    NODE_NAME
        .get()
        .expect("Node name not initialized")
        .clone()
        .to_string()
}
