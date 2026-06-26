use serde::{Deserialize, Serialize};
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
static NODE_TYPE: OnceLock<Arc<NodeType>> = OnceLock::new();

#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum NodeType {
    Host = 0,
    Worker = 1,
}

pub fn set_node_name(node_name: String) {
    NODE_NAME
        .set(Arc::new(node_name))
        .expect("Node name already set");
}

pub fn get_node_name() -> String {
    NODE_NAME
        .get()
        .expect("Node name not initialized")
        .to_string()
}

pub fn set_node_type(node_type: NodeType) {
    NODE_TYPE
        .set(Arc::new(node_type))
        .expect("Node type already set");
}

pub fn get_node_type() -> Arc<NodeType> {
    NODE_TYPE.get().expect("Node type not initialized").clone()
}
