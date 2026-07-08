use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock, RwLock};

// Re‑export public submodules.
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

/// Global node name (set once at startup).
static NODE_NAME: OnceLock<RwLock<String>> = OnceLock::new();
/// Global node type (can be changed during tests).
static NODE_TYPE: RwLock<Option<Arc<NodeType>>> = RwLock::new(None);

/// The role of a node in the system.
#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy, Serialize, Deserialize, Encode, Decode)]
pub enum NodeType {
    /// A host node that coordinates workers, manages the WAL, and handles
    /// system‑level control topics.
    Host = 0,
    /// A worker node that consumes and processes messages from topics.
    Worker = 1,
}

/// Sets the global node name.
///
/// This must be called once during application startup before using any
/// function that retrieves the node name. It will panic if called more than once.
///
/// # Arguments
/// * `node_name` - A unique identifier for this node (e.g., hostname or UUID).
pub fn set_node_name(node_name: String) {
    NODE_NAME
        .set(RwLock::new(node_name))
        .expect("Node name already set");
}

/// Returns the global node name.
///
/// # Panics
/// Panics if [`set_node_name`] has not been called before this function is invoked.
pub fn get_node_name() -> String {
    NODE_NAME
        .get()
        .expect("Node name not initialized")
        .read()
        .expect("NODE_NAME poisoned")
        .clone()
}

/// Sets the global node type.
///
/// Can be called multiple times (e.g., in tests) to change the node type.
///
/// # Arguments
/// * `node_type` - The role of this node (`Host` or `Worker`).
pub fn set_node_type(node_type: NodeType) {
    *NODE_TYPE.write().expect("NODE_TYPE poisoned") = Some(Arc::new(node_type));
}

/// Returns the global node type.
///
/// # Panics
/// Panics if [`set_node_type`] has not been called before this function is invoked.
pub fn get_node_type() -> Arc<NodeType> {
    NODE_TYPE
        .read()
        .expect("NODE_TYPE poisoned")
        .as_ref()
        .expect("Node type not initialized")
        .clone()
}
