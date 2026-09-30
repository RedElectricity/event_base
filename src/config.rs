//! Typed `eb.toml` node configuration for [`Bootstrap`](crate::Bootstrap).
//!
//! `NodeConfig` is the *data* half of the config feature: it parses and
//! validates the TOML a node boots from, with **unknown keys rejected** so a
//! typo (`capasity`, `backned`) fails at load instead of being silently ignored.
//! Backend *construction* lives in [`crate::bootstrap`], which turns a
//! `NodeConfig` into a running system.
//!
//! ```toml
//! [node]
//! name = "orders-host"        # omitted → the process hostname
//! role = "host"               # host | worker
//!
//! [queue]
//! backend = "memory"          # memory | redis
//! capacity = 100_000          # memory only (TOML integer underscores are fine)
//!
//! [wal]
//! backend = "memory"          # memory | persistent | redis
//!
//! # Optional: expose the gRPC control plane.
//! [grpc]
//! addr = "0.0.0.0:50051"
//! token = ""                  # empty → unauthenticated
//! reflection = true
//! health = true
//! ```

use serde::{Deserialize, Serialize};
use std::path::Path;

/// A queue‑transport backend selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum QueueBackend {
    /// In‑process crossfire fabric (default; single machine).
    #[default]
    Memory,
    /// Redis Streams — the first true cross‑process backend. Requires the
    /// crate's `redis` feature; a config selecting it without the feature
    /// fails at [`start`](crate::Bootstrap::start), not at parse time.
    Redis,
}

/// A WAL backend selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum WalBackend {
    /// In‑RAM WAL (lost on restart; default).
    #[default]
    Memory,
    /// File‑backed WAL. Requires the `persistent` feature.
    Persistent,
    /// Redis‑backed WAL. Requires the `redis` feature.
    Redis,
}

/// This node's identity and role.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeSection {
    /// Globally unique node name. Defaults to the process hostname, then
    /// `"node"` if that is unavailable.
    pub name: String,
    /// `host` (coordinates the fleet) or `worker` (consumes topics).
    pub role: Role,
}

/// Node role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    #[default]
    Host,
    Worker,
}

impl Default for NodeSection {
    fn default() -> Self {
        Self {
            name: default_host_name(),
            role: Role::Host,
        }
    }
}

/// Queue transport settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueSection {
    pub backend: QueueBackend,
    /// Bound of the in‑process crossfire channels (memory backend only).
    pub capacity: usize,
    /// Redis connection URL (redis backend only), e.g. `redis://127.0.0.1:6379`.
    pub url: String,
    /// Redis key/stream prefix namespace.
    pub prefix: String,
    /// XREADGROUP block time in milliseconds.
    pub block_ms: usize,
    /// Redis consumer‑group node id; empty → [`NodeSection::name`].
    pub node: String,
}

impl Default for QueueSection {
    fn default() -> Self {
        Self {
            backend: QueueBackend::Memory,
            capacity: 100_000,
            url: String::new(),
            prefix: "event_base".to_string(),
            block_ms: 2000,
            node: String::new(),
        }
    }
}

/// WAL settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WalSection {
    pub backend: WalBackend,
    /// File path (persistent backend only).
    pub path: String,
    /// Redis URL + prefix (redis backend only). Reuses [`QueueSection::url`]
    /// when left empty so a single Redis URL drives both planes by default.
    pub url: String,
    pub prefix: String,
}

impl Default for WalSection {
    fn default() -> Self {
        Self {
            backend: WalBackend::Memory,
            path: "event_base.wal".to_string(),
            url: String::new(),
            prefix: "event_base".to_string(),
        }
    }
}

/// Optional gRPC control‑plane endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GrpcSection {
    /// `host:port` to bind. Requires the `gRPC` feature.
    pub addr: String,
    /// Bearer token; empty → the control plane is unauthenticated (localhost
    /// admin only). Set this whenever the port may be reachable by others.
    pub token: String,
    /// PEM cert/key file paths for TLS; both empty → plaintext. Requires
    /// `grpc-tls`.
    pub tls_cert: String,
    pub tls_key: String,
    /// Serve gRPC Server Reflection (default **on**; disable in production).
    pub reflection: bool,
    /// Serve the standard Health service (default **on**).
    pub health: bool,
}

impl Default for GrpcSection {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:50051".to_string(),
            token: String::new(),
            tls_cert: String::new(),
            tls_key: String::new(),
            reflection: true,
            health: true,
        }
    }
}

/// The complete `eb.toml` document.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    pub node: NodeSection,
    pub queue: QueueSection,
    pub wal: WalSection,
    /// Absent `[grpc]` table → no control plane is served.
    pub grpc: Option<GrpcSection>,
    /// Capacity of the audit ring buffer (0 → the built‑in default, 1024).
    pub audit_buf_capacity: usize,
}

impl NodeConfig {
    /// Parse a TOML string. Rejects unknown fields (a typo'd key is an error).
    ///
    /// # Errors
    /// Returns a human‑readable message describing the parse/validation failure.
    pub fn from_toml_str(s: &str) -> Result<Self, String> {
        toml::from_str(s).map_err(|e| format!("invalid eb.toml: {e}"))
    }

    /// Load and parse an `eb.toml` from disk.
    ///
    /// # Errors
    /// I/O failure or a TOML validation error.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, String> {
        let text = std::fs::read_to_string(path.as_ref())
            .map_err(|e| format!("reading {}: {e}", path.as_ref().display()))?;
        Self::from_toml_str(&text)
    }

    /// The Redis node id to use, falling back to the node name.
    pub fn redis_node_id(&self) -> String {
        if self.queue.node.is_empty() {
            self.node.name.clone()
        } else {
            self.queue.node.clone()
        }
    }

    /// The Redis URL for the WAL, falling back to the queue's URL.
    pub fn wal_redis_url(&self) -> String {
        if self.wal.url.is_empty() {
            self.queue.url.clone()
        } else {
            self.wal.url.clone()
        }
    }

    /// Effective audit ring‑buffer capacity (0 → 1024).
    pub fn audit_capacity(&self) -> usize {
        if self.audit_buf_capacity == 0 {
            1024
        } else {
            self.audit_buf_capacity
        }
    }
}

/// Best‑effort process hostname for the default node name.
fn default_host_name() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "node".to_string())
}
