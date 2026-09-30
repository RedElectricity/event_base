//! One‑call system startup: [`Bootstrap`] turns a [`NodeConfig`](crate::NodeConfig)
//! into a running node.
//!
//! `Bootstrap` is the *behaviour* half of the config feature — it is exactly
//! the `start_system!` recipe (global init → queue factory → WAL → system
//! handlers → user handlers → consumer loop → scheduler / heartbeat loops)
//! wrapped behind a typed config, plus an optional gRPC control plane. It
//! changes no physics; it exists so a deployer writes `eb.toml` + one line of
//! Rust instead of reconstructing the global‑init order by hand.
//!
//! ```no_run
//! # use event_base::Bootstrap;
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! // Boot from eb.toml; ctrl‑C or a `Shutdown` command stops the node.
//! let running = Bootstrap::from_file("eb.toml")?.start().await?;
//! running.wait().await;
//! # Ok(())
//! # }
//! ```

use crate::config::{NodeConfig, QueueBackend, Role, WalBackend};
use event_base_core::queues::factory::QueueFactory;
use event_base_core::shutdown::{ShutdownReceiver, ShutdownSender, shutdown_channel};
use event_base_core::system_handlers::system::SystemHandlerBuilder;
use event_base_core::wal::wal::Wal;
use event_base_core::{NodeType, try_set_node_name};
use event_base_macro_func::start_system::start_system_impl;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A fatal error while assembling or starting a node.
#[derive(Debug, Clone)]
pub struct BootError(String);

impl BootError {
    fn new(msg: impl Into<String>) -> Self {
        BootError(msg.into())
    }
}

impl std::fmt::Display for BootError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bootstrap failed: {}", self.0)
    }
}

impl std::error::Error for BootError {}

impl From<event_base_core::error::CoreError> for BootError {
    fn from(e: event_base_core::error::CoreError) -> Self {
        BootError::new(e.to_string())
    }
}

/// The running node handle returned by [`Bootstrap::start`].
///
/// Owns the shutdown channel (now genuinely shared with the built‑in handlers
/// and the control plane — see [`SystemHandlerBuilder::shutdown_sender`]) and,
/// when configured, the spawned gRPC server task.
pub struct Running {
    node_name: String,
    shutdown_tx: ShutdownSender,
    shutdown_rx: ShutdownReceiver,
    grpc: Option<tokio::task::JoinHandle<Result<(), event_base_grpc::ServeError>>>,
}

impl Running {
    /// This node's resolved name.
    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    /// A clone of the shutdown sender — fire it (or hand it to a signal
    /// handler) to initiate graceful shutdown fleet‑wide.
    pub fn shutdown_sender(&self) -> ShutdownSender {
        self.shutdown_tx.clone()
    }

    /// Trigger shutdown now. Returns the number of receivers that got the
    /// signal (0 means nothing is listening — e.g. [`wait`](Self::wait) was not
    /// awaited).
    pub fn shutdown(&self) -> usize {
        self.shutdown_tx.send(()).unwrap_or(0)
    }

    /// Block until a shutdown signal arrives **or** the process gets `Ctrl‑C`,
    /// then tear down the gRPC task. This is what a `main` ends with.
    pub async fn wait(self) {
        let Running {
            mut shutdown_rx,
            grpc,
            ..
        } = self;
        tokio::select! {
            _ = shutdown_rx.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
        if let Some(handle) = grpc {
            handle.abort();
        }
    }
}

/// Builder for a single node boot. See the [module docs](self).
pub struct Bootstrap {
    config: NodeConfig,
}

impl Bootstrap {
    /// Wrap a config you already have.
    pub fn new(config: NodeConfig) -> Self {
        Self { config }
    }

    /// Load `eb.toml` from disk.
    ///
    /// # Errors
    /// I/O or TOML validation failure (unknown keys are rejected).
    pub fn from_file(path: impl AsRef<std::path::Path>) -> Result<Self, BootError> {
        NodeConfig::load(path)
            .map(Self::new)
            .map_err(BootError::new)
    }

    /// Parse the config from a TOML string (tests, embedded configs).
    ///
    /// # Errors
    /// TOML validation failure.
    pub fn from_toml_str(s: &str) -> Result<Self, BootError> {
        NodeConfig::from_toml_str(s)
            .map(Self::new)
            .map_err(BootError::new)
    }

    /// A default in‑memory **Host** named `name` (no control plane).
    pub fn host(name: impl Into<String>) -> Self {
        let mut config = NodeConfig::default();
        config.node.name = name.into();
        config.node.role = Role::Host;
        Self::new(config)
    }

    /// A default in‑memory **Worker** named `name`.
    pub fn worker(name: impl Into<String>) -> Self {
        let mut config = NodeConfig::default();
        config.node.name = name.into();
        config.node.role = Role::Worker;
        Self::new(config)
    }

    /// Start the node: initialize globals, wire the chosen backends, register
    /// every `#[handler]`, and (if `[grpc]` is present) spawn the control plane.
    ///
    /// # Errors
    /// A backend needs a disabled cargo feature, the config is unbootable
    /// (e.g. an already‑initialized global → a second `start()` in‑process), or
    /// a queue/WAL connection fails.
    pub async fn start(self) -> Result<Running, BootError> {
        let node_name = if self.config.node.name.is_empty() {
            default_name()
        } else {
            self.config.node.name.clone()
        };
        // First‑boot wins the process‑wide name; a re‑boot is rejected later by
        // TopicRouter::init (AlreadyInitialized), so ignoring a set error here
        // just avoids a panic on the OnceLock.
        let _ = try_set_node_name(node_name.clone());

        let node_type = match self.config.node.role {
            Role::Host => NodeType::Host,
            Role::Worker => NodeType::Worker,
        };

        let factory = self.build_factory().await?;

        // Two independent WAL handles, matching the `start_system!` contract:
        // the registry WAL (worker snapshots) and the WAL‑sync WAL (`_system.*`
        // record state updates) are distinct concerns. Redis is the recommended
        // backend when both must observe one shared store.
        let (registry_wal, builder_wal) = self.build_wal().await?;

        let (shutdown_tx, shutdown_rx) = shutdown_channel();
        let builder = SystemHandlerBuilder::new(
            builder_wal,
            shutdown_tx.clone(),
            self.config.audit_capacity(),
        );

        let final_tx = start_system_impl(node_type, factory, registry_wal, builder).await?;

        let grpc = match &self.config.grpc {
            Some(g) => Some(spawn_grpc(g).await?),
            None => None,
        };

        Ok(Running {
            node_name,
            shutdown_tx: final_tx,
            shutdown_rx,
            grpc,
        })
    }

    async fn build_factory(&self) -> Result<Arc<dyn QueueFactory>, BootError> {
        match self.config.queue.backend {
            QueueBackend::Memory => Ok(Arc::new(
                event_base_queue::crossfire::MemoryQueueFactory::new(self.config.queue.capacity),
            )),
            QueueBackend::Redis => build_redis_factory(&self.config).await,
        }
    }

    async fn build_wal(&self) -> Result<(Box<dyn Wal>, Arc<RwLock<dyn Wal>>), BootError> {
        match self.config.wal.backend {
            WalBackend::Memory => Ok((
                Box::new(event_base_wal::memory::MemoryWal::new()),
                Arc::new(RwLock::new(event_base_wal::memory::MemoryWal::new())),
            )),
            WalBackend::Persistent => {
                let path = self.config.wal.path.clone();
                let a = event_base_wal::persistent::PersistentWal::new(path.clone()).await?;
                let b = event_base_wal::persistent::PersistentWal::new(path).await?;
                Ok((Box::new(a), Arc::new(RwLock::new(b))))
            }
            WalBackend::Redis => build_redis_wal(&self.config).await,
        }
    }
}

/// Spawn the gRPC control plane from a `[grpc]` section.
async fn spawn_grpc(
    g: &crate::config::GrpcSection,
) -> Result<tokio::task::JoinHandle<Result<(), event_base_grpc::ServeError>>, BootError> {
    let addr: SocketAddr = g
        .addr
        .parse()
        .map_err(|e| BootError::new(format!("invalid grpc.addr {:?}: {e}", g.addr)))?;

    let mut config = event_base_grpc::ServeConfig::new(addr)
        .reflection(g.reflection)
        .health(g.health);
    if !g.token.is_empty() {
        config = config.token(g.token.clone());
    }

    #[cfg(feature = "grpc-tls")]
    {
        if !g.tls_cert.is_empty() && !g.tls_key.is_empty() {
            let cert = std::fs::read(&g.tls_cert)
                .map_err(|e| BootError::new(format!("reading tls_cert {}: {e}", g.tls_cert)))?;
            let key = std::fs::read(&g.tls_key)
                .map_err(|e| BootError::new(format!("reading tls_key {}: {e}", g.tls_key)))?;
            config = config.tls(cert, key);
        } else if !g.tls_cert.is_empty() || !g.tls_key.is_empty() {
            return Err(BootError::new(
                "grpc TLS needs BOTH tls_cert and tls_key set",
            ));
        }
    }

    Ok(tokio::spawn(async move { config.serve().await }))
}

#[cfg(feature = "redis")]
async fn build_redis_factory(config: &NodeConfig) -> Result<Arc<dyn QueueFactory>, BootError> {
    let qc = event_base_queue::redis_streams::RedisQueueConfig::new(config.queue.url.clone())
        .with_prefix(config.queue.prefix.clone())
        .with_node(config.redis_node_id())
        .with_block_ms(config.queue.block_ms);
    let factory = event_base_queue::redis_streams::RedisStreamQueueFactory::with_config(qc).await?;
    Ok(Arc::new(factory))
}

#[cfg(not(feature = "redis"))]
async fn build_redis_factory(_config: &NodeConfig) -> Result<Arc<dyn QueueFactory>, BootError> {
    Err(BootError::new(
        "redis queue backend requested but the `redis` feature is disabled — \
         build event_base with --features redis",
    ))
}

#[cfg(feature = "redis")]
async fn build_redis_wal(
    config: &NodeConfig,
) -> Result<(Box<dyn Wal>, Arc<RwLock<dyn Wal>>), BootError> {
    let url = config.wal_redis_url();
    let prefix = config.wal.prefix.clone();
    let a = event_base_wal::redis::RedisWal::with_prefix(url.clone(), prefix.clone()).await?;
    let b = event_base_wal::redis::RedisWal::with_prefix(url, prefix).await?;
    Ok((Box::new(a), Arc::new(RwLock::new(b))))
}

#[cfg(not(feature = "redis"))]
async fn build_redis_wal(
    _config: &NodeConfig,
) -> Result<(Box<dyn Wal>, Arc<RwLock<dyn Wal>>), BootError> {
    Err(BootError::new(
        "redis WAL backend requested but the `redis` feature is disabled — \
         build event_base with --features redis",
    ))
}

/// Process hostname (env), else `"node"`.
fn default_name() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "node".to_string())
}
