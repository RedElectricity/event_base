use crate::server::EventBaseService;
use crate::server::event_base::event_base_server::EventBaseServer;
use std::net::SocketAddr;
use tonic::metadata::MetadataMap;
use tonic::service::InterceptorLayer;
use tonic::transport::Server;
use tonic::{Request, Status};

pub mod server;

/// Re-exports so callers (and `ebctl`) get the generated client + wire types
/// from one place instead of the internal module path.
pub use crate::server::event_base::event_base_client::EventBaseClient;
pub use crate::server::event_base::{
    Empty, ListTopicsResponse, ListWorkersResponse, NodeMetrics, NodeStatusResponse,
    PublishRequest, PublishResponse, ShutdownRequest, ShutdownResponse, TopicStatsResponse,
    shutdown_request::Strategy,
};

/// Constant‑time byte comparison so token checks do not leak the secret's
/// prefix through response timing.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false; // length of a shared secret is not a useful secret
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Validates the `authorization` metadata against `token`. Accepts both
/// `Bearer <token>` (RFC 6750 style, what gRPC clients normally send) and the
/// bare token (handy for `grpcurl -H 'authorization: <token>'`).
fn check_token(meta: &MetadataMap, token: &str) -> Result<(), Status> {
    let provided = meta
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Status::unauthenticated("missing authorization metadata"))?;
    let provided = provided.strip_prefix("Bearer ").unwrap_or(provided);
    if constant_time_eq(provided.as_bytes(), token.as_bytes()) {
        Ok(())
    } else {
        Err(Status::unauthenticated("invalid token"))
    }
}

/// Errors surfaced while assembling or running the control‑plane server.
///
/// `Send`‑clean so a `serve` future can be spawned. Unlike the historical
/// `Box<dyn Error>` of [`serve`], the transport half keeps its concrete type
/// and the setup half (reflection / TLS identity) is a readable message.
#[derive(Debug)]
pub enum ServeError {
    /// Transport‑level failure (bind, TLS config at the tonic layer, serve loop).
    Transport(tonic::transport::Error),
    /// Construction‑time failure (bad TLS identity, reflection registration).
    Setup(String),
}

impl std::fmt::Display for ServeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServeError::Transport(e) => write!(f, "transport error: {e}"),
            ServeError::Setup(m) => write!(f, "server setup error: {m}"),
        }
    }
}

impl std::error::Error for ServeError {}

impl From<tonic::transport::Error> for ServeError {
    fn from(e: tonic::transport::Error) -> Self {
        ServeError::Transport(e)
    }
}

/// Declarative configuration for the gRPC control plane.
///
/// Replaces the trio of near‑duplicate `serve*` functions with one builder so a
/// production node, a TLS‑fronted fleet gateway, and a local dev server differ
/// only in which setters they call. **Server Reflection and the standard gRPC
/// Health service are on by default** (see [`reflection`](Self::reflection) /
/// [`health`](Self::health)); turn them off for a hardened production surface.
///
/// ```no_run
/// # use std::net::SocketAddr;
/// # async fn demo() -> Result<(), event_base_grpc::ServeError> {
/// let addr: SocketAddr = "0.0.0.0:50051".parse().unwrap();
/// event_base_grpc::ServeConfig::new(addr)
///     .token("s3cr3t")          // require `authorization: Bearer <token>`
///     .reflection(false)        // hide the schema in production
///     .serve()
///     .await
/// # }
/// ```
#[derive(Clone)]
pub struct ServeConfig {
    addr: SocketAddr,
    token: Option<String>,
    #[cfg(feature = "tls")]
    tls: Option<(Vec<u8>, Vec<u8>)>,
    reflection: bool,
    health: bool,
}

impl ServeConfig {
    /// New config bound to `addr` with reflection + health enabled and no auth.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            token: None,
            #[cfg(feature = "tls")]
            tls: None,
            reflection: true,
            health: true,
        }
    }

    /// Require `authorization: Bearer <token>` on every RPC (constant‑time
    /// compare; failures map to `Code::Unauthenticated`). When TLS is also
    /// configured the token rides inside the encrypted channel.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Enable/ disable gRPC Server Reflection (default **on**). Reflection lets
    /// `grpcurl`/`buf` introspect the node without the `.proto`; disable it to
    /// withhold the API schema from unauthenticated callers in production.
    /// Note: when a [`token`](Self::token) is set, reflection also demands it.
    pub fn reflection(mut self, on: bool) -> Self {
        self.reflection = on;
        self
    }

    /// Enable/ disable the standard `grpc.health.v1.Health` service (default
    /// **on**), reporting SERVING for the `event_base.EventBase` service.
    /// Orchestrator liveness probes can hit this; when a token is set they must
    /// supply it too (the auth layer wraps every service uniformly).
    pub fn health(mut self, on: bool) -> Self {
        self.health = on;
        self
    }

    /// Serve over TLS (rustls) with the server's PEM leaf cert + key.
    ///
    /// Combines with [`token`](Self::token) when both are set. Requires the
    /// `tls` feature.
    #[cfg(feature = "tls")]
    pub fn tls(mut self, cert_pem: Vec<u8>, key_pem: Vec<u8>) -> Self {
        self.tls = Some((cert_pem, key_pem));
        self
    }

    /// Assemble and run the server until it exits. This is the single canonical
    /// boot path; the free [`serve`]/[`serve_with_token`]/[`serve_tls`] functions
    /// are retained for backward compatibility and delegate the same semantics.
    pub async fn serve(self) -> Result<(), ServeError> {
        let service = EventBaseService;
        let token = self.token.clone();

        let server = Server::builder();

        #[cfg(feature = "tls")]
        let server = match &self.tls {
            Some((cert_pem, key_pem)) => {
                use tonic::transport::{Identity, ServerTlsConfig};
                let tls = ServerTlsConfig::new().identity(Identity::from_pem(cert_pem, key_pem));
                server.tls_config(tls)
                    .map_err(|e| ServeError::Setup(format!("tls config: {e}")))?
            }
            None => server,
        };

        // Always install the layer so the concrete `Server` type is fixed before
        // branching; a `None` token makes the interceptor a transparent pass‑
        // through, preserving the unauthenticated `serve` behaviour exactly.
        let mut router = server
            .layer(InterceptorLayer::new(move |req: Request<()>| {
                if let Some(token) = &token {
                    check_token(req.metadata(), token)?;
                }
                Ok(req)
            }))
            .add_service(EventBaseServer::new(service));

        if self.reflection {
            let refl = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(server_fdset())
                .build_v1()
                .map_err(|e| ServeError::Setup(format!("reflection: {e}")))?;
            router = router.add_service(refl);
        }

        if self.health {
            let (reporter, health_service) = tonic_health::server::health_reporter();
            reporter
                .clone()
                .set_serving::<EventBaseServer<EventBaseService>>()
                .await;
            router = router.add_service(health_service);
        }

        router.serve(self.addr).await.map_err(ServeError::Transport)
    }
}

/// The encoded `FileDescriptorSet` backing Server Reflection.
fn server_fdset() -> &'static [u8] {
    crate::server::event_base::FILE_DESCRIPTOR_SET
}

/// Serves the control plane on `addr` **without authentication**.
///
/// Kept for single‑machine use (Hermes‑style local admin). Anywhere the port
/// may be reachable by another process, prefer
/// [`ServeConfig::token`] or [`ServeConfig::tls`].
pub async fn serve(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    ServeConfig::new(addr).serve().await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Serves the control plane requiring `authorization: Bearer <token>` on
/// every RPC. Thin wrapper over [`ServeConfig`].
///
/// Returns `tonic::transport::Error` (a `Send` error, unlike the
/// `Box<dyn Error>` of [`serve`]) so callers can `tokio::spawn` it directly.
pub async fn serve_with_token(
    addr: SocketAddr,
    token: impl Into<String>,
) -> Result<(), tonic::transport::Error> {
    let token = token.into();
    let service = EventBaseService;
    Server::builder()
        .layer(InterceptorLayer::new(move |req: Request<()>| {
            check_token(req.metadata(), &token)?;
            Ok(req)
        }))
        .add_service(EventBaseServer::new(service))
        .serve(addr)
        .await
}

/// Serves over TLS (rustls) with optional token auth, when the `tls` feature
/// is enabled. Thin wrapper over [`ServeConfig`].
#[cfg(feature = "tls")]
pub async fn serve_tls(
    addr: SocketAddr,
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
    token: Option<String>,
) -> Result<(), tonic::transport::Error> {
    use tonic::transport::{Identity, ServerTlsConfig};

    let identity = Identity::from_pem(cert_pem, key_pem);
    let tls = ServerTlsConfig::new().identity(identity);
    let service = EventBaseService;
    let token = token.clone();
    Server::builder()
        .tls_config(tls)?
        .layer(InterceptorLayer::new(move |req: Request<()>| {
            if let Some(token) = &token {
                check_token(req.metadata(), token)?;
            }
            Ok(req)
        }))
        .add_service(EventBaseServer::new(service))
        .serve(addr)
        .await
}

// ────────────────────────────── client helpers ──────────────────────────────

/// The client produced by [`connect_with_token`] / [`connect_tls`]: a channel
/// wrapped in a bearer‑token [`AuthInterceptor`].
pub type AuthClient = EventBaseClient<
    tonic::codegen::InterceptedService<tonic::transport::Channel, AuthInterceptor>,
>;

/// Connect a control‑plane client to `addr` (scheme optional; `http://` is
/// assumed when omitted). This is the one‑line entry `ebctl` and remote
/// admin tools build on.
///
/// ```no_run
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let mut client = event_base_grpc::connect("127.0.0.1:50051").await?;
/// let resp = client.list_topics(event_base_grpc::Empty::default()).await?;
/// println!("{} topics", resp.into_inner().total);
/// # Ok(())
/// # }
/// ```
pub async fn connect(
    addr: impl Into<String>,
) -> Result<EventBaseClient<tonic::transport::Channel>, tonic::transport::Error> {
    EventBaseClient::connect(http_uri(addr.into())).await
}

/// Connect to a plain (non‑TLS) server that requires a bearer token. Every
/// request carries `authorization: Bearer <token>` via [`AuthInterceptor`], so
/// callers never touch per‑request metadata by hand.
pub async fn connect_with_token(
    addr: impl Into<String>,
    token: impl Into<String>,
) -> Result<AuthClient, tonic::transport::Error> {
    let channel = tonic::transport::Endpoint::from_shared(http_uri(addr.into()))?
        .connect()
        .await?;
    Ok(EventBaseClient::with_interceptor(
        channel,
        AuthInterceptor {
            token: token.into(),
        },
    ))
}

/// Connect over TLS, verifying the server against `ca_pem`. When `token` is
/// `Some`, requests also carry the bearer token. Requires the `tls` feature.
#[cfg(feature = "tls")]
pub async fn connect_tls(
    addr: impl Into<String>,
    ca_pem: Vec<u8>,
    token: Option<String>,
) -> Result<AuthClient, tonic::transport::Error> {
    use tonic::transport::{Certificate, ClientTlsConfig};
    let uri = addr.into();
    let uri = if uri.contains("://") {
        uri
    } else {
        format!("https://{uri}")
    };
    let tls = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca_pem));
    let channel = tonic::transport::Endpoint::from_shared(uri)?
        .tls_config(tls)?
        .connect()
        .await?;
    Ok(EventBaseClient::with_interceptor(
        channel,
        AuthInterceptor {
            token: token.unwrap_or_default(),
        },
    ))
}

/// Add the `http://` scheme when a bare `host:port` was given.
fn http_uri(addr: String) -> String {
    if addr.contains("://") {
        addr
    } else {
        format!("http://{addr}")
    }
}

/// Attach a bearer token to an outgoing request. Exposed so callers driving a
/// plain [`EventBaseClient`] (constructed by hand, without TLS) can authenticate
/// per‑request without reimplementing the metadata key. A token containing bytes
/// illegal in an HTTP header is dropped here (the server then answers
/// `Unauthenticated`), never a panic.
///
/// ```
/// let mut req = tonic::Request::new(event_base_grpc::Empty::default());
/// event_base_grpc::attach_token(&mut req, "s3cr3t");
/// ```
pub fn attach_token<T>(req: &mut Request<T>, token: &str) {
    use tonic::metadata::AsciiMetadataValue;
    if let Ok(value) = AsciiMetadataValue::try_from(format!("Bearer {token}")) {
        req.metadata_mut().insert("authorization", value);
    }
}

/// Client interceptor that injects the bearer token on every call.
#[derive(Clone)]
pub struct AuthInterceptor {
    token: String,
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if !self.token.is_empty() {
            attach_token(&mut req, &self.token);
        }
        Ok(req)
    }
}
