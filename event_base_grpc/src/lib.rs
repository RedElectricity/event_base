use crate::server::EventBaseService;
use crate::server::event_base::event_base_server::EventBaseServer;
use std::net::SocketAddr;
use tonic::metadata::MetadataMap;
use tonic::service::InterceptorLayer;
use tonic::transport::Server;
use tonic::{Request, Status};

pub mod server;

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

/// Serves the control plane on `addr` **without authentication**.
///
/// Kept for single‑machine use (Hermes‑style local admin). Anywhere the port
/// may be reachable by another process, prefer [`serve_with_token`] or
/// [`serve_tls`].
pub async fn serve(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let service = EventBaseService;
    Server::builder()
        .add_service(EventBaseServer::new(service))
        .serve(addr)
        .await?;
    Ok(())
}

/// Serves the control plane requiring `authorization: Bearer <token>` on
/// every RPC. The comparison is constant‑time; failures map to
/// `Code::Unauthenticated`.
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
/// is enabled. `cert_pem`/`key_pem` are the server's PEM‑encoded leaf
/// certificate and private key; pass `token: Some(..)` to also require the
/// bearer token inside the encrypted channel.
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
        .await?;
    Ok(())
}
