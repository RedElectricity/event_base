//! Token‑gate coverage for the gRPC control plane (`serve_with_token`).
//!
//! Exercises `resp_check` — the only RPC that needs no booted system —
//! through the interceptor: missing, wrong, and correct credentials.

use event_base_grpc::server::event_base::Empty;
use event_base_grpc::server::event_base::event_base_client::EventBaseClient;
use std::time::Duration;
use tonic::metadata::MetadataValue;
use tonic::{Request, transport::Channel};

async fn connect(addr: std::net::SocketAddr) -> EventBaseClient<Channel> {
    // The server task binds asynchronously; retry until the listener answers.
    let url = format!("http://{addr}");
    for _ in 0..40 {
        match EventBaseClient::connect(url.clone()).await {
            Ok(client) => return client,
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    panic!("grpc server never came up on {addr}");
}

fn bearer(token: &str) -> MetadataValue<tonic::metadata::Ascii> {
    MetadataValue::try_from(format!("Bearer {token}")).expect("ascii token")
}

fn request_with_auth(value: MetadataValue<tonic::metadata::Ascii>) -> Request<Empty> {
    let mut req = Request::new(Empty {});
    req.metadata_mut().insert("authorization", value);
    req
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_with_token_gates_every_rpc() {
    // Claim a free port with a throwaway listener, then hand it to the server
    // (connect retries cover the tiny race window).
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);

    let server = tokio::spawn(event_base_grpc::serve_with_token(addr, "sekret"));
    let mut client = connect(addr).await;

    // No credential → Unauthenticated, before the service is touched.
    let err = client.resp_check(Empty {}).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated, "{err}");

    // Wrong token → still Unauthenticated.
    let mut c2 = connect(addr).await;
    let err = c2
        .resp_check(request_with_auth(bearer("nope")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated, "{err}");

    // Correct bearer token → allowed through.
    let mut c3 = connect(addr).await;
    let resp = c3
        .resp_check(request_with_auth(bearer("sekret")))
        .await
        .expect("bearer token accepted");
    assert!(resp.into_inner().ready);

    // Bare token (no "Bearer " prefix) is accepted too — handy for curl‑style
    // debugging tools that set the header verbatim.
    let mut c4 = connect(addr).await;
    let resp = c4
        .resp_check(request_with_auth(
            MetadataValue::try_from("sekret").unwrap(),
        ))
        .await
        .expect("raw token accepted");
    assert!(resp.into_inner().ready);

    server.abort();
}
