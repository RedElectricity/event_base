//! Smoke test that the `ebctl` binary actually talks to a served control plane.
//!
//! Spawns a `ServeConfig` server on a free port and runs the compiled `ebctl`
//! against it. `RespCheck` (`ping`) needs no booted system, so it is the safe
//! round trip to assert here; the read RPCs that consult core globals are
//! covered by `event_base_test`'s `grpc_control_plane`.

use std::process::Command;
use tokio::net::TcpListener;

async fn free_addr() -> std::net::SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ebctl_ping_round_trips() {
    let addr = free_addr().await;
    let server = tokio::spawn(async move {
        event_base_grpc::ServeConfig::new(addr)
            .reflection(false)
            .health(false)
            .serve()
            .await
    });

    // Wait until the listener answers before invoking the CLI.
    let mut ready = false;
    for _ in 0..40 {
        if event_base_grpc::connect(addr.to_string()).await.is_ok() {
            ready = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(ready, "server never came up");

    let out = Command::new(env!("CARGO_BIN_EXE_ebctl"))
        .args(["-a", &addr.to_string(), "ping"])
        .output()
        .expect("run ebctl");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("ready=true"),
        "ebctl ping failed: status={} stdout={stdout} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );

    // Unreachable address → non‑zero exit (the client connect error surfaces).
    let dead = free_addr().await;
    let bad = Command::new(env!("CARGO_BIN_EXE_ebctl"))
        .args(["-a", &dead.to_string(), "ping"])
        .output()
        .expect("run ebctl against dead port");
    assert!(!bad.status.success(), "ping to a dead address must fail");

    server.abort();
}

#[test]
fn ebctl_help_lists_all_commands() {
    let out = Command::new(env!("CARGO_BIN_EXE_ebctl"))
        .arg("--help")
        .output()
        .expect("ebctl --help");
    let stdout = String::from_utf8_lossy(&out.stdout);
    for cmd in [
        "ping",
        "status",
        "topics",
        "workers",
        "metrics",
        "publish",
        "shutdown",
    ] {
        assert!(stdout.contains(cmd), "`{cmd}` missing from help: {stdout}");
    }
}
