use event_base_core::shutdown::messages::{
    ShutdownAck, ShutdownCommand, ShutdownStatus, ShutdownStrategy,
};
use event_base_core::shutdown::shutdown_channel;
use std::time::SystemTime;

#[test]
fn shutdown_channel_create_and_signal() {
    let (tx, mut rx) = shutdown_channel();
    tx.send(()).expect("send should succeed");
    let received = rx.try_recv();
    assert!(received.is_ok(), "shutdown signal should be receivable");
}

#[test]
fn shutdown_channel_cloned_receivers_get_signal() {
    let (tx, rx) = shutdown_channel();
    let mut rx2 = rx;
    tx.send(()).expect("send should succeed");
    assert!(rx2.try_recv().is_ok());
}

#[test]
fn shutdown_command_two_stage_serialization() {
    let cmd = ShutdownCommand {
        strategy: ShutdownStrategy::TwoStage {
            poll_interval_ms: 100,
            force_timeout_secs: 30,
        },
    };
    let json = serde_json::to_string(&cmd).expect("serialize should succeed");
    let decoded: ShutdownCommand = serde_json::from_str(&json).expect("deserialize should succeed");
    match decoded.strategy {
        ShutdownStrategy::TwoStage {
            poll_interval_ms,
            force_timeout_secs,
        } => {
            assert_eq!(poll_interval_ms, 100);
            assert_eq!(force_timeout_secs, 30);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn shutdown_command_graceful_serialization() {
    let cmd = ShutdownCommand {
        strategy: ShutdownStrategy::Graceful {
            worker_name: "worker-a".to_string(),
            poll_interval_ms: 50,
        },
    };
    let json = serde_json::to_string(&cmd).expect("serialize should succeed");
    let decoded: ShutdownCommand = serde_json::from_str(&json).expect("deserialize should succeed");
    match decoded.strategy {
        ShutdownStrategy::Graceful {
            worker_name,
            poll_interval_ms,
        } => {
            assert_eq!(worker_name, "worker-a");
            assert_eq!(poll_interval_ms, 50);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn shutdown_command_force_serialization() {
    let cmd = ShutdownCommand {
        strategy: ShutdownStrategy::Force,
    };
    let json = serde_json::to_string(&cmd).expect("serialize should succeed");
    let decoded: ShutdownCommand = serde_json::from_str(&json).expect("deserialize should succeed");
    assert!(matches!(decoded.strategy, ShutdownStrategy::Force));
}

#[test]
fn shutdown_command_timeout_serialization() {
    let cmd = ShutdownCommand {
        strategy: ShutdownStrategy::Timeout {
            total_timeout_secs: 10,
        },
    };
    let json = serde_json::to_string(&cmd).expect("serialize should succeed");
    let decoded: ShutdownCommand = serde_json::from_str(&json).expect("deserialize should succeed");
    match decoded.strategy {
        ShutdownStrategy::Timeout { total_timeout_secs } => {
            assert_eq!(total_timeout_secs, 10);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn shutdown_command_state_based_idle_serialization() {
    let cmd = ShutdownCommand {
        strategy: ShutdownStrategy::StateBasedIdle,
    };
    let json = serde_json::to_string(&cmd).expect("serialize should succeed");
    let decoded: ShutdownCommand = serde_json::from_str(&json).expect("deserialize should succeed");
    assert!(matches!(decoded.strategy, ShutdownStrategy::StateBasedIdle));
}

#[test]
fn shutdown_command_batched_serialization() {
    let cmd = ShutdownCommand {
        strategy: ShutdownStrategy::Batched {
            batch_size: 4,
            interval_ms: 250,
        },
    };
    let json = serde_json::to_string(&cmd).expect("serialize should succeed");
    let decoded: ShutdownCommand = serde_json::from_str(&json).expect("deserialize should succeed");
    match decoded.strategy {
        ShutdownStrategy::Batched {
            batch_size,
            interval_ms,
        } => {
            assert_eq!(batch_size, 4);
            assert_eq!(interval_ms, 250);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn shutdown_ack_serialization() {
    let ack = ShutdownAck {
        worker_name: "worker-b".to_string(),
        status: ShutdownStatus::Completed,
        timestamp: SystemTime::now(),
        error: None,
    };
    let json = serde_json::to_vec(&ack).expect("serialize should succeed");
    let decoded: ShutdownAck = serde_json::from_slice(&json).expect("deserialize should succeed");
    assert_eq!(decoded.worker_name, "worker-b");
    assert!(matches!(decoded.status, ShutdownStatus::Completed));
    assert!(decoded.error.is_none());
}

#[test]
fn shutdown_ack_with_error() {
    let ack = ShutdownAck {
        worker_name: "worker-c".to_string(),
        status: ShutdownStatus::Failed,
        timestamp: SystemTime::now(),
        error: Some("something went wrong".to_string()),
    };
    let json = serde_json::to_vec(&ack).expect("serialize should succeed");
    let decoded: ShutdownAck = serde_json::from_slice(&json).expect("deserialize should succeed");
    assert!(matches!(decoded.status, ShutdownStatus::Failed));
    assert_eq!(decoded.error.as_deref(), Some("something went wrong"));
}

#[test]
fn shutdown_ack_timeout_status() {
    let ack = ShutdownAck {
        worker_name: "worker-d".to_string(),
        status: ShutdownStatus::Timeout,
        timestamp: SystemTime::now(),
        error: None,
    };
    let json = serde_json::to_vec(&ack).expect("serialize should succeed");
    let decoded: ShutdownAck = serde_json::from_slice(&json).expect("deserialize should succeed");
    assert!(matches!(decoded.status, ShutdownStatus::Timeout));
}
