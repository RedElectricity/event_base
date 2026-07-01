//! Additional tests for CoreError variants, AuditError Display,
//! and other error-related coverage.

use event_base_core::error::CoreError;
use event_base_core::error::audit::AuditError;
use std::time::Duration;

// ──────────────────────────────────────────────
// AuditError tests (not covered elsewhere)
// ──────────────────────────────────────────────

#[test]
fn audit_error_display_write() {
    let err = AuditError::Write("disk full".to_string());
    assert_eq!(err.to_string(), "Write error: disk full");
}

#[test]
fn audit_error_display_buffer_full() {
    let err = AuditError::BufferFull;
    assert_eq!(err.to_string(), "Buffer full");
}

#[test]
fn audit_error_display_missing_span() {
    let err = AuditError::MissingSpan;
    assert_eq!(err.to_string(), "Missing Span");
}

#[test]
fn audit_error_debug() {
    let err = AuditError::Write("oops".to_string());
    let debug = format!("{:?}", err);
    assert!(debug.contains("Write"));
    assert!(debug.contains("oops"));
}

// ──────────────────────────────────────────────
// CoreError::from(AuditError)
// ──────────────────────────────────────────────

#[test]
fn core_error_from_audit_error() {
    let audit_err = AuditError::BufferFull;
    let core: CoreError = audit_err.into();
    let display = core.to_string();
    assert!(display.contains("Audit error"));
    assert!(display.contains("Buffer full"));
}

// ──────────────────────────────────────────────
// Additional CoreError variant Display tests
// ──────────────────────────────────────────────

#[test]
fn core_error_queue_send_error_display() {
    let err = CoreError::QueueSendError("channel closed".to_string());
    assert_eq!(err.to_string(), "Queue Send Error: channel closed");
}

#[test]
fn core_error_invalid_parameter_display() {
    let err = CoreError::InvalidParameter("batch size must be > 0".to_string());
    assert_eq!(err.to_string(), "Invalid Parameter: batch size must be > 0");
}

#[test]
fn core_error_invalid_data_display() {
    let err = CoreError::InvalidData("malformed header".to_string());
    assert_eq!(err.to_string(), "Invalid Type: malformed header");
}

#[test]
fn core_error_task_join_error_display() {
    let err = CoreError::TaskJoinError("worker-a panicked".to_string());
    assert_eq!(err.to_string(), "Task Join Error: worker-a panicked");
}

#[test]
fn core_error_already_initialized_display() {
    let err = CoreError::AlreadyInitialized;
    assert_eq!(err.to_string(), "Object already exists");
}

#[test]
fn core_error_error_time_display() {
    let err = CoreError::ErrorTime;
    assert_eq!(err.to_string(), "Error Time");
}

#[test]
fn core_error_shutting_down_display() {
    let err = CoreError::ShuttingDown;
    assert_eq!(err.to_string(), "Shutting down");
}

#[test]
fn core_error_other_display() {
    let err = CoreError::Other("custom failure".to_string());
    assert_eq!(err.to_string(), "Other: custom failure");
}

#[test]
fn core_error_timeout_display() {
    let dur = Duration::from_secs(5);
    let err = CoreError::Timeout(dur);
    let display = err.to_string();
    assert!(display.contains("Timeout"));
    assert!(display.contains("5s"));
}

#[test]
fn core_error_worker_not_found_display() {
    let err = CoreError::WorkerNotFound("worker-xyz".to_string());
    assert_eq!(err.to_string(), "Worker Not Found: worker-xyz");
}

#[test]
fn core_error_unsupported_display() {
    let err = CoreError::Unsupported("broadcast on worker node".to_string());
    assert_eq!(err.to_string(), "Unsupported: broadcast on worker node");
}

// ──────────────────────────────────────────────
// CoreError Debug format
// ──────────────────────────────────────────────

#[test]
fn core_error_debug_contains_message() {
    let err = CoreError::ShuttingDown;
    let debug = format!("{:?}", err);
    assert!(debug.contains("ShuttingDown") || debug.contains("Shutting down"));
}

#[test]
fn core_error_from_io_error() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
    let core: CoreError = io_err.into();
    let display = core.to_string();
    assert!(display.contains("IO error"));
    assert!(display.contains("file missing"));
}

#[test]
fn core_error_from_serde_json_error() {
    let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
    let core: CoreError = json_err.into();
    let display = core.to_string();
    assert!(display.contains("Serde"));
}

#[test]
fn core_error_from_system_time_error() {
    use std::time::SystemTime;
    // Create a SystemTimeError by subtracting a later time from an earlier one
    let early = SystemTime::now();
    let later = early + Duration::from_secs(1);
    let result = early.duration_since(later);
    assert!(result.is_err());
    let time_err = result.unwrap_err();
    let core: CoreError = time_err.into();
    let display = core.to_string();
    assert!(display.contains("Process Time Error"));
}
