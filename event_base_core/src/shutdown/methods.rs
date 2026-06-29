//! Concrete shutdown implementations for different strategies.
//!
//! These functions can be called programmatically to shut down workers using
//! various policies, such as two‑stage, graceful, force, timeout, idle‑only,
//! and batched shutdowns.

use crate::error::CoreError;
use crate::queues::consumer_router::ConsumerRouter;
use crate::worker::WorkerStatus::Idle;
use std::time::Duration;
use tokio::sync::broadcast;

/// Performs a two‑stage shutdown of all workers.
///
/// First, it sends a shutdown signal via the broadcast channel. Then it polls
/// periodically, waiting for all workers to complete their shutdown. If the
/// timeout is reached before all workers are done, it forcefully removes the
/// remaining workers by calling their `shutdown` method with zero timeout and
/// deleting them from the router.
///
/// # Arguments
/// * `shutdown_tx` - The broadcast sender to signal workers.
/// * `timeout` - Maximum total time to wait for graceful shutdown.
/// * `poll_interval` - How often to check worker status.
///
/// # Returns
/// `Ok(())` if all workers shut down gracefully within the timeout, or after
/// forced cleanup. Returns `CoreError` if any worker's forceful shutdown fails.
///
/// # Note
/// This function will always attempt to clean up all workers, even if the
/// timeout expires. It logs warnings for any workers that are force‑removed.
pub async fn shutdown_all_workers_two_stage(
    shutdown_tx: broadcast::Sender<()>,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(), CoreError> {
    let _ = shutdown_tx.send(());

    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        let workers = ConsumerRouter::global().get_all_workers().await;
        if workers.is_empty() {
            return Ok(());
        }
        let all_done = workers.iter().all(|w| w.is_shutdown_complete());
        if all_done {
            // 全部完成，统一清理
            for w in workers {
                let _ = ConsumerRouter::global().del_worker(&w.name).await;
            }
            return Ok(());
        }
        tokio::time::sleep(poll_interval).await;
    }

    let workers = ConsumerRouter::global().get_all_workers().await;
    for worker in workers {
        worker
            .shutdown(Duration::new(0, 0), Option::from(Duration::new(0, 0)))
            .await?;
        let _ = ConsumerRouter::global().del_worker(&worker.name).await;
        tracing::warn!("Force removed worker: {}", worker.name);
    }
    Ok(())
}

/// Gracefully shuts down a single worker by its ID.
///
/// This function polls the worker until it becomes idle, then calls its
/// `shutdown` method with zero‑duration timeouts and removes it from the router.
///
/// # Arguments
/// * `worker_id` - The name of the worker to shut down.
/// * `poll_interval` - How often to check the worker's status.
///
/// # Errors
/// Returns `CoreError` if the worker does not exist or if the shutdown call fails.
pub async fn graceful_shutdown(worker_id: &str, poll_interval: Duration) -> Result<(), CoreError> {
    let worker = ConsumerRouter::global().get_worker(worker_id).await?;

    loop {
        if worker.get_status().await == Idle {
            worker
                .shutdown(Duration::new(0, 0), Option::from(Duration::new(0, 0)))
                .await?;
            ConsumerRouter::global().del_worker(&worker.name).await?;
            break;
        }
        tokio::time::sleep(poll_interval).await;
    }

    Ok(())
}

/// Immediately and forcefully shuts down all workers.
///
/// This function does not wait for workers to finish processing; it calls
/// `shutdown` with zero timeouts and deletes each worker from the router.
/// Warnings are logged for each force‑removed worker.
pub async fn shutdown_force() {
    let workers = ConsumerRouter::global().get_all_workers().await;
    for worker in workers {
        let _ = worker
            .shutdown(Duration::new(0, 0), Option::from(Duration::new(0, 0)))
            .await;
        let _ = ConsumerRouter::global().del_worker(&worker.name).await;
    }
}

/// Waits for a specified timeout and then forcefully shuts down all workers.
///
/// This is equivalent to sleeping for `timeout` and then calling [`shutdown_force`].
///
/// # Arguments
/// * `timeout` - The duration to wait before forcing shutdown.
pub async fn shutdown_timeout(timeout: Duration) {
    tokio::time::sleep(timeout).await;
    shutdown_force().await;
}

/// Shuts down only workers that are currently idle.
///
/// Workers that are processing messages are left untouched. This is a
/// non‑blocking operation that only affects idle workers.
pub async fn shutdown_idle_only() {
    let workers = ConsumerRouter::global().get_all_workers().await;
    for worker in workers {
        if worker.get_status().await == Idle {
            let _ = worker
                .shutdown(Duration::new(0, 0), Option::from(Duration::new(0, 0)))
                .await;
            let _ = ConsumerRouter::global().del_worker(&worker.name).await;
        }
    }
}

/// Shuts down workers in batches with a delay between each batch.
///
/// This iterates over all workers in chunks of `batch_size`, shuts down each
/// worker in the chunk, and then waits for `interval` before processing the
/// next chunk.
///
/// # Arguments
/// * `batch_size` - The number of workers to shut down per batch.
/// * `interval` - The delay between batches.
pub async fn shutdown_batched(batch_size: usize, interval: Duration) {
    let workers = ConsumerRouter::global().get_all_workers().await;
    for chunk in workers.chunks(batch_size) {
        for worker in chunk {
            let _ = worker
                .shutdown(Duration::new(0, 0), Option::from(Duration::new(0, 0)))
                .await;
            let _ = ConsumerRouter::global().del_worker(&worker.name).await;
        }
        tokio::time::sleep(interval).await;
    }
}
