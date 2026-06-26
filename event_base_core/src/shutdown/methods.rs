use crate::error::CoreError;
use crate::queues::consumer_router::ConsumerRouter;
use crate::worker::WorkerStatus::Idle;
use std::time::Duration;
use tokio::sync::broadcast;

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
        tokio::time::sleep(poll_interval).await;
    }

    let workers = ConsumerRouter::global().get_all_workers().await;
    for worker in workers {
        worker
            .shutdown(Duration::new(0, 0), Option::from(Duration::new(0, 0)))
            .await;
        let _ = ConsumerRouter::global().del_worker(&worker.name).await;
        tracing::warn!("Force removed worker: {}", worker.name);
    }
    Ok(())
}

pub async fn graceful_shutdown(worker_id: &str, poll_interval: Duration) -> Result<(), CoreError> {
    let worker = ConsumerRouter::global().get_worker(worker_id).await;

    loop {
        if worker.get_status().await == Idle {
            worker
                .shutdown(Duration::new(0, 0), Option::from(Duration::new(0, 0)))
                .await;
            ConsumerRouter::global().del_worker(&worker.name).await?;
            break;
        }
        tokio::time::sleep(poll_interval).await;
    }

    Ok(())
}

pub async fn shutdown_force() {
    let workers = ConsumerRouter::global().get_all_workers().await;
    for worker in workers {
        worker
            .shutdown(Duration::new(0, 0), Option::from(Duration::new(0, 0)))
            .await;
        let _ = ConsumerRouter::global().del_worker(&worker.name).await;
    }
}

pub async fn shutdown_timeout(timeout: Duration) {
    tokio::time::sleep(timeout).await;
    shutdown_force().await;
}

pub async fn shutdown_idle_only() {
    let workers = ConsumerRouter::global().get_all_workers().await;
    for worker in workers {
        if worker.get_status().await == Idle {
            worker
                .shutdown(Duration::new(0, 0), Option::from(Duration::new(0, 0)))
                .await;
            let _ = ConsumerRouter::global().del_worker(&worker.name).await;
        }
    }
}

pub async fn shutdown_batched(batch_size: usize, interval: Duration) {
    let workers = ConsumerRouter::global().get_all_workers().await;
    for chunk in workers.chunks(batch_size) {
        for worker in chunk {
            worker
                .shutdown(Duration::new(0, 0), Option::from(Duration::new(0, 0)))
                .await;
            let _ = ConsumerRouter::global().del_worker(&worker.name).await;
        }
        tokio::time::sleep(interval).await;
    }
}
