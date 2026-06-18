use tokio::sync::broadcast;

pub type ShutdownSender = broadcast::Sender<()>;
pub type ShutdownReceiver = broadcast::Receiver<()>;

pub fn shutdown_channel() -> (ShutdownSender, ShutdownReceiver) {
    // Only one signal
    broadcast::channel(1)
}