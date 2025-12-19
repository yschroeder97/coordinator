use tokio::sync::watch;

pub trait Notifier {
    type Notification;
    fn subscribe(&self) -> watch::Receiver<Self::Notification>;
    fn notify(&self);
}
