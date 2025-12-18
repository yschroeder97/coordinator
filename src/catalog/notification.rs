use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotificationScope {
    Worker,
    Query,
}

pub struct NotificationManager {
    worker_sender: watch::Sender<()>,
    worker_receiver: watch::Receiver<()>,
    query_sender: watch::Sender<()>,
    query_receiver: watch::Receiver<()>,
}

impl NotificationManager {
    pub fn new() -> Self {
        let (worker_sender, worker_receiver) = watch::channel(());
        let (query_sender, query_receiver) = watch::channel(());
        
        Self {
            worker_sender,
            worker_receiver,
            query_sender,
            query_receiver,
        }
    }

    pub fn subscribe(&self, scope: NotificationScope) -> watch::Receiver<()> {
        match scope {
            NotificationScope::Worker => self.worker_receiver.clone(),
            NotificationScope::Query => self.query_receiver.clone(),
        }
    }

    pub fn notify(&self, scope: NotificationScope) {
        match scope {
            NotificationScope::Worker => {
                let _ = self.worker_sender.send(());
            }
            NotificationScope::Query => {
                let _ = self.query_sender.send(());
            }
        }
    }
}