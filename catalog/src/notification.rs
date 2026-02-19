use tokio::sync::watch;

pub struct NotificationChannel {
    intent_tx: watch::Sender<()>,
    intent_rx: watch::Receiver<()>,
    state_tx: watch::Sender<()>,
    state_rx: watch::Receiver<()>,
}

impl NotificationChannel {
    pub fn new() -> Self {
        let (intent_tx, intent_rx) = watch::channel(());
        let (state_tx, state_rx) = watch::channel(());
        Self {
            intent_tx,
            intent_rx,
            state_tx,
            state_rx,
        }
    }

    pub fn notify_intent(&self) {
        self.intent_tx
            .send(())
            .expect("Receiver is owned and should therefore be alive");
    }

    pub fn notify_state(&self) {
        self.state_tx
            .send(())
            .expect("Receiver is owned and should therefore be alive");
    }

    pub fn subscribe_intent(&self) -> watch::Receiver<()> {
        self.intent_rx.clone()
    }

    pub fn subscribe_state(&self) -> watch::Receiver<()> {
        self.state_rx.clone()
    }
}

pub trait NotifiableCatalog {
    fn subscribe_intent(&self) -> watch::Receiver<()>;
    fn subscribe_state(&self) -> watch::Receiver<()>;
}
