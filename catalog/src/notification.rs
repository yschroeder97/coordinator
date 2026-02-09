use tokio::sync::watch;

/// Channel for notifying reconcilers of client intent changes.
///
/// This implements a level-triggered notification system where subscribers
/// are notified that "something changed" and should re-query the database
/// for the current state.
///
/// Notified when client intent changes (e.g., new query created, drop requested).
/// Reconcilers should wake up and call `get_mismatch()` to find work.
pub struct IntentChannel<I> {
    intent_sender: watch::Sender<I>,
    intent_receiver: watch::Receiver<I>,
}

impl<I> IntentChannel<I> {
    pub fn new(initial_intent: I) -> Self {
        let (intent_sender, intent_receiver) = watch::channel(initial_intent);
        Self {
            intent_sender,
            intent_receiver,
        }
    }

    /// Notify reconcilers that client intent has changed.
    /// Called by catalog write methods (create, drop, etc.).
    pub fn notify_intent(&self, intent: I) {
        self.intent_sender
            .send(intent)
            .expect("Receiver is owned and should therefore be alive");
    }

    /// Subscribe to intent notifications (for reconcilers).
    pub fn subscribe_intent(&self) -> watch::Receiver<I> {
        self.intent_receiver.clone()
    }
}

/// Trait for catalogs that support intent notifications.
///
/// Implementors provide subscriptions for intent changes
/// (client → reconciler): new entities, state change requests.
pub trait NotifiableCatalog {
    type Intent;

    /// Subscribe to be notified when client intent changes.
    /// Reconcilers use this to wake up and process new work.
    fn subscribe_intent(&self) -> watch::Receiver<Self::Intent>;
}
