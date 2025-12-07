use flume::{Receiver, Sender};

#[derive(Clone)]
pub struct CoordinatorHandle<Req> {
    sender: Sender<Req>,
}

pub struct CoordinatorReceiver<Req> {
    receiver: Receiver<Req>,
}

impl<Req: Send + Sync + 'static> CoordinatorHandle<Req> {}

impl<Req: Send + Sync + 'static> CoordinatorReceiver<Req> {
    pub(crate) fn recv(&mut self) -> Option<Req> {
        self.receiver.recv().ok()
    }
}

pub fn message_bus<Req: Send + Sync + 'static>(
    capacity: usize,
) -> (CoordinatorHandle<Req>, CoordinatorReceiver<Req>) {
    assert!(capacity > 0, "Batch size must be greater than 0");
    let (tx, rx) = flume::bounded(capacity);

    (
        CoordinatorHandle { sender: tx },
        CoordinatorReceiver { receiver: rx },
    )
}
