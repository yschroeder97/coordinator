use crate::coordinator::CoordinatorRequest;
use flume::{Receiver, Sender};

pub type CoordinatorHandle = Sender<CoordinatorRequest>;

pub struct CoordinatorReceiver {
    receiver: Receiver<CoordinatorRequest>,
}

impl CoordinatorReceiver {
    pub(crate) async fn recv(&mut self) -> Option<CoordinatorRequest> {
        self.receiver.recv_async().await.ok()
    }
}

pub fn message_bus(capacity: usize) -> (CoordinatorHandle, CoordinatorReceiver) {
    let (tx, rx) = flume::bounded(capacity);

    (tx, CoordinatorReceiver { receiver: rx })
}
