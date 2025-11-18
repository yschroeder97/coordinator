use crate::coordinator::CoordinatorRequest;
use crate::errors::BoxedError;
use crate::requests::Request;
use flume::{Receiver, Sender};

#[derive(Clone)]
pub struct MessageBusSender<Req> {
    sender: Sender<Req>,
}

pub struct MessageBusReceiver<Req> {
    receiver: Receiver<Req>,
}

impl<Req: Send + Sync + 'static> MessageBusSender<Req> {
    pub async fn ask<P, R>(&self, payload: P) -> Result<R, BoxedError>
    where
        Request<P, R>: Into<Req>,
    {
        let (tx, rx) = flume::bounded(1);
        let request = Request {
            payload,
            respond_to: tx,
        };
        self.sender.send_async(request.into()).await?;
        Ok(rx.recv_async().await?)
    }

    pub fn ask_blocking<P, R>(&self, payload: P) -> Result<R, BoxedError>
    where
        Request<P, R>: Into<Req>,
    {
        let (tx, rx) = flume::bounded(1);
        let request = Request {
            payload,
            respond_to: tx,
        };
        self.sender.send(request.into())?;
        Ok(rx.recv()?)
    }
}

impl<Req: Send + Sync + 'static> MessageBusReceiver<Req> {
    pub fn recv(&mut self) -> Option<Req> {
        match self.receiver.recv() {
            Ok(req) => Some(req),
            Err(_) => None,
        }
    }
}

pub fn message_bus<Req: Send + Sync + 'static>(
    batch_size: usize,
) -> (MessageBusSender<Req>, MessageBusReceiver<Req>) {
    assert!(batch_size > 0, "Batch size must be greater than 0");
    let (tx, rx) = flume::bounded(batch_size);

    (
        MessageBusSender { sender: tx },
        MessageBusReceiver { receiver: rx },
    )
}

pub fn coordinator_message_bus(
    batch_size: usize,
) -> (
    MessageBusSender<CoordinatorRequest>,
    MessageBusReceiver<CoordinatorRequest>,
) {
    message_bus(batch_size)
}
