use crate::events::{Heartbeat, RegisterWorker};
use crate::message_bus::{MessageBusReceiver, MessageBusSender, message_bus};
use crate::requests::{CreateLogicalSourceRequest, CreatePhysicalSourceRequest, CreateQueryRequest, CreateSinkRequest, CreateWorkerRequest};
use std::time::Duration;
use crate::messages::Message;


#[derive(Clone)]
pub enum CoordinatorEvent {
    Heartbeat(Heartbeat),
    RegisterWorker(RegisterWorker),
}
pub enum CoordinatorRequest {
    CreateLogicalSource(CreateLogicalSourceRequest),
    CreatePhysicalSource(CreatePhysicalSourceRequest),
    CreateSink(CreateSinkRequest),
    CreateWorker(CreateWorkerRequest),
    CreateQuery(CreateQueryRequest),
}

macro_rules! impl_from {
    ($variant:ident, $type:ty, $enum:ident) => {
        impl From<$type> for $enum {
            fn from(value: $type) -> Self {
                $enum::$variant(value)
            }
        }
    };
}

impl_from!(Heartbeat, Heartbeat, CoordinatorEvent);
impl_from!(RegisterWorker, RegisterWorker, CoordinatorEvent);
impl_from!(CreateLogicalSource, CreateLogicalSourceRequest, CoordinatorRequest);
impl_from!(CreatePhysicalSource, CreatePhysicalSourceRequest, CoordinatorRequest);
impl_from!(CreateSink, CreateSinkRequest, CoordinatorRequest);
impl_from!(CreateWorker, CreateWorkerRequest, CoordinatorRequest);
impl_from!(CreateQuery, CreateQueryRequest, CoordinatorRequest);

pub fn start_coordinator(
    batch_size: usize,
    timeout: Duration,
) -> MessageBusSender<CoordinatorEvent, CoordinatorRequest> {
    let (sender, receiver) =
        message_bus::<CoordinatorEvent, CoordinatorRequest>(batch_size, timeout);

    std::thread::spawn(move || {
        MessageReceiver::new(receiver, batch_size).event_loop();
    });

    sender
}

pub type CoordinatorMessage = Message<CoordinatorEvent, CoordinatorRequest>;

pub struct MessageReceiver {
    receiver: MessageBusReceiver<CoordinatorEvent, CoordinatorRequest>,
    batch_size: usize,
}

impl MessageReceiver {
    pub fn new(
        receiver: MessageBusReceiver<CoordinatorEvent, CoordinatorRequest>,
        batch_size: usize
    ) -> MessageReceiver {
        Self { receiver, batch_size }
    }

    pub fn event_loop(mut self) -> () {
        while let Some(_batch) = self.receiver.recv() {
            // do something with the batch
        }
    }
}
