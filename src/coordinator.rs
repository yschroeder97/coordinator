use crate::db::Database;
use crate::events::{Heartbeat, RegisterWorker};
use crate::message_bus::{MessageBusReceiver, MessageBusSender, message_bus};
use crate::messages::Message;
use crate::requests::{
    CreateLogicalSourceRequest, CreatePhysicalSourceRequest, CreateQueryRequest, CreateSinkRequest,
    CreateWorkerRequest,
};
use std::time::Duration;
use tracing::log::warn;

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
impl_from!(
    CreateLogicalSource,
    CreateLogicalSourceRequest,
    CoordinatorRequest
);
impl_from!(
    CreatePhysicalSource,
    CreatePhysicalSourceRequest,
    CoordinatorRequest
);
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
        tokio::spawn(async move {
            let db = Database::from_env().await.unwrap();
            MessageReceiver::new(receiver, batch_size, db).event_loop()
        });
    });

    sender
}

pub type CoordinatorMessage = Message<CoordinatorEvent, CoordinatorRequest>;

pub struct MessageReceiver {
    receiver: MessageBusReceiver<CoordinatorEvent, CoordinatorRequest>,
    batch_size: usize,
    database: Database,
}

impl MessageReceiver {
    pub fn new(
        receiver: MessageBusReceiver<CoordinatorEvent, CoordinatorRequest>,
        batch_size: usize,
        database: Database,
    ) -> MessageReceiver {
        Self {
            receiver,
            batch_size,
            database,
        }
    }

    pub async fn event_loop(mut self) -> () {
        while let Some(batch) = self.receiver.recv() {
            for msg in batch.into_iter() {
                match msg {
                    Message::Request(CoordinatorRequest::CreateLogicalSource(create_logical)) => {
                        let insert_result = self
                            .database
                            .insert_logical_source(&create_logical.payload)
                            .await;
                        if let Err(recipient_dropped) =
                            create_logical.respond(insert_result.map_err(Into::into))
                        {
                            warn!("Failed to respond to request: {:?}", recipient_dropped);
                        }
                    }
                    Message::Request(CoordinatorRequest::CreatePhysicalSource(create_physical)) => {
                        let insert_result = self
                            .database
                            .insert_physical_source(&create_physical.payload)
                            .await;
                        if let Err(recipient_dropped) =
                            create_physical.respond(insert_result.map_err(Into::into))
                        {
                            warn!("Failed to respond to request: {:?}", recipient_dropped);
                        }
                    }
                    Message::Request(CoordinatorRequest::CreateSink(create_sink)) => {
                        let insert_result = self.database.insert_sink(&create_sink.payload).await;
                        if let Err(recipient_dropped) =
                            create_sink.respond(insert_result.map_err(Into::into))
                        {
                            warn!("Failed to respond to request: {:?}", recipient_dropped);
                        }
                    }
                    Message::Request(_) => todo!(),
                    Message::Event(_) => todo!(),
                }
            }
        }
    }
}

#[cfg(test)]
mod coordinator_tests {
    use super::*;

    #[tokio::test]
    async fn test_simple_request() {
        assert!(true);
    }
}
