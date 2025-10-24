use crate::catalog::Catalog;
use crate::events::{Heartbeat, RegisterWorker};
use crate::message_bus::{MessageBusReceiver, MessageBusSender, message_bus};
use crate::messages::Message;
use crate::requests::{CreateLogicalSourceRequest, CreatePhysicalSourceRequest, CreateQueryRequest, CreateSinkRequest, CreateWorkerRequest, Request, ShowLogicalSourcesRequest, ShowPhysicalSources, ShowPhysicalSourcesRequest, ShowQueriesRequest, ShowSinksRequest, ShowWorkersRequest};
use std::time::Duration;
use tokio::runtime::Runtime;
use tracing::log::warn;
use tracing::{Instrument, info_span};
use crate::db_errors::DatabaseError;
use crate::errors::{BoxedError, CoordinatorError};

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
    ShowLogicalSources(ShowLogicalSourcesRequest),
    ShowPhysicalSources(ShowPhysicalSourcesRequest),
    ShowSinks(ShowSinksRequest),
    ShowWorkers(ShowWorkersRequest),
    ShowQueries(ShowQueriesRequest),
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

impl_from!(ShowLogicalSources, ShowLogicalSourcesRequest, CoordinatorRequest);
impl_from!(ShowPhysicalSources, ShowPhysicalSourcesRequest, CoordinatorRequest);
impl_from!(ShowSinks, ShowSinksRequest, CoordinatorRequest);
impl_from!(ShowWorkers, ShowWorkersRequest, CoordinatorRequest);
impl_from!(ShowQueries, ShowQueriesRequest, CoordinatorRequest);

pub fn start_coordinator(
    batch_size: usize,
    timeout: Duration,
) -> Result<MessageBusSender<CoordinatorEvent, CoordinatorRequest>, BoxedError> {
    let (sender, receiver) =
        message_bus::<CoordinatorEvent, CoordinatorRequest>(batch_size, timeout);

    let rt = Runtime::new().map_err(|e| format!("Failed to create tokio runtime: {}", e))?;

    let catalog = rt.block_on(async {
        Catalog::from_env().await
    })?;

    std::thread::spawn(move || {
        rt.block_on(
            async move {
                CoordinatorReceiver::new(receiver, batch_size, catalog)
                    .event_loop()
                    .await;
            }
            .instrument(info_span!("receiver_event_loop")),
        );
    });

    Ok(sender)
}

pub type CoordinatorMessage = Message<CoordinatorEvent, CoordinatorRequest>;

pub struct CoordinatorReceiver {
    receiver: MessageBusReceiver<CoordinatorEvent, CoordinatorRequest>,
    batch_size: usize,
    catalog: Catalog,
}

impl CoordinatorReceiver {
    pub fn new(
        receiver: MessageBusReceiver<CoordinatorEvent, CoordinatorRequest>,
        batch_size: usize,
        catalog: Catalog,
    ) -> CoordinatorReceiver {
        Self {
            receiver,
            batch_size,
            catalog,
        }
    }
    
    async fn reply<ReqPayload, RspPayload>(
        &self,
        request: Request<ReqPayload, Result<RspPayload, CoordinatorError>>,
        result: Result<RspPayload, DatabaseError>,
    ) {
        if let Err(recipient_dropped) = request.respond(result.map_err(Into::into)) {
            warn!("Failed to respond to request: {:?}", recipient_dropped);
        }
    }

    pub async fn event_loop(mut self) -> () {
        while let Some(batch) = self.receiver.recv() {
            for msg in batch.into_iter() {
                match msg {
                    Message::Request(CoordinatorRequest::CreateLogicalSource(create_logical)) => {
                        let insert_result = self
                            .catalog
                            .insert_logical_source(&create_logical.payload)
                            .await;
                        self.reply(create_logical, insert_result).await;
                    }
                    Message::Request(CoordinatorRequest::CreatePhysicalSource(create_physical)) => {
                        let insert_result = self
                            .catalog
                            .insert_physical_source(&create_physical.payload)
                            .await;
                        self.reply(create_physical, insert_result).await;
                    }
                    Message::Request(CoordinatorRequest::CreateSink(create_sink)) => {
                        let insert_result = self.catalog.insert_sink(&create_sink.payload).await;
                        self.reply(create_sink, insert_result).await;
                    }
                    Message::Request(CoordinatorRequest::CreateWorker(create_worker)) => {
                        let insert_result = self.catalog.insert_worker(&create_worker.payload).await;
                        self.reply(create_worker, insert_result).await;
                    }
                    Message::Request(CoordinatorRequest::CreateQuery(create_query)) => {
                        let insert_result = self.catalog.insert_query(&create_query.payload).await;
                        self.reply(create_query, insert_result).await;
                    }
                    Message::Request(CoordinatorRequest::ShowLogicalSources(show_logical)) => {
                        let insert_result = self.catalog.show_logical_sources(&show_logical.payload).await;
                        self.reply(show_logical, insert_result).await;
                    }
                    Message::Request(CoordinatorRequest::ShowPhysicalSources(show_physical)) => {
                        let insert_result = self.catalog.show_physical_sources(&show_physical.payload).await;
                        self.reply(show_physical, insert_result).await;
                    }
                    Message::Event(_) => todo!(),
                    _ => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod coordinator_tests {
    use crate::requests::{ShowLogicalSources, ShowPhysicalSources, ShowQueries, ShowSinks, ShowWorkers};
    use super::*;

    #[tokio::test]
    async fn test_coordinator_integration() {
        // Create catalog from the test pool
        let client = start_coordinator(10, Duration::from_millis(250)).unwrap();

        let workers_stmt = ShowWorkers {
            host_name: None,
        };
        let response = client.ask(workers_stmt).await.unwrap();

        let logical_stmt = ShowLogicalSources {
            source_name: None,
        };
        let response = client.ask(logical_stmt).await.unwrap();

        let physical_stmt = ShowPhysicalSources {
            for_logical_source: None,
            on_node: None,
            by_type: None,
        };
        let response = client.ask(physical_stmt).await.unwrap();

        let sinks_stmt = ShowSinks {
            name: None,
            on_node: None,
            by_type: None,
        };
        let response = client.ask(sinks_stmt).await.unwrap();

        let queries_stmt = ShowQueries {
            query_id: None,
            by_sink: None,
        };
        let response = client.ask(queries_stmt).await.unwrap();
    }
}
