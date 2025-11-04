pub use crate::data_model::logical_source::{CreateLogicalSource, LogicalSource, ShowLogicalSources};
pub use crate::data_model::physical_source::{
    CreatePhysicalSource, PhysicalSource, ShowPhysicalSources,
};
pub use crate::data_model::query::{CreateQuery, Query, ShowQueries};
pub use crate::data_model::sink::{CreateSink, ShowSinks, Sink};
pub use crate::data_model::worker::{CreateWorker, ShowWorkers, Worker};
use crate::errors::CoordinatorError;

// Request type aliases
pub type CreateLogicalSourceRequest = Request<CreateLogicalSource, Result<(), CoordinatorError>>;
pub type CreatePhysicalSourceRequest = Request<CreatePhysicalSource, Result<(), CoordinatorError>>;
pub type CreateSinkRequest = Request<CreateSink, Result<(), CoordinatorError>>;
pub type CreateWorkerRequest = Request<CreateWorker, Result<(), CoordinatorError>>;
pub type CreateQueryRequest = Request<CreateQuery, Result<(), CoordinatorError>>;

pub type ShowLogicalSourcesRequest =
    Request<ShowLogicalSources, Result<Vec<LogicalSource>, CoordinatorError>>;
pub type ShowPhysicalSourcesRequest =
    Request<ShowPhysicalSources, Result<Vec<PhysicalSource>, CoordinatorError>>;
pub type ShowSinksRequest = Request<ShowSinks, Result<Vec<Sink>, CoordinatorError>>;
pub type ShowWorkersRequest = Request<ShowWorkers, Result<Vec<Worker>, CoordinatorError>>;
pub type ShowQueriesRequest = Request<ShowQueries, Result<Vec<Query>, CoordinatorError>>;

pub struct Request<Payload, Response> {
    pub payload: Payload,
    pub respond_to: flume::Sender<Response>,
}

impl<Payload, Response> Request<Payload, Response> {
    pub fn respond(self, response: Response) -> Result<(), flume::SendError<Response>> {
        // Will not block, because there are no other senders
        self.respond_to.send(response)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DeltaTag {
    Create,
    Drop,
    Show,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum BaseEntityTag {
    LogicalSource,
    PhysicalSource,
    Sink,
    Worker,
    Query,
}

pub trait RequestHeader<KeyT> {
    const DELTA: DeltaTag;
    const ENTITY: BaseEntityTag;

    type EntityData: differential_dataflow::Data;

    fn to_entity_data(&self) -> Self::EntityData;
    fn key(&self) -> KeyT;
}
