use crate::catalog::logical_source::DropLogicalSource;
pub use crate::catalog::logical_source::{
    CreateLogicalSource, LogicalSource, GetLogicalSource,
};
use crate::catalog::physical_source::DropPhysicalSource;
pub use crate::catalog::physical_source::{
    CreatePhysicalSource, PhysicalSource, GetPhysicalSource,
};
use crate::catalog::query::DropQuery;
pub use crate::catalog::query::{CreateQuery, GetQuery, Query};
use crate::catalog::sink::DropSink;
pub use crate::catalog::sink::{CreateSink, GetSink, Sink};
use crate::catalog::worker::DropWorker;
pub use crate::catalog::worker::{CreateWorker, GetWorker, Worker};
use crate::errors::CoordinatorError;

// Request type aliases
pub type CreateLogicalSourceRequest = Request<CreateLogicalSource, Result<(), CoordinatorError>>;
pub type CreatePhysicalSourceRequest = Request<CreatePhysicalSource, Result<(), CoordinatorError>>;
pub type CreateSinkRequest = Request<CreateSink, Result<(), CoordinatorError>>;
pub type CreateWorkerRequest = Request<CreateWorker, Result<(), CoordinatorError>>;
pub type CreateQueryRequest = Request<CreateQuery, Result<(), CoordinatorError>>;

pub type GetLogicalSourceRequest =
    Request<GetLogicalSource, Result<Vec<LogicalSource>, CoordinatorError>>;
pub type GetPhysicalSourceRequest =
    Request<GetPhysicalSource, Result<Vec<PhysicalSource>, CoordinatorError>>;
pub type GetSinkRequest = Request<GetSink, Result<Vec<Sink>, CoordinatorError>>;
pub type GetWorkerRequest = Request<GetWorker, Result<Vec<Worker>, CoordinatorError>>;
pub type GetQueryRequest = Request<GetQuery, Result<Vec<Query>, CoordinatorError>>;

pub type DropLogicalSourceRequest = Request<DropLogicalSource, Result<(), CoordinatorError>>;
pub type DropPhysicalSourceRequest = Request<DropPhysicalSource, Result<(), CoordinatorError>>;
pub type DropSinkRequest = Request<DropSink, Result<(), CoordinatorError>>;
pub type DropWorkerRequest = Request<DropWorker, Result<(), CoordinatorError>>;
pub type DropQueryRequest = Request<DropQuery, Result<(), CoordinatorError>>;


pub struct Request<P, R> {
    pub payload: P,
    pub respond_to: flume::Sender<R>,
}

impl<P, R> Request<P, R> {
    pub fn respond(&self, response: R) -> Result<(), flume::SendError<R>> {
        // Will not block, because there are no other senders
        self.respond_to.send(response)
    }
}
