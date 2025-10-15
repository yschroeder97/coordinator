use std::collections::HashMap;
use crate::data_model::{HostName, LogicalSource, LogicalSourceName, PhysicalSource, Query, QueryId, Schema, Sink, SinkName, SinkType, SourceType, Worker};
use crate::db_errors::{RequestContext, RequestType};

// Request type aliases
pub type CreateLogicalSourceRequest = Request<CreateLogicalSource, Result<LogicalSource, String>>;
pub type CreatePhysicalSourceRequest = Request<CreatePhysicalSource, Result<PhysicalSource, String>>;
pub type CreateSinkRequest = Request<CreateSink, Result<Sink, String>>;
pub type CreateWorkerRequest = Request<CreateWorker, Result<Worker, String>>;
pub type CreateQueryRequest = Request<CreateQuery, Result<Query, String>>;

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

pub struct CreateLogicalSource {
    pub source_name: LogicalSourceName,
    pub schema: Schema,
}

pub struct CreatePhysicalSource {
    pub logical_source: LogicalSourceName,
    pub placement: HostName,
    pub source_type: SourceType,
    pub source_config: HashMap<String, String>,
    pub parser_config: HashMap<String, String>,
}

pub struct DropLogicalSource {
    pub name: LogicalSourceName,
}

pub struct DropPhysicalSource {
    pub physical_source_id: i64,
}

pub struct CreateSink {
    pub name: SinkName,
    pub placement: HostName,
    pub sink_type: SinkType,
    pub config: HashMap<String, String>,
}

pub struct DropSink {
    pub name: SinkName,
}

pub struct CreateWorker {
    pub host_name: HostName,
    pub grpc_port: u16,
    pub data_port: u16,
    pub num_slots: Option<u32>,
}

pub struct DropWorker {
    pub host_name: HostName,
}

pub struct CreateQuery {
    pub id: QueryId,
    pub statement: String,
    pub sink: SinkName,
}

pub struct DropQuery {
    pub query_id: String,
}

// RequestContext implementations
impl RequestContext for CreateLogicalSource {
    fn request_type(&self) -> RequestType {
        RequestType::LogicalSource(self.source_name.clone())
    }
}

impl RequestContext for &CreateLogicalSource {
    fn request_type(&self) -> RequestType {
        RequestType::LogicalSource(self.source_name.clone())
    }
}

impl RequestContext for CreatePhysicalSource {
    fn request_type(&self) -> RequestType {
        RequestType::PhysicalSource(self.logical_source.clone())
    }
}

impl RequestContext for &CreatePhysicalSource {
    fn request_type(&self) -> RequestType {
        RequestType::PhysicalSource(self.logical_source.clone())
    }
}

impl RequestContext for CreateSink {
    fn request_type(&self) -> RequestType {
        RequestType::Sink(self.name.clone())
    }
}

impl RequestContext for &CreateSink {
    fn request_type(&self) -> RequestType {
        RequestType::Sink(self.name.clone())
    }
}

impl RequestContext for CreateWorker {
    fn request_type(&self) -> RequestType {
        RequestType::Worker(self.host_name.clone())
    }
}

impl RequestContext for &CreateWorker {
    fn request_type(&self) -> RequestType {
        RequestType::Worker(self.host_name.clone())
    }
}

impl RequestContext for CreateQuery {
    fn request_type(&self) -> RequestType {
        RequestType::Query(self.id.clone())
    }
}

impl RequestContext for &CreateQuery {
    fn request_type(&self) -> RequestType {
        RequestType::Query(self.id.clone())
    }
}
