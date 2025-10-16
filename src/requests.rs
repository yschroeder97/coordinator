use crate::data_model::{
    HostName, LogicalSourceName, QueryId, Schema,
    SinkName, SinkType, SourceType,
};
use crate::db_errors::{DatabaseError, ErrorTranslation};
use crate::errors::CoordinatorError;
use std::collections::HashMap;

// Request type aliases
pub type CreateLogicalSourceRequest = Request<CreateLogicalSource, Result<(), CoordinatorError>>;
pub type CreatePhysicalSourceRequest = Request<CreatePhysicalSource, Result<(), CoordinatorError>>;
pub type CreateSinkRequest = Request<CreateSink, Result<(), CoordinatorError>>;
pub type CreateWorkerRequest = Request<CreateWorker, Result<(), CoordinatorError>>;
pub type CreateQueryRequest = Request<CreateQuery, Result<(), CoordinatorError>>;

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
    pub num_slots: u32,
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

impl ErrorTranslation for CreateLogicalSource {
    fn unique_violation(&self, _err: sqlx::Error) -> DatabaseError {
        DatabaseError::LogicalSourceAlreadyExists {
            name: self.source_name.clone(),
        }
    }
}

impl ErrorTranslation for CreatePhysicalSource {}
impl ErrorTranslation for CreateWorker {
    fn unique_violation(&self, _err: sqlx::Error) -> DatabaseError {
        DatabaseError::WorkerAlreadyExists {
            host_name: self.host_name.clone(),
        }
    }
}
impl ErrorTranslation for CreateSink {
    fn unique_violation(&self, _err: sqlx::Error) -> DatabaseError {
        DatabaseError::SinkAlreadyExists {
            name: self.name.clone(),
        }
    }
}
impl ErrorTranslation for CreateQuery {
    fn unique_violation(&self, _err: sqlx::Error) -> DatabaseError {
        DatabaseError::QueryAlreadyExists {
            id: self.id.clone(),
        }
    }

    fn fk_violation(&self, _err: sqlx::Error) -> DatabaseError {
        DatabaseError::SinkNotFoundForQuery {
            sink_name: self.sink.clone(),
            query_id: self.id.clone(),
        }
    }
}
