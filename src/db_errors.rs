use thiserror::Error;
use crate::data_model::{HostName, LogicalSource, LogicalSourceName, PhysicalSourceId, QueryId, SinkName};

#[derive(Debug, Clone)]
pub enum RequestType {
    Worker(HostName),
    LogicalSource(LogicalSourceName),
    PhysicalSource(LogicalSourceName), // Pass the logical source name for better error context
    Sink(SinkName),
    Query(QueryId),
}

pub trait RequestContext {
    fn request_type(&self) -> RequestType;
}

struct SqliteErrorCodes;
impl SqliteErrorCodes {
    pub const CONSTRAINT_UNIQUE: &'static [&'static str] = &["2067", "1555"];
    pub const CONSTRAINT_FOREIGNKEY: &'static [&'static str] = &["787"];
    pub const CONSTRAINT_NOTNULL: &'static [&'static str] = &["1299"];
    pub const CONSTRAINT_CHECK: &'static [&'static str] = &["275"];
}

#[derive(Error, Debug)]
pub enum DatabaseError {
    // Worker-related errors
    #[error("Worker with host name '{host_name}' already exists")]
    WorkerAlreadyExists { host_name: HostName },
    #[error("Worker with host name '{host_name}' not found")]
    WorkerNotFound { host_name: HostName },
    #[error("Invalid worker configuration: {reason}")]
    InvalidWorkerConfig { reason: String },

    // Logical Source errors
    #[error("Logical source with name '{name}' already exists")]
    LogicalSourceAlreadyExists { name: LogicalSourceName },
    #[error("Logical source with name '{name}' not found")]
    LogicalSourceNotFound { name: LogicalSourceName },
    #[error("Invalid schema for logical source '{name}': {reason}")]
    InvalidSchema { name: LogicalSourceName, reason: String },

    // Physical Source errors
    #[error("Physical source with id {id} not found")]
    PhysicalSourceNotFound { id: PhysicalSourceId },
    #[error("Cannot create physical source: logical source '{logical_source_name}' not found")]
    LogicalSourceNotFoundForPhysical { logical_source_name: LogicalSourceName },
    #[error("Cannot create physical source: worker '{host_name}' not found")]
    WorkerNotFoundForPhysical { host_name: HostName },

    // Query errors
    #[error("Query with id '{id}' already exists")]
    QueryAlreadyExists { id: QueryId },

    #[error("Query with id '{id}' not found")]
    QueryNotFound { id: QueryId },

    // Sink errors
    #[error("Sink with name {name} not found")]
    SinkNotFound { name: SinkName },
    #[error("Sink with name {name} already exists")]
    SinkAlreadyExists { name: SinkName },

    #[error("Cannot create sink: worker '{host_name}' not found")]
    WorkerNotFoundForSink { host_name: HostName },

    // Fragment errors
    #[error("Query fragment not found for query '{query_id}' on worker '{host_name}'")]
    QueryFragmentNotFound { query_id: QueryId, host_name: HostName },

    // General database errors
    #[error("Database connection failed: {reason}")]
    ConnectionError { reason: String },

    #[error("Database migration failed: {reason}")]
    MigrationError { reason: String },

    #[error("Serialization error: {reason}")]
    SerializationError { reason: String },

    #[error("Database constraint violation: {constraint}")]
    ConstraintViolation { constraint: String },

    // Catch-all for unexpected database errors
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl DatabaseError {
    /// Create a DatabaseError from an SQLx error with request context
    pub fn from(err: sqlx::Error, ctx: impl RequestContext) -> Self {
        Self::from_request_type(err, ctx.request_type())
    }
    
    /// Create a DatabaseError from an SQLx error with request type
    fn from_request_type(err: sqlx::Error, request_type: RequestType) -> Self {
        match &err {
            sqlx::Error::Database(db_err) => {
                let code = db_err.code().map(|c| c.as_ref().to_string()).unwrap_or_default();
                
                if SqliteErrorCodes::CONSTRAINT_UNIQUE.contains(&code.as_str()) {
                    match request_type {
                        RequestType::LogicalSource(name) => Self::LogicalSourceAlreadyExists { name },
                        RequestType::Sink(name) => Self::SinkAlreadyExists { name },
                        RequestType::Worker(host_name) => Self::WorkerAlreadyExists { host_name },
                        RequestType::Query(id) => Self::QueryAlreadyExists { id },
                        _ => Self::ConstraintViolation { constraint: db_err.message().to_string() },
                    }
                } else if SqliteErrorCodes::CONSTRAINT_FOREIGNKEY.contains(&code.as_str()) {
                    match request_type {
                        RequestType::PhysicalSource(logical_source_name) => {
                            Self::LogicalSourceNotFoundForPhysical { logical_source_name }
                        }
                        _ => Self::ConstraintViolation { constraint: db_err.message().to_string() }
                    }
                } else if SqliteErrorCodes::CONSTRAINT_NOTNULL.contains(&code.as_str()) {
                    Self::ConstraintViolation { constraint: format!("NOT NULL constraint failed: {}", db_err.message()) }
                } else if SqliteErrorCodes::CONSTRAINT_CHECK.contains(&code.as_str()) {
                    Self::ConstraintViolation { constraint: format!("CHECK constraint failed: {}", db_err.message()) }
                } else {
                    Self::ConstraintViolation { constraint: db_err.message().to_string() }
                }
            }
            sqlx::Error::RowNotFound => match request_type {
                RequestType::Worker(host_name) => Self::WorkerNotFound { host_name },
                RequestType::LogicalSource(name) => Self::LogicalSourceNotFound { name },
                RequestType::Query(id) => Self::QueryNotFound { id },
                RequestType::Sink(name) => Self::SinkNotFound { name },
                _ => Self::ConstraintViolation { constraint: "Resource not found".to_string() },
            },
            _ => Self::Database(err),
        }
    }
}
