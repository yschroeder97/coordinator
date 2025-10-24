use crate::data_model::{HostName, LogicalSourceName, QueryId, SinkName};
use sqlx::Error;
use sqlx::error::ErrorKind;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    // Worker-related errors
    #[error("Worker with host name '{host_name}' already exists")]
    WorkerAlreadyExists { host_name: HostName },
    #[error("Invalid worker configuration: {reason}")]
    InvalidWorkerConfig { reason: String },

    // Logical Source errors
    #[error("Logical source with name '{name}' already exists")]
    LogicalSourceAlreadyExists { name: LogicalSourceName },
    #[error("Invalid schema for logical source '{name}': {reason}")]
    InvalidSchema {
        name: LogicalSourceName,
        reason: String,
    },

    // Physical Source errors
    #[error("Cannot create physical source: logical source '{logical_source_name}' not found")]
    LogicalSourceNotFoundForPhysical {
        logical_source_name: LogicalSourceName,
    },
    #[error("Worker '{host_name}' not found for physical source")]
    WorkerNotFoundForPhysical { host_name: HostName },

    // Query errors
    #[error("Query with id '{id}' already exists")]
    QueryAlreadyExists { id: QueryId },
    #[error("Sink with name {sink_name} not found for query {query_id} already exists")]
    SinkNotFoundForQuery { sink_name: SinkName, query_id: QueryId },

    // Sink errors
    #[error("Sink with name {name} already exists")]
    SinkAlreadyExists { name: SinkName },
    #[error("Cannot create sink: worker '{host_name}' not found")]
    WorkerNotFoundForSink { host_name: HostName },

    // General database errors
    #[error("Database connection failed: {reason}")]
    ConnectionError { reason: String },

    #[error("Database migration failed: {reason}")]
    MigrationError { reason: String },

    #[error("Serialization error: {reason}")]
    SerializationError { reason: String },

    #[error("Database foreign key violation: {reason}")]
    ForeignKeyViolation { reason: String},
    
    #[error("Database constraint violation: {constraint}")]
    ConstraintViolation { constraint: String },

    #[error("Database not-null violation")]
    NotNullViolation {},

    // Catch-all for unexpected database errors
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl DatabaseError {
    /// Create a DatabaseError from an SQLx error with type-safe request tags.
    /// This is the main entry point for error translation.
    pub fn from<RequestT: ErrorTranslation>(err: sqlx::Error, request: &RequestT) -> Self {
        match &err {
            Error::Database(db_err) => match db_err.kind() {
                ErrorKind::UniqueViolation => RequestT::unique_violation(request, err),
                ErrorKind::ForeignKeyViolation => RequestT::fk_violation(request, err),
                ErrorKind::NotNullViolation => RequestT::not_null_violation(request, err),
                ErrorKind::CheckViolation => RequestT::check_violation(request, err),
                _ => Self::Database(err),
            },
            _ => Self::Database(err),
        }
    }
}

pub trait ErrorTranslation {
    fn unique_violation(&self, err: sqlx::Error) -> DatabaseError {
        DatabaseError::Database(err)
    }
    fn fk_violation(&self, err: sqlx::Error) -> DatabaseError {
        DatabaseError::Database(err)
    }
    fn not_null_violation(&self, err: sqlx::Error) -> DatabaseError {
        DatabaseError::Database(err)
    }
    fn check_violation(&self, err: sqlx::Error) -> DatabaseError {
       DatabaseError::Database(err)
    }
}
