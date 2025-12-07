use crate::catalog::logical_source::LogicalSourceName;
use crate::catalog::query::QueryId;
use crate::catalog::sink::SinkName;
use crate::catalog::worker::HostName;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CatalogError {
    // Worker-related errors
    #[error("Worker with host name '{host_name}' already exists")]
    WorkerAlreadyExists { host_name: HostName },
    #[error("Invalid worker config: {reason}")]
    InvalidWorkerConfig { reason: String },

    // Logical Source errors
    #[error("Logical source with name '{name}' already exists")]
    LogicalSourceAlreadyExists { name: LogicalSourceName },
    #[error("Logical source is referenced by physical sources")]
    LogicalSourceReferencedByPhysical { name: LogicalSourceName },
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
    SinkNotFoundForQuery {
        sink_name: SinkName,
        query_id: QueryId,
    },

    // Sink errors
    #[error("Sink with name {name} already exists")]
    SinkAlreadyExists { name: SinkName },
    #[error("Cannot create sink: worker '{host_name}' not found")]
    WorkerNotFoundForSink { host_name: HostName },

    #[error("Catalog not-null violation")]
    NotNullViolation {},

    #[error("At least one of the predicates must be `Some`")]
    EmptyPredicate {},

    #[error("Cannot connect to the database")]
    ConnectionError { reason: String },

    #[error("Error during database migration")]
    MigrationError { details: String },

    // Database-specific errors
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Invariant 'invariant' was broken")]
    BrokenInvariant { invariant: String },

    // Catch-all for unexpected catalog errors
    #[error("Unknown error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}
