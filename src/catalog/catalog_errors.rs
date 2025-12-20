use crate::catalog::database::DatabaseErr;
use crate::catalog::query::query_catalog::QueryCatalogError;
use crate::catalog::sink::sink_catalog::SinkCatalogError;
use crate::catalog::source::source_catalog::SourceCatalogErr;
use crate::catalog::worker::worker_catalog::WorkerCatalogErr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CatalogErr {
    #[error("Source catalog error: {0}")]
    Source(#[from] SourceCatalogErr),

    #[error("Worker catalog error: {0}")]
    Worker(#[from] WorkerCatalogErr),

    #[error("Query catalog error: {0}")]
    Query(#[from] QueryCatalogError),

    #[error("Sink catalog error: {0}")]
    Sink(#[from] SinkCatalogError),

    #[error("Database error: {0}")]
    Database(#[from] DatabaseErr),

    #[error("Legacy database error: {0}")]
    LegacyDatabase(#[from] sqlx::Error),

    #[error("Catalog not-null violation")]
    NotNullViolation {},

    #[error("At least one of the predicates must be `Some`")]
    EmptyPredicate {},

    #[error("Cannot connect to the database")]
    ConnectionError { reason: String },

    #[error("Error during database migration")]
    MigrationError { details: String },

    #[error("Invariant '{invariant}' was broken")]
    BrokenInvariant { invariant: String },

    // Catch-all for unexpected catalog errors
    #[error("Unknown error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}
