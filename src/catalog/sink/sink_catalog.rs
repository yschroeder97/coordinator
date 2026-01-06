use crate::catalog::database::{Database, DatabaseErr};
use crate::catalog::query_builder::ToSql;
use crate::catalog::sink::{CreateSink, DropSink, GetSink, Sink, SinkName};
use crate::catalog::worker::endpoint::HostName;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SinkCatalogError {
    #[error("Sink with name '{name}' already exists")]
    SinkAlreadyExists { name: SinkName },

    #[error("Sink with name '{name}' not found")]
    SinkNotFound { name: SinkName },

    #[error("Worker '{host_name}' not found for sink")]
    WorkerNotFoundForSink { host_name: HostName },

    #[error("Invalid sink configuration: {reason}")]
    InvalidSinkConfig { reason: String },

    #[error("Database error: {0}")]
    Database(#[from] DatabaseErr),

    #[error("At least one of the predicates must be `Some`")]
    EmptyPredicate {},
}

pub struct SinkCatalog {
    db: Arc<Database>,
}

impl SinkCatalog {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    pub async fn create_sink(&self, sink: &CreateSink) -> Result<(), SinkCatalogError> {
        let config = sqlx::types::Json(&sink.config);
        let query = sqlx::query!(
            "INSERT INTO sinks (name, placement_host_name, placement_grpc_port, sink_type, config) VALUES (?, ?, ?, ?, ?)",
            sink.name,
            sink.placement_host_name,
            sink.placement_grpc_port,
            sink.sink_type,
            config as _
        );

        self.db.execute(query).await?;
        Ok(())
    }

    pub async fn drop_sink(&self, drop_sink: &DropSink) -> Result<Vec<Sink>, SinkCatalogError> {
        let (stmt, args) = drop_sink.to_sql();
        self.db.delete_many(&stmt, args).await.map_err(Into::into)
    }

    pub async fn get_sinks(&self, get_sinks: &GetSink) -> Result<Vec<Sink>, SinkCatalogError> {
        let (stmt, args) = get_sinks.to_sql();
        self.db.select(&stmt, args).await.map_err(Into::into)
    }
}
