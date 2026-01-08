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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::test_utils::{test_prop, CreateSinkWithRefs};
    use crate::catalog::worker::CreateWorker;
    use quickcheck_macros::quickcheck;

    #[quickcheck]
    fn sink_name_is_unique(req: CreateSinkWithRefs) -> bool {
        test_prop(|catalog| async move {
            catalog
                .worker
                .create_worker(&req.create_worker)
                .await
                .expect("Worker setup should succeed");

            catalog
                .sink
                .create_sink(&req.create_sink)
                .await
                .expect("First sink creation should succeed");

            assert!(
                catalog.sink.create_sink(&req.create_sink).await.is_err(),
                "Duplicate sink name '{}' should be rejected",
                req.create_sink.name
            );
        })
    }

    #[quickcheck]
    fn sink_worker_exists(create_worker: CreateWorker, mut create_sink: CreateSink) -> bool {
        test_prop(|catalog| async move {
            assert!(
                catalog.sink.create_sink(&create_sink).await.is_err(),
                "CreateSink without prior worker creation should be rejected"
            );

            create_sink.placement_host_name = create_worker.host_name.clone();
            create_sink.placement_grpc_port = create_worker.grpc_port;
            catalog
                .worker
                .create_worker(&create_worker)
                .await
                .expect("Worker setup should succeed");
            catalog
                .sink
                .create_sink(&create_sink)
                .await
                .expect("CreateSink with valid worker ref should succeed");
        })
    }
}
