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
    use crate::catalog::test_utils::{arb_create_sink, test_prop, SinkWithRefs};
    use crate::catalog::Catalog;
    use proptest::prelude::*;

    async fn prop_sink_name_unique(catalog: Catalog, create_sink: SinkWithRefs) {
        catalog
            .worker
            .create_worker(&create_sink.worker)
            .await
            .expect("Worker creation should succeed");

        catalog
            .sink
            .create_sink(&create_sink.sink)
            .await
            .expect("First sink creation should succeed");

        assert!(
            catalog.sink.create_sink(&create_sink.sink).await.is_err(),
            "Duplicate sink name '{}' should be rejected",
            create_sink.sink.name
        );
    }

    async fn prop_sink_worker_exists(catalog: Catalog, create_sink: SinkWithRefs) {
        assert!(
            catalog.sink.create_sink(&create_sink.sink).await.is_err(),
            "CreateSink without prior worker creation should be rejected"
        );

        catalog
            .worker
            .create_worker(&create_sink.worker)
            .await
            .expect("Worker setup should succeed");
        catalog
            .sink
            .create_sink(&create_sink.sink)
            .await
            .expect("Sink creation with valid worker ref should succeed");
    }

    proptest! {
        #[test]
        fn sink_name_unique(req in arb_create_sink()) {
            test_prop(|catalog| async move {
                prop_sink_name_unique(catalog, req).await;
            });
        }

        #[test]
        fn sink_worker_exists(req in arb_create_sink()) {
            test_prop(|catalog| async move {
                prop_sink_worker_exists(catalog, req).await;
            })
        }
    }
}
