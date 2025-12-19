use crate::catalog::database::{Database, DatabaseErr};
use crate::catalog::query_builder::ToSql;
use crate::catalog::source::logical_source::{CreateLogicalSource, DropLogicalSource, LogicalSource, LogicalSourceName};
use crate::catalog::source::physical_source::{CreatePhysicalSource, DropPhysicalSource, PhysicalSource};
use crate::catalog::worker::worker_endpoint::HostName;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SourceCatalogErr {
    #[error("Logical source with name '{name}' already exists")]
    LogicalSourceAlreadyExists { name: LogicalSourceName },

    #[error("Logical source '{name}' not found")]
    LogicalSourceNotFound { name: LogicalSourceName },

    #[error("Logical source is referenced by physical sources")]
    LogicalSourceReferencedByPhysical { name: LogicalSourceName },

    #[error("Invalid schema for logical source '{name}': {reason}")]
    InvalidSchema {
        name: LogicalSourceName,
        reason: String,
    },

    #[error("Worker '{host_name}' not found for physical source")]
    WorkerNotFoundForPhysical { host_name: HostName },

    #[error("Database error: {0}")]
    Database(#[from] DatabaseErr),

    #[error("At least one of the predicates must be `Some`")]
    EmptyPredicate {},
}

pub struct SourceCatalog {
    db: Arc<Database>,
}

impl SourceCatalog {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    pub async fn create_logical_source(
        &self,
        logical: &CreateLogicalSource,
    ) -> Result<(), SourceCatalogErr> {
        let schema = sqlx::types::Json(&logical.schema);
        let query = sqlx::query!(
            "INSERT INTO logical_sources (name, schema) VALUES (?, ?)",
            logical.source_name,
            schema as _,
        );

        self.db.execute(query).await?;
        Ok(())
    }

    pub async fn create_physical_source(
        &self,
        physical: &CreatePhysicalSource,
    ) -> Result<(), SourceCatalogErr> {
        let source_config = sqlx::types::Json(&physical.source_config);
        let parser_config = sqlx::types::Json(&physical.parser_config);

        let query = sqlx::query!(
            "INSERT INTO physical_sources (logical_source, placement_host_name, placement_grpc_port, source_type, source_config, parser_config)
             VALUES (?, ?, ?, ?, ?, ?)",
            physical.logical_source,
            physical.placement_host_name,
            physical.placement_grpc_port,
            physical.source_type,
            source_config as _,
            parser_config as _
        );

        self.db.execute(query).await?;
        Ok(())
    }

    pub async fn drop_logical_source(
        &self,
        drop_logical: &DropLogicalSource,
    ) -> Result<LogicalSource, SourceCatalogErr> {
        let (stmt, args) = drop_logical.to_sql();
        self.db.delete_one(&stmt, args).await.map_err(|e| e.into())
    }

    pub async fn drop_physical_source(
        &self,
        drop_physical: &DropPhysicalSource,
    ) -> Result<Vec<PhysicalSource>, SourceCatalogErr> {
        let (stmt, args) = drop_physical.to_sql();
        self.db.delete_many(&stmt, args).await.map_err(|e| e.into())
    }
}
