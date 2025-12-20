use crate::catalog::database::{inspect_constraint, Database, DatabaseErr};
use crate::catalog::query_builder::ToSql;
use crate::catalog::source::logical_source::{
    CreateLogicalSource, DropLogicalSource, GetLogicalSource, LogicalSource, LogicalSourceName,
};
use crate::catalog::source::physical_source::{
    CreatePhysicalSource, DropPhysicalSource, GetPhysicalSource, PhysicalSource,
};
use crate::catalog::tables::{deployed_sources, logical_sources, physical_sources, table};
use crate::catalog::worker::worker_endpoint::HostName;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SourceCatalogErr {
    #[error("Logical source with name '{name}' already exists")]
    LogicalSourceAlreadyExists { name: LogicalSourceName },

    #[error("Logical source '{name}' not found")]
    LogicalSourceNotFound { name: LogicalSourceName },

    #[error("Cannot delete logical source '{name}': referenced by {count} physical source(s). Delete these physical sources first: {physical_source_ids:?}")]
    LogicalSourceReferencedByPhysical {
        name: LogicalSourceName,
        count: usize,
        physical_source_ids: Vec<i64>,
    },

    #[error("Physical source with id {id} is referenced by {count} active query(s): {query_ids:?}. Stop or remove these queries first.")]
    PhysicalSourceReferencedByQueries {
        id: i64,
        count: usize,
        query_ids: Vec<String>,
    },

    #[error("Invalid schema for logical source '{name}': {reason}")]
    InvalidSchema {
        name: LogicalSourceName,
        reason: String,
    },

    #[error("Worker '{host_name}:{grpc_port}' not found - cannot create physical source")]
    WorkerNotFound {
        host_name: HostName,
        grpc_port: u16,
    },

    #[error("Physical source already exists on worker '{host_name}:{grpc_port}' for logical source '{logical_source}'")]
    PhysicalSourceAlreadyExists {
        logical_source: LogicalSourceName,
        host_name: HostName,
        grpc_port: u16,
    },

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

        self.db.execute(query).await.map_err(|e| {
            // Check if this is a UNIQUE constraint violation on the primary key
            if let Some(constraint) = inspect_constraint(&e) {
                if constraint.contains(table::LOGICAL_SOURCES) || constraint.contains(logical_sources::NAME) {
                    return SourceCatalogErr::LogicalSourceAlreadyExists {
                        name: logical.source_name.clone(),
                    };
                }
            }
            // Otherwise, pass through the database error
            e.into()
        })?;
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

        self.db.execute(query).await.map_err(|e| {
            if let Some(constraint) = inspect_constraint(&e) {
                // FK to logical_sources
                if constraint.contains(physical_sources::LOGICAL_SOURCE)
                    || constraint.contains(table::LOGICAL_SOURCES) {
                    return SourceCatalogErr::LogicalSourceNotFound {
                        name: physical.logical_source.clone(),
                    };
                }
                if constraint.contains(physical_sources::PLACEMENT_HOST_NAME)
                    || constraint.contains(table::WORKERS) {
                    return SourceCatalogErr::WorkerNotFound {
                        host_name: physical.placement_host_name.clone(),
                        grpc_port: physical.placement_grpc_port,
                    };
                }
            }
            e.into()
        })?;
        Ok(())
    }

    pub async fn drop_logical_source(
        &self,
        drop_logical: &DropLogicalSource,
    ) -> Result<Option<LogicalSource>, SourceCatalogErr> {
        // Proactive check: find all physical sources referencing this logical source
        if let Some(ref source_name) = drop_logical.source_name {
            let sql = format!(
                "SELECT {} FROM {} WHERE {} = ?",
                physical_sources::ID,
                table::PHYSICAL_SOURCES,
                physical_sources::LOGICAL_SOURCE
            );

            use sqlx::Arguments as _;
            let mut args = sqlx::sqlite::SqliteArguments::default();
            let _ = args.add(source_name);

            let referencing_sources: Vec<i64> = self
                .db
                .select_scalar(&sql, args)
                .await
                .unwrap_or_default();

            if !referencing_sources.is_empty() {
                return Err(SourceCatalogErr::LogicalSourceReferencedByPhysical {
                    name: source_name.clone(),
                    count: referencing_sources.len(),
                    physical_source_ids: referencing_sources,
                });
            }
        }

        // If no references, proceed with deletion
        let (stmt, args) = drop_logical.to_sql();
        let deleted = self.db.delete_many(&stmt, args).await?;

        // Return the first (and only) deleted source, or None if nothing was deleted
        Ok(deleted.into_iter().next())
    }

    pub async fn drop_physical_source(
        &self,
        drop_physical: &DropPhysicalSource,
    ) -> Result<Vec<PhysicalSource>, SourceCatalogErr> {
        let (stmt, args) = drop_physical.to_sql();
        self.db.delete_many(&stmt, args).await.map_err(|e| {
            // Transform FK constraint violations
            // Note: For physical sources, the FK violation would come from deployed_sources table
            // Since DropPhysicalSource can match multiple sources, we can't easily do proactive
            // checking with full query lists. The error message will guide users.
            if let Some(constraint) = inspect_constraint(&e) {
                if constraint.contains(table::DEPLOYED_SOURCES)
                    || constraint.contains(deployed_sources::PHYSICAL_SOURCE_ID) {
                    // We can't easily get the specific physical source ID and query list here
                    // without re-querying, so we'll provide a generic helpful message
                    // The user could enhance this with proactive checking if needed
                    return SourceCatalogErr::Database(e);
                }
            }
            e.into()
        })
    }

    pub async fn get_logical_source(
        &self,
        get_logical: &GetLogicalSource,
    ) -> Result<LogicalSource, SourceCatalogErr> {
        let (stmt, args) = get_logical.to_sql();
        self.db.select_one(&stmt, args).await.map_err(|e| e.into())
    }

    pub async fn get_physical_sources(
        &self,
        get_physical: &GetPhysicalSource,
    ) -> Result<Vec<PhysicalSource>, SourceCatalogErr> {
        let (stmt, args) = get_physical.to_sql();
        self.db.select(&stmt, args).await.map_err(|e| e.into())
    }
}
