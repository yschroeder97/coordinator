use std::env;
use crate::db_errors::DatabaseError;
use sqlx::SqlitePool;
use crate::requests::{CreateLogicalSource, CreatePhysicalSource, CreateQuery, CreateSink, CreateWorker};

pub struct Database {
    pool: SqlitePool,
}

impl Database {
    pub fn from_pool(pool: SqlitePool) -> Self {
        Database { pool }
    }

    pub async fn from_env() -> Result<Self, DatabaseError> {
        let database_url = env::var("DATABASE_URL").map_err(|e| {
            DatabaseError::ConnectionError {
                reason: format!("DATABASE_URL not set: {}", e),
            }
        })?;
        let pool = SqlitePool::connect(&database_url).await.map_err(|e| {
            DatabaseError::ConnectionError {
                reason: e.to_string(),
            }
        })?;

        // Run migrations
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| DatabaseError::MigrationError {
                reason: e.to_string(),
            })?;

        Ok(Database { pool })
    }

    pub async fn create_worker(&self, worker: &CreateWorker) -> Result<(), DatabaseError> {
        sqlx::query!(
            "INSERT INTO workers (host_name, grpc_port, data_port, num_slots) VALUES (?, ?, ?, ?)",
            worker.host_name,
            worker.grpc_port,
            worker.data_port,
            worker.num_slots
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DatabaseError::from(e, worker))?;

        Ok(())
    }

    pub async fn create_logical_source(&self, logical: &CreateLogicalSource) -> Result<(), DatabaseError> {
        let schema_json = serde_json::to_value(&logical.schema).map_err(|e| {
            DatabaseError::SerializationError {
                reason: e.to_string(),
            }
        })?;

        sqlx::query!(
            "INSERT INTO logical_sources (name, schema) VALUES (?, ?)",
            logical.source_name,
            schema_json,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DatabaseError::from(e, logical))?;

        Ok(())
    }

    pub async fn create_physical_source(
        &self,
        physical: &CreatePhysicalSource,
    ) -> Result<(), DatabaseError> {
        let source_config_json = serde_json::to_value(&physical.source_config).map_err(|e| {
            DatabaseError::SerializationError {
                reason: e.to_string(),
            }
        })?;
        let parser_config_json = serde_json::to_value(&physical.parser_config).map_err(|e| {
            DatabaseError::SerializationError {
                reason: e.to_string(),
            }
        })?;
        let source_type = physical.source_type.to_string();

        sqlx::query!(
            "INSERT INTO physical_sources (logical_source, placement, source_type, source_config, parser_config) 
             VALUES (?, ?, ?, ?, ?)",
            physical.logical_source,
            physical.placement,
            source_type,
            source_config_json,
            parser_config_json
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DatabaseError::from(e, physical))?;

        Ok(())
    }

    pub async fn create_sink(&self, sink: &CreateSink) -> Result<(), DatabaseError> {
        let config_json =
            serde_json::to_string(&sink.config).map_err(|e| DatabaseError::SerializationError {
                reason: e.to_string(),
            })?;
        let sink_type = sink.sink_type.to_string();

        sqlx::query!(
            "INSERT INTO sinks (name, placement, sink_type, config) VALUES (?, ?, ?, ?)",
            sink.name,
            sink.placement,
            sink_type,
            config_json
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DatabaseError::from(e, sink))?;

        Ok(())
    }

    pub async fn create_query(&self, query: &CreateQuery) -> Result<(), DatabaseError> {

        sqlx::query!(
            "INSERT INTO queries (id, statement, sink) VALUES (?, ?, ?)",
            query.id,
            query.statement,
            query.sink
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DatabaseError::from(e, query))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::data_model::{DataType, Schema, SourceType};
    use super::*;
    use crate::db_errors::DatabaseError;
    use crate::requests::{CreateLogicalSource};

    #[sqlx::test]
    async fn duplicate_logical_source(pool: SqlitePool) -> sqlx::Result<()> {
        let db = Database::from_pool(pool);
        let duplicate = CreateLogicalSource {
            source_name: "src".to_string(),
            schema: Schema::new(vec![("ts".to_string(), DataType::UINT64)]),
        };
        let res = db.create_logical_source(&duplicate).await;
        
        assert!(matches!(res, Err(DatabaseError::LogicalSourceAlreadyExists { name }) if name == "src"));
        Ok(())
    }

    #[sqlx::test]
    async fn physical_without_logical(pool: SqlitePool) -> sqlx::Result<()> {
        let db = Database::from_pool(pool);
        let missing_logical = CreatePhysicalSource {
            logical_source: "unknown".to_string(),
            placement: "worker1".to_string(),
            source_type: SourceType::File,
            source_config: Default::default(),
            parser_config: Default::default(),
        };
        let result = db.create_physical_source(&missing_logical).await;
        assert!(matches!(result, Err(DatabaseError::LogicalSourceNotFoundForPhysical { logical_source_name }) if logical_source_name == "unknown"));

        let missing_placement = CreatePhysicalSource {
            logical_source: "src".to_string(),
            placement: "unknown".to_string(),
            source_type: SourceType::File,
            source_config: Default::default(),
            parser_config: Default::default(),
        };
        let result = db.create_physical_source(&missing_placement).await;
        assert!(matches!(result, Err(DatabaseError::WorkerNotFoundForPhysical { host_name: "unknown" })));
        Ok(())
    }
}
