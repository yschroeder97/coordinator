use crate::db_errors::DatabaseError;
use sqlx::{Executor, Row, SqlitePool};
use std::env;
use crate::data_model::logical_source::LogicalSource;
use crate::data_model::physical_source::{CreatePhysicalSource, PhysicalSource, ShowPhysicalSources};
use crate::data_model::query::{CreateQuery, Query, QueryState};
use crate::data_model::sink::{CreateSink, Sink, SinkType};
use crate::data_model::worker::{CreateWorker, Worker};
use crate::requests::{CreateLogicalSource, ShowLogicalSources};

#[derive(Clone)]
pub struct Catalog {
    pool: SqlitePool,
}

impl Catalog {
    pub fn from_pool(pool: SqlitePool) -> Self {
        Catalog { pool }
    }

    pub async fn from_env() -> Result<Self, DatabaseError> {
        let database_url =
            env::var("DATABASE_URL").map_err(|e| DatabaseError::ConnectionError {
                reason: format!("DATABASE_URL not set: {}", e),
            })?;
        let pool = SqlitePool::connect(&database_url).await.map_err(|e| {
            DatabaseError::ConnectionError {
                reason: e.to_string(),
            }
        })?;

        sqlx::migrate!()
            .run(&pool)
            .await
            .map_err(|e| DatabaseError::MigrationError {
                reason: e.to_string(),
            })?;

        Ok(Catalog { pool })
    }

    pub async fn insert_worker(&self, worker: &CreateWorker) -> Result<(), DatabaseError> {
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

    pub async fn insert_logical_source(
        &self,
        logical: &CreateLogicalSource,
    ) -> Result<(), DatabaseError> {
        let schema = sqlx::types::Json(&logical.schema);
        sqlx::query!(
            "INSERT INTO logical_sources (name, schema) VALUES (?, ?)",
            logical.source_name,
            schema as _,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DatabaseError::from(e, logical))?;

        Ok(())
    }

    pub async fn insert_physical_source(
        &self,
        physical: &CreatePhysicalSource,
    ) -> Result<(), DatabaseError> {
        let source_config = sqlx::types::Json(&physical.source_config);
        let parser_config = sqlx::types::Json(&physical.parser_config);
        sqlx::query!(
            "INSERT INTO physical_sources (logical_source, placement, source_type, source_config, parser_config)
             VALUES (?, ?, ?, ?, ?)",
            physical.logical_source,
            physical.placement,
            physical.source_type,
            source_config as _,
            parser_config as _
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DatabaseError::from(e, physical))?;

        Ok(())
    }

    pub async fn insert_sink(&self, sink: &CreateSink) -> Result<(), DatabaseError> {
        let config = sqlx::types::Json(&sink.config);
        sqlx::query!(
            "INSERT INTO sinks (name, placement, sink_type, config) VALUES (?, ?, ?, ?)",
            sink.name,
            sink.placement,
            sink.sink_type,
            config as _
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DatabaseError::from(e, sink))?;

        Ok(())
    }

    pub async fn insert_query(&self, query: &CreateQuery) -> Result<(), DatabaseError> {
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

    // Query functions
    pub async fn show_logical_sources(
        &self,
        show_logical: &ShowLogicalSources,
    ) -> Result<Vec<LogicalSource>, DatabaseError> {
        if let Some(name) = &show_logical.source_name {
            sqlx::query_as::<_, LogicalSource>(
                "SELECT name, schema FROM logical_sources WHERE name = ?"
            )
            .bind(name)
            .fetch_all(&self.pool)
            .await
            .map_err(DatabaseError::Database)
        } else {
            sqlx::query_as::<_, LogicalSource>(
                "SELECT name, schema FROM logical_sources"
            )
            .fetch_all(&self.pool)
            .await
            .map_err(DatabaseError::Database)
        }
    }

    pub async fn show_physical_sources(
        &self,
        show_physical: &ShowPhysicalSources,
    ) -> Result<Vec<PhysicalSource>, DatabaseError> {
        use sqlx::QueryBuilder;

        let mut query = QueryBuilder::new(
            "SELECT id, logical_source, placement, source_type, source_config, parser_config FROM physical_sources"
        );

        let mut has_where = false;

        if let Some(logical) = &show_physical.for_logical_source {
            query.push(" WHERE logical_source = ");
            query.push_bind(logical);
            has_where = true;
        }

        if let Some(node) = &show_physical.on_node {
            query.push(if has_where { " AND" } else { " WHERE" });
            query.push(" placement = ");
            query.push_bind(node);
            has_where = true;
        }

        if let Some(source_type) = &show_physical.by_type {
            query.push(if has_where { " AND" } else { " WHERE" });
            query.push(" source_type = ");
            query.push_bind(source_type);
        }

        query
            .build_query_as::<PhysicalSource>()
            .fetch_all(&self.pool)
            .await
            .map_err(DatabaseError::Database)
    }

    pub async fn get_all_sinks(&self) -> Result<Vec<Sink>, DatabaseError> {
        let rows = sqlx::query!("SELECT name, placement, sink_type, config FROM sinks")
            .fetch_all(&self.pool)
            .await
            .map_err(DatabaseError::Database)?;

        rows.into_iter()
            .map(|row| {
                let sink_type = match row.sink_type.as_str() {
                    "File" => SinkType::File,
                    "Print" => SinkType::Print,
                    _ => {
                        return Err(DatabaseError::SerializationError {
                            reason: format!("Unknown sink type: {}", row.sink_type),
                        });
                    }
                };
                let config = serde_json::from_str(&row.config).map_err(|e| {
                    DatabaseError::SerializationError {
                        reason: e.to_string(),
                    }
                })?;

                Ok(Sink {
                    name: row.name.expect("name is primary key and cannot be null"),
                    placement: row.placement,
                    sink_type,
                    config,
                })
            })
            .collect()
    }

    pub async fn get_all_workers(&self) -> Result<Vec<Worker>, DatabaseError> {
        let rows = sqlx::query!("SELECT host_name, grpc_port, data_port, num_slots FROM workers")
            .fetch_all(&self.pool)
            .await
            .map_err(DatabaseError::Database)?;

        Ok(rows
            .into_iter()
            .map(|row| Worker {
                host_name: row
                    .host_name
                    .expect("host_name is primary key and cannot be null"),
                grpc_port: row.grpc_port as u16,
                data_port: row.data_port as u16,
                num_slots: row.num_slots as u32,
            })
            .collect())
    }

    pub async fn get_all_queries(&self) -> Result<Vec<Query>, DatabaseError> {
        let rows = sqlx::query!("SELECT id, statement, state, sink FROM queries")
            .fetch_all(&self.pool)
            .await
            .map_err(DatabaseError::Database)?;

        rows.into_iter()
            .map(|row| {
                let state = match row.state.as_str() {
                    "Pending" => QueryState::Pending,
                    "Running" => QueryState::Running,
                    "Completed" => QueryState::Completed,
                    "Failed" => QueryState::Failed,
                    _ => {
                        return Err(DatabaseError::SerializationError {
                            reason: format!("Unknown query state: {}", row.state),
                        });
                    }
                };

                Ok(Query {
                    id: row.id.expect("id is primary key and cannot be null"),
                    statement: row.statement,
                    state,
                    sink: row.sink,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod db_tests {
    use crate::data_model::physical_source::{CreatePhysicalSource, SourceType};
    use crate::data_model::schema::{DataType, Schema};
    use super::*;
    use crate::db_errors::DatabaseError;
    use crate::requests::CreateLogicalSource;

    #[sqlx::test]
    async fn duplicate_logical_source(pool: SqlitePool) -> sqlx::Result<()> {
        let db = Catalog::from_pool(pool);
        let duplicate = CreateLogicalSource {
            source_name: "src".to_string(),
            schema: Schema::with(vec![("ts".to_string(), DataType::UINT64)]),
        };
        let result = db.insert_logical_source(&duplicate).await;

        assert!(
            matches!(result, Err(DatabaseError::LogicalSourceAlreadyExists { name }) if name == "src")
        );
        Ok(())
    }

    #[sqlx::test]
    async fn physical_without_logical_and_worker(pool: SqlitePool) -> sqlx::Result<()> {
        let db = Catalog::from_pool(pool);
        let missing_logical = CreatePhysicalSource {
            logical_source: "unknown".to_string(),
            placement: "worker1".to_string(),
            source_type: SourceType::File,
            source_config: Default::default(),
            parser_config: Default::default(),
        };
        let result = db.insert_physical_source(&missing_logical).await;
        assert!(matches!(result, Err(DatabaseError::Database(_))));

        let missing_placement = CreatePhysicalSource {
            logical_source: "src".to_string(),
            placement: "unknown".to_string(),
            source_type: SourceType::File,
            source_config: Default::default(),
            parser_config: Default::default(),
        };
        let result = db.insert_physical_source(&missing_placement).await;
        assert!(matches!(result, Err(DatabaseError::Database(_))));
        Ok(())
    }
}
