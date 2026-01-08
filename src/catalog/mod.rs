use crate::catalog::catalog_errors::CatalogErr;
use crate::catalog::database::Database;
use crate::catalog::query::query_catalog::QueryCatalog;
use crate::catalog::sink::sink_catalog::SinkCatalog;
use crate::catalog::source::source_catalog::SourceCatalog;
use crate::catalog::worker::worker_catalog::WorkerCatalog;
use sqlx::SqlitePool;
use std::env;
use std::sync::Arc;
use crate::catalog::query::{FragmentState, QueryState};
use crate::catalog::tables::{query_fragment_states, query_states, table, worker_states};
use crate::catalog::worker::WorkerState;

pub mod catalog_errors;
mod database;
pub mod notification;
pub mod query;
pub mod query_builder;
pub mod sink;
pub mod source;
pub mod tables;
pub mod worker;
#[cfg(test)]
pub mod test_utils;

pub struct Catalog {
    pub source: Arc<SourceCatalog>,
    pub sink: Arc<SinkCatalog>,
    pub worker: Arc<WorkerCatalog>,
    pub query: Arc<QueryCatalog>,
    pool: SqlitePool,
}

impl Catalog {
    fn from_db(db: Arc<Database>) -> Self {
        Self {
            source: Arc::new(SourceCatalog::new(db.clone())),
            sink: Arc::new(SinkCatalog::new(db.clone())),
            worker: Arc::new(WorkerCatalog::new(db.clone())),
            query: Arc::new(QueryCatalog::new(db.clone())),
            pool: db.pool(),
        }
    }

    pub async fn from_env() -> Result<Self, CatalogErr> {
        let database_url = env::var("DATABASE_URL").map_err(|e| CatalogErr::ConnectionError {
            reason: format!("DATABASE_URL not set: {}", e),
        })?;

        let pool = sqlx::SqlitePool::connect(&database_url)
            .await
            .map_err(|e| CatalogErr::ConnectionError {
                reason: e.to_string(),
            })?;

        let db = Database::from_pool(pool)
            .await
            .map_err(|e| CatalogErr::MigrationError {
                details: e.to_string(),
            })?;

        Ok(Self::from_db(db))
    }

    pub async fn from_pool(pool: sqlx::SqlitePool) -> Result<Self, CatalogErr> {
        let db = Database::from_pool(pool)
            .await
            .map_err(|e| CatalogErr::MigrationError {
                details: e.to_string(),
            })?;

        Ok(Self::from_db(db))
    }

    pub fn source_catalog(&self) -> Arc<SourceCatalog> {
        self.source.clone()
    }

    pub fn worker_catalog(&self) -> Arc<WorkerCatalog> {
        self.worker.clone()
    }

    pub fn query_catalog(&self) -> Arc<QueryCatalog> {
        self.query.clone()
    }

    pub fn sink_catalog(&self) -> Arc<SinkCatalog> {
        self.sink.clone()
    }

    pub fn pool(&self) -> SqlitePool {
        self.pool.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::query::{FragmentState, QueryState};
    use crate::catalog::tables::{query_fragment_states, query_states, table, worker_states};
    use crate::catalog::worker::WorkerState;
    use sqlx::{Sqlite, SqlitePool};
    use strum::IntoEnumIterator;

    async fn cmp_tbl_enum<T>(pool: &SqlitePool, tbl_name: &'static str, col_name: &'static str)
    where
        T: Send + Unpin + IntoEnumIterator + PartialEq + std::fmt::Debug,
        T: for<'r> sqlx::Decode<'r, Sqlite> + sqlx::Type<Sqlite>,
    {
        let stmt = format!("SELECT {} FROM {}", col_name, tbl_name);

        let actual: Vec<T> = sqlx::query_scalar(stmt.as_str())
            .fetch_all(pool)
            .await
            .expect("Failed to fetch enum values from DB");

        let expected: Vec<T> = T::iter().collect();

        // Note: This relies on the DB order matching the Enum definition order.
        assert_eq!(
            actual, expected,
            "Internal table {} out-of-sync with enum",
            tbl_name
        );
    }

    #[sqlx::test]
    async fn static_tables_in_sync(pool: SqlitePool) {
        let catalog = Catalog::from_pool(pool).await.unwrap();
        cmp_tbl_enum::<WorkerState>(&catalog.pool, table::WORKER_STATES, worker_states::STATE)
            .await;
        cmp_tbl_enum::<QueryState>(&catalog.pool, table::QUERY_STATES, query_states::STATE).await;
        cmp_tbl_enum::<FragmentState>(
            &catalog.pool,
            table::QUERY_FRAGMENT_STATES,
            query_fragment_states::STATE,
        )
        .await;
    }
}
