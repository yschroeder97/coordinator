use sqlx::query::Query;
use sqlx::sqlite::{SqliteArguments, SqliteRow};
use sqlx::{migrate::MigrateError, FromRow, Sqlite, SqlitePool};
use std::env;
use std::sync::Arc;

pub struct Database {
    pool: SqlitePool,
}

#[derive(Debug)]
pub struct DatabaseErr(sqlx::Error);

impl From<sqlx::Error> for DatabaseErr {
    fn from(err: sqlx::Error) -> Self {
        DatabaseErr(err)
    }
}

impl From<MigrateError> for DatabaseErr {
    fn from(err: MigrateError) -> Self {
        DatabaseErr(err.into())
    }
}

impl std::fmt::Display for DatabaseErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Database error: {}", self.0)
    }
}

impl std::error::Error for DatabaseErr {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl Database {
    pub async fn from(pool: SqlitePool) -> Result<Arc<Self>, DatabaseErr> {
        sqlx::migrate!().run(&pool).await?;
        Ok(Arc::new(Database { pool }))
    }

    pub async fn from_env() -> Result<Arc<Self>, DatabaseErr> {
        let database_url = env::var("DATABASE_URL")
            .map_err(|e| DatabaseErr(sqlx::Error::Configuration(e.into())))?;

        let pool = SqlitePool::connect(&database_url).await?;
        sqlx::migrate!().run(&pool).await?;

        Ok(Arc::new(Database { pool }))
    }

    pub async fn insert<'q>(
        &self,
        query: Query<'q, Sqlite, SqliteArguments<'q>>,
    ) -> Result<(), DatabaseErr> {
        query.execute(&self.pool).await?;
        Ok(())
    }

    pub async fn delete_one<T>(
        &self,
        sql: &str,
        args: SqliteArguments<'_>,
    ) -> Result<T, DatabaseErr>
    where
        T: for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    {
        let deleted = sqlx::query_as_with::<_, T, _>(sql, args)
            .fetch_one(&self.pool)
            .await?;

        Ok(deleted)
    }

    pub async fn delete_many<T>(
        &self,
        sql: &str,
        args: SqliteArguments<'_>,
    ) -> Result<Vec<T>, DatabaseErr>
    where
        T: for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    {
        let deleted = sqlx::query_as_with::<_, T, _>(sql, args)
            .fetch_all(&self.pool)
            .await?;

        Ok(deleted)
    }

    /// Executes a list of queries inside a single transaction.
    /// Returns early if any query fails, rolling back the transaction.
    pub async fn txn<'a>(
        &self,
        queries: Vec<Query<'a, Sqlite, SqliteArguments<'a>>>
    ) -> Result<(), DatabaseErr> {
        let mut tx = self.pool.begin().await?;

        for query in queries {
            query.execute(&mut *tx).await?;
        }

        tx.commit().await?;
        Ok(())
    }}
