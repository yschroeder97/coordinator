use sqlx::query::Query;
use sqlx::sqlite::{SqliteArguments, SqliteRow};
use sqlx::{migrate::MigrateError, FromRow, Sqlite, SqlitePool};
use std::env;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use tokio_retry2::strategy::{jitter, FixedInterval};
use tokio_retry2::{Retry, RetryError};
use tracing::error;

const SQLITE_BUSY_CODE: &str = "5";
const SQLITE_LOCKED_CODE: &str = "6";

pub struct Database {
    pool: SqlitePool,
}

#[derive(Error, Debug)]
pub(crate) enum TxnErr {
    #[error("Failed txn: {0}")]
    Failed(sqlx::Error),
}

#[derive(Error, Debug)]
pub enum DatabaseErr {
    #[error("Configuration error: {0}")]
    Config(#[from] env::VarError),

    #[error("Migration failed: {0}")]
    Migration(#[from] MigrateError),

    #[error(transparent)]
    Transaction(#[from] TxnErr),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

pub fn should_retry(err: sqlx::Error) -> RetryError<TxnErr> {
    match &err {
        sqlx::Error::Database(db_err) => {
            if let Some(code) = db_err.code() {
                if code == SQLITE_BUSY_CODE || code == SQLITE_LOCKED_CODE {
                    RetryError::transient(TxnErr::Failed(err))
                } else {
                    RetryError::permanent(TxnErr::Failed(err))
                }
            } else {
                RetryError::permanent(TxnErr::Failed(err))
            }
        }
        _ => RetryError::permanent(TxnErr::Failed(err)),
    }
}

/// Helper to inspect constraint violations from database errors.
/// Returns the constraint name if the error is a constraint violation, None otherwise.
/// Use this to transform generic database errors into specific domain errors.
pub fn inspect_constraint(err: &DatabaseErr) -> Option<&str> {
    match err {
        DatabaseErr::Database(sqlx_err) => match sqlx_err {
            sqlx::Error::Database(db_err) => db_err.constraint(),
            _ => None,
        },
        _ => None,
    }
}

impl Database {
    pub async fn from_pool(pool: SqlitePool) -> Result<Arc<Self>, DatabaseErr> {
        sqlx::migrate!().run(&pool).await?;
        Ok(Arc::new(Database { pool }))
    }

    pub async fn from_env() -> Result<Arc<Self>, DatabaseErr> {
        let database_url = env::var("DATABASE_URL")?;

        let pool = SqlitePool::connect(&database_url).await?;
        sqlx::migrate!().run(&pool).await?;

        Ok(Arc::new(Database { pool }))
    }

    pub async fn execute<'q>(
        &self,
        query: Query<'q, Sqlite, SqliteArguments<'q>>,
    ) -> Result<(), DatabaseErr> {
        query.execute(&self.pool).await?;
        Ok(())
    }

    pub async fn update<'q>(
        &self,
        sql: &str,
        args: SqliteArguments<'q>,
    ) -> Result<(), DatabaseErr> {
        sqlx::query_with(sql, args).execute(&self.pool).await?;
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
        sqlx::query_as_with::<_, T, _>(sql, args)
            .fetch_all(&self.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn select<T>(
        &self,
        sql: &str,
        args: SqliteArguments<'_>,
    ) -> Result<Vec<T>, DatabaseErr>
    where
        T: for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    {
        sqlx::query_as_with::<_, T, _>(sql, args)
            .fetch_all(&self.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn select_scalar<T>(
        &self,
        sql: &str,
        args: SqliteArguments<'_>,
    ) -> Result<Vec<T>, DatabaseErr>
    where
        T: for<'r> sqlx::Decode<'r, Sqlite> + sqlx::Type<Sqlite> + Send + Unpin,
    {
        sqlx::query_scalar_with::<_, T, _>(sql, args)
            .fetch_all(&self.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn select_one<T>(
        &self,
        sql: &str,
        args: SqliteArguments<'_>,
    ) -> Result<T, DatabaseErr>
    where
        T: for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    {
        sqlx::query_as_with::<_, T, _>(sql, args)
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into)
    }

    /// Implements a SQLite txn with automatic retries.
    ///
    /// Submit a fn that implements `Fn` such that the function can be invoked more than once.
    /// `action` should implement the sequence of queries executed on top of this DB instance.
    /// Wrap all retryable code in `Box::pin`.
    pub async fn txn<T, F>(&self, action: F) -> Result<T, TxnErr>
    where
        F: for<'c> Fn(
            &'c mut sqlx::Transaction<'_, Sqlite>,
        ) -> Pin<Box<dyn Future<Output = Result<T, sqlx::Error>> + Send + 'c>>,
    {
        const TXN_RETRY_MILLIS: u64 = 50;

        let strategy = FixedInterval::from_millis(TXN_RETRY_MILLIS)
            .map(jitter)
            .take(5);

        // Strategy: 5 retries after 50ms each
        // Action: Retry the txn
        // Condition: SQLite returned transient or permanent err
        Retry::spawn(strategy, || async {
            let mut tx = match self.pool.begin().await {
                Ok(tx) => tx,
                Err(e) => return Err(should_retry(e)),
            };

            let result = action(&mut tx).await;

            match result {
                Ok(val) => match tx.commit().await {
                    Ok(_) => Ok(val),
                    Err(e) => Err(should_retry(e)),
                },
                Err(e) => Err(should_retry(e)),
            }
        })
        .await
    }
    
    pub fn pool(&self) -> SqlitePool {
        self.pool.clone()
    }
}
