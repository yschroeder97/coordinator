use anyhow::{Result, bail};
use migration::{Migrator, MigratorTrait};
use sea_orm::{ConnectOptions, DatabaseConnection, DbErr, RuntimeErr, SqlxError};
use std::future::Future;
use std::time::Duration;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tracing::{error, warn};

pub enum StateBackend {
    Memory,
    Sqlite {
        endpoint: String,
        opts: ConnectOptions,
    },
    Postgres {
        endpoint: String,
        opts: ConnectOptions,
    },
}

#[derive(Clone)]
pub struct Database {
    pub conn: DatabaseConnection,
    pub endpoint: String,
}

impl Database {
    pub async fn with(backend: StateBackend) -> Result<Self> {
        const MAX_DURATION: Duration = Duration::new(u64::MAX / 4, 0);

        match backend {
            StateBackend::Memory => {
                const IN_MEMORY_DB: &str = "sqlite::memory:";

                let conn = sea_orm::Database::connect(
                    ConnectOptions::new(IN_MEMORY_DB)
                        .min_connections(1)
                        .max_connections(1)
                        .acquire_timeout(MAX_DURATION)
                        .connect_timeout(MAX_DURATION)
                        .sqlx_logging(false)
                        .to_owned(),
                )
                .await?;
                Ok(Self {
                    conn,
                    endpoint: IN_MEMORY_DB.to_owned(),
                })
            }
            _ => bail!("Storage backend currently not supported"),
        }
    }

    #[cfg(any(test, feature = "testing"))]
    pub async fn for_test() -> Self {
        let this = Self::with(StateBackend::Memory).await.unwrap();
        Migrator::up(&this.conn, None).await.unwrap();
        this
    }

    pub async fn migrate(&self) -> Result<()> {
        Migrator::up(&self.conn, None).await?;
        Ok(())
    }

    pub async fn with_retry<F, Fut, T>(&self, op: F) -> Result<T, DbErr>
    where
        F: Fn(DatabaseConnection) -> Fut,
        Fut: Future<Output = Result<T, DbErr>>,
    {
        let mut delays = retry_strategy();
        loop {
            match op(self.conn.clone()).await {
                Ok(v) => return Ok(v),
                Err(e) if is_retryable(&e) => match delays.next() {
                    Some(delay) => {
                        warn!("Transient database error, retrying in {delay:?}: {e}");
                        tokio::time::sleep(delay).await;
                    }
                    None => {
                        error!("Database operation failed after all retries: {e}");
                        return Err(e);
                    }
                },
                Err(e) => return Err(e),
            }
        }
    }
}

fn retry_strategy() -> impl Iterator<Item = Duration> {
    ExponentialBackoff::from_millis(50).map(jitter).take(5)
}

fn is_retryable(err: &DbErr) -> bool {
    match err {
        DbErr::ConnectionAcquire(_) => true,
        DbErr::Conn(RuntimeErr::SqlxError(e))
        | DbErr::Exec(RuntimeErr::SqlxError(e))
        | DbErr::Query(RuntimeErr::SqlxError(e)) => is_sqlx_retryable(e),
        _ => false,
    }
}

fn is_sqlx_retryable(err: &SqlxError) -> bool {
    matches!(err, SqlxError::PoolTimedOut | SqlxError::Io(_))
}
