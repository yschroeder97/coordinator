use anyhow::{Result, bail};
use common::error::Retryable;
use migration::{Migrator, MigratorTrait};
use sea_orm::{ConnectOptions, DatabaseConnection, DbErr};
use std::future::Future;
use std::time::Duration;
use tokio_retry::RetryIf;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tracing::warn;

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
    pub(crate) conn: DatabaseConnection,
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
                Ok(Self { conn })
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
        RetryIf::spawn(
            ExponentialBackoff::from_millis(50).map(jitter).take(5),
            || op(self.conn.clone()),
            |err: &DbErr| {
                if err.retryable() {
                    warn!("Transient database error, retrying: {err}");
                    return true;
                }
                false
            },
        )
        .await
    }
}
