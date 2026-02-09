use migration::{Migrator, MigratorTrait};
use sea_orm::{ConnectOptions, DatabaseConnection, sqlx};
use std::env;
use std::time::Duration;
use thiserror::Error;
use tokio_retry::RetryIf;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

const SQLITE_BUSY_CODE: &str = "5";
const SQLITE_LOCKED_CODE: &str = "6";
const SQLITE_BUSY_RECOVERY: &str = "261";
const SQLITE_BUSY_SNAPSHOT: &str = "517";
const SQLITE_LOCKED_SHARED_CACHE: &str = "262";

const SQLITE_CONSTRAINT_UNIQUE: &str = "2067";
const SQLITE_CONSTRAINT_PRIMARY_KEY: &str = "1555";
const SQLITE_CONSTRAINT_FOREIGN_KEY: &str = "787";
const SQLITE_CONSTRAINT_CHECK: &str = "275";

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
pub struct State {
    pub conn: DatabaseConnection,
    pub endpoint: String,
}

#[derive(Error, Debug)]
pub enum TxnError {
    #[error("Failed txn: {0}")]
    Failed(sqlx::Error),
}

#[derive(Error, Debug)]
pub enum StateError {
    #[error("Configuration error: {0}")]
    Config(#[from] env::VarError),

    #[error(transparent)]
    Transaction(#[from] TxnError),

    #[error("Database error: {0}")]
    Database(#[from] sea_orm::DbErr),

    #[error("Storage backend currently not supported")]
    NotImplemented,
}

impl State {
    pub async fn with(backend: StateBackend) -> Result<Self, StateError> {
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
                        .to_owned(),
                )
                .await?;
                Ok(Self {
                    conn,
                    endpoint: IN_MEMORY_DB.to_owned(),
                })
            }
            _ => Err(StateError::NotImplemented),
        }
    }

    #[cfg(test)]
    pub async fn for_test() -> Self {
        let this = Self::with(StateBackend::Memory).await.unwrap();
        Migrator::up(&this.conn, None).await.unwrap();
        this
    }

    pub async fn migrate(&self) -> Result<(), StateError> {
        Migrator::up(&self.conn, None).await?;
        Ok(())
    }

    pub fn should_retry(err: &sqlx::Error) -> bool {
        match err {
            sqlx::Error::Database(db_err) => {
                if let Some(code) = db_err.code() {
                    if code == SQLITE_BUSY_CODE
                        || code == SQLITE_LOCKED_CODE
                        || code == SQLITE_BUSY_SNAPSHOT
                        || code == SQLITE_BUSY_RECOVERY
                        || code == SQLITE_LOCKED_SHARED_CACHE
                    {
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            sqlx::Error::Io(_) => true,
            sqlx::Error::PoolTimedOut => true,
            sqlx::Error::Protocol(_) => true,
            _ => false,
        }
    }

    fn retry_strategy() -> impl Iterator<Item = std::time::Duration> {
        const DB_BASE_RETRY_DURATION: u64 = 50;
        const DB_MAX_RETRIES: usize = 5;

        ExponentialBackoff::from_millis(DB_BASE_RETRY_DURATION)
            .map(jitter)
            .take(DB_MAX_RETRIES)
    }

    // async fn with_retry<T, F, Fut>(&self, action: F) -> Result<T, DatabaseError>
    // where
    //     // F is a Factory: it creates a new Future every time it is called.
    //     // This is necessary because a Future cannot be re-run after it fails.
    //     // However, SqliteArguments borrow string slices to build queries.
    //     F: Fn() -> Fut,
    //     Fut: Future<Output = Result<T, sqlx::Error>>,
    // {
    //     RetryIf::spawn(Self::retry_strategy(), action, Self::should_retry)
    //         .await
    //         .map_err(Into::into)
    // }
}
