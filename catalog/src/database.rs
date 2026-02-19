use anyhow::{Result, bail};
use migration::{Migrator, MigratorTrait};
use sea_orm::{ConnectOptions, DatabaseConnection};
use std::time::Duration;

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

    #[cfg(test)]
    pub async fn for_test() -> Self {
        let this = Self::with(StateBackend::Memory).await.unwrap();
        Migrator::up(&this.conn, None).await.unwrap();
        this
    }

    pub async fn migrate(&self) -> Result<()> {
        Migrator::up(&self.conn, None).await?;
        Ok(())
    }
}
