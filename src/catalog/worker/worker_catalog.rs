use super::worker::{CreateWorker, DropWorker, GetWorker, Worker};
use crate::catalog::database::{Database, DatabaseErr, TxnErr};
use crate::catalog::notification::Notifier;
use crate::catalog::query_builder::ToSql;
use crate::catalog::tables::table;
use crate::catalog::worker::worker::MarkWorker;
use crate::catalog::worker::worker::WorkerState;
use crate::catalog::worker::worker_endpoint::{GrpcAddr, HostName};
use sqlx::QueryBuilder;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;

#[derive(Error, Debug)]
pub enum WorkerCatalogErr {
    #[error("Worker with host name '{host_name}' already exists")]
    WorkerAlreadyExists { host_name: HostName },

    #[error("Worker with host name '{host_name}' not found")]
    WorkerNotFound { host_name: HostName },

    #[error("Invalid worker config: {reason}")]
    InvalidWorkerConfig { reason: String },

    #[error("Database error: {0}")]
    Database(#[from] DatabaseErr),

    #[error("Txn error: {0}")]
    Txn(#[from] TxnErr),
}

pub struct WorkerCatalog {
    db: Arc<Database>,
    notifier_tx: watch::Sender<()>,
    notifier_rx: watch::Receiver<()>,
}

impl Notifier for WorkerCatalog {
    type Notification = ();

    fn subscribe(&self) -> watch::Receiver<()> {
        self.notifier_rx.clone()
    }

    fn notify(&self) {
        let _ = self.notifier_tx.send(());
    }
}

impl WorkerCatalog {
    pub fn new(db: Arc<Database>) -> Self {
        let (notifier_tx, notifier_rx) = watch::channel(());
        Self {
            db,
            notifier_tx,
            notifier_rx,
        }
    }

    pub async fn create_worker(&self, worker: &CreateWorker) -> Result<(), WorkerCatalogErr> {
        // If no peers, just insert the worker directly without a transaction
        if worker.peers.is_empty() {
            let query = sqlx::query(
                "INSERT INTO workers (host_name, grpc_port, data_port, capacity) VALUES (?, ?, ?, ?)",
            )
            .bind(&worker.host_name)
            .bind(worker.grpc_port)
            .bind(worker.data_port)
            .bind(worker.capacity);

            self.db.execute(query).await?;
        } else {
            // If there are peers, use a transaction to insert both worker and network links atomically
            let worker = worker.clone();

            self.db
                .txn(move |txn| {
                    let worker = worker.clone();

                    Box::pin(async move {
                        let worker_insert = sqlx::query(
                            "INSERT INTO workers (host_name, grpc_port, data_port, capacity) VALUES (?, ?, ?, ?)",
                        )
                        .bind(&worker.host_name)
                        .bind(worker.grpc_port)
                        .bind(worker.data_port)
                        .bind(worker.capacity);

                        let mut builder = QueryBuilder::new(format!("INSERT INTO {}", table::NETWORK_LINKS));
                        let network_links_insert = builder
                            .push_values(&worker.peers, |mut b, peer| {
                                b.push_bind(&worker.host_name)
                                    .push_bind(worker.grpc_port)
                                    .push_bind(&peer.host)
                                    .push_bind(peer.port);
                            })
                            .build();

                        worker_insert.execute(&mut **txn).await?;
                        network_links_insert.execute(&mut **txn).await?;

                        Ok(())
                    })
                })
                .await?;
        }

        self.notify();
        Ok(())
    }

    pub async fn drop_worker(&self, drop_req: &DropWorker) -> Result<(), WorkerCatalogErr> {
        let (sql, args) = drop_req.to_sql();
        self.db.update(&sql, args).await?;
        self.notify();
        Ok(())
    }

    pub async fn delete_worker(&self, id: &GrpcAddr) -> Result<(), WorkerCatalogErr> {
        self.db
            .execute(sqlx::query!(
                "DELETE FROM workers WHERE host_name = ? AND grpc_port = ?",
                id.host,
                id.port
            ))
            .await?;
        Ok(())
    }

    pub async fn mark_worker(
        &self,
        grpc_addr: &GrpcAddr,
        new_state: WorkerState,
    ) -> Result<(), WorkerCatalogErr> {
        let stmt = MarkWorker {
            addr: grpc_addr,
            new_current: new_state,
        };

        let (sql, args) = stmt.to_sql();
        self.db.update(&sql, args).await?;
        Ok(())
    }

    pub async fn get_workers(
        &self,
        get_worker: &GetWorker,
    ) -> Result<Vec<Worker>, WorkerCatalogErr> {
        let (sql, args) = get_worker.to_sql();
        self.db.select(&sql, args).await.map_err(Into::into)
    }
}
