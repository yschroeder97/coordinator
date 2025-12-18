use super::worker::{CreateWorker, DropWorker, MarkWorkerDesired, Worker, WorkerState};
use crate::catalog::database::{Database, DatabaseErr};
use crate::catalog::tables::table;
use crate::catalog::worker::worker_endpoint::{GrpcAddr, HostName};
use sqlx::QueryBuilder;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WorkerCatalogError {
    #[error("Worker with host name '{host_name}' already exists")]
    WorkerAlreadyExists { host_name: HostName },

    #[error("Worker with host name '{host_name}' not found")]
    WorkerNotFound { host_name: HostName },

    #[error("Invalid worker config: {reason}")]
    InvalidWorkerConfig { reason: String },

    #[error("Database error: {0}")]
    Database(#[from] DatabaseErr),
}

pub struct WorkerCatalog {
    db: Arc<Database>,
}

impl WorkerCatalog {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    pub async fn create_worker(&self, worker: &CreateWorker) -> Result<(), WorkerCatalogError> {
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

        self.db
            .txn(vec![worker_insert, network_links_insert])
            .await?;

        Ok(())
    }

    pub async fn mark_workers_for_deletion(
        &self,
        query: &MarkWorkerDesired,
    ) -> Result<u64, WorkerCatalogError> {
        todo!()
    }

    pub async fn mark_worker(
        &self,
        grpc_addr: &GrpcAddr,
        current_state: WorkerState,
    ) -> Result<(), WorkerCatalogError> {
        todo!()
    }

    pub async fn drop_worker(&self, query: &DropWorker) -> Result<Vec<Worker>, WorkerCatalogError> {
        todo!()
    }
}
