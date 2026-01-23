use super::endpoint::{GrpcAddr, HostName};
use super::{CreateWorker, DropWorker, GetWorker, MarkWorker, Worker, WorkerState};
use crate::catalog::database::{Database, DatabaseErr, TxnErr};
use crate::catalog::notification::Notifier;
use crate::catalog::query_builder::ToSql;
use crate::catalog::tables::table;
use sqlx::sqlite::SqliteArguments;
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
            // If there are peers, use a transaction to insert both worker and cluster_controller links atomically
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


                        let mut builder = QueryBuilder::new(format!("INSERT INTO {} ", table::NETWORK_LINKS));
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
        // Note: This needs to be done in a txn, because the links are no FKs to the workers table
        // We don't want to force users to insert the workers in an order that keeps FK constraints at all times
        self.db
            .txn(move |txn| {
                let id = id.clone();

                Box::pin(async move {
                    let delete_worker = sqlx::query!(
                        "DELETE FROM workers WHERE host_name = ? AND grpc_port = ?",
                        id.host,
                        id.port
                    );

                    let delete_links = sqlx::query!(
                        "DELETE FROM network_links WHERE (src_host_name = ? AND src_grpc_port = ?) OR dst_host_name = ? AND dst_grpc_port = ?",
                         id.host,
                         id.port,
                         id.host,
                         id.port
                    );

                    delete_worker.execute(&mut **txn).await?;
                    delete_links.execute(&mut **txn).await?;

                    Ok(())
                })
            })
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

    pub async fn get_mismatch(&self) -> Result<Vec<Worker>, WorkerCatalogErr> {
        self.db
            .select(
                "SELECT * FROM workers WHERE current_state != desired_state",
                SqliteArguments::default(),
            )
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::test_utils::{
        arb_create_sink, arb_physical_with_refs, test_prop, PhysicalSourceWithRefs, SinkWithRefs,
    };
    use crate::catalog::Catalog;
    use proptest::proptest;

    async fn prop_worker_drop_with_refs_fails(catalog: Catalog, req: PhysicalSourceWithRefs) {
        catalog
            .source
            .create_logical_source(&req.logical)
            .await
            .expect("Logical source creation should succeed");
        catalog
            .worker
            .create_worker(&req.worker)
            .await
            .expect("Worker creation should succeed");
        catalog
            .source
            .create_physical_source(&req.physical)
            .await
            .expect("Physical source creation should succeed");

        // Property: Cannot drop worker while physical sources reference it
        let grpc_addr = GrpcAddr::new(req.worker.host_name, req.worker.grpc_port);

        assert!(
            catalog.worker.delete_worker(&grpc_addr).await.is_err(),
            "Should not be able to drop worker '{}' while physical sources reference it",
            grpc_addr,
        );
    }

    async fn prop_sink_worker_exists(catalog: Catalog, req: SinkWithRefs) {
        catalog
            .worker
            .create_worker(&req.worker)
            .await
            .expect("Worker creation should succeed");
        catalog
            .sink
            .create_sink(&req.sink)
            .await
            .expect("Sink creation should succeed");

        // Property: Cannot delete worker while sinks reference it
        let grpc_addr = GrpcAddr::new(req.worker.host_name, req.worker.grpc_port);

        assert!(
            catalog.worker.delete_worker(&grpc_addr).await.is_err(),
            "Should not be able to drop worker '{}' while sinks reference it",
            grpc_addr
        );
    }

    proptest! {
        #[test]
        fn worker_drop_with_refs_fails(req in arb_physical_with_refs()) {
            test_prop(|catalog| async move {
                prop_worker_drop_with_refs_fails(catalog, req).await;
            });
        }

        #[test]
        fn worker_drop_with_sinks_fails(req in arb_create_sink()) {
            test_prop(|catalog| async move {
                prop_sink_worker_exists(catalog, req).await;
            })
        }
    }
}
