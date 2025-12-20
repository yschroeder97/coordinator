use crate::catalog::notification::Notifier;
use crate::catalog::worker::worker::GetWorker;
use crate::catalog::worker::worker::WorkerState;
use crate::catalog::worker::worker_endpoint::{GrpcAddr, NetworkAddr};
use crate::catalog::worker::worker_catalog::WorkerCatalog;
use crate::network::cluster_service::WorkerStateInternal::{Active, Pending};
use crate::network::poly_join_set::{AbortHandle, JoinSet};
use crate::network::worker_client::{ConnErr, Rpc, WorkerClient};
use crate::network::worker_registry::{WorkerRegistry, WorkerRegistryHandle};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::debug;
use tracing::{error, info};

#[derive(Debug)]
enum WorkerStateInternal {
    Pending { handle: AbortHandle },
    Active { client: WorkerClient },
    Unreachable { last_seen: std::time::Instant },
    Failed { error: ClusterServiceErr },
}

#[derive(Error, Debug)]
pub enum ClusterServiceErr {
    #[error("Could not connect to worker with '{addr}'")]
    ConnectionErr { addr: NetworkAddr, msg: String },
}

pub(crate) struct ClusterService {
    catalog: Arc<WorkerCatalog>,
    registry: WorkerRegistry,
    workers: HashMap<GrpcAddr, WorkerStateInternal>,
    pending: JoinSet<Result<(flume::Sender<Rpc>, WorkerClient), ConnErr>>,
}

/// Invariants:
/// - An addr should be only in a single state set at a time
/// - The number of workers in all sets combined should be the number of workers in the catalog's table AFTER a call to on_client_request
/// - The current state in the catalog should match the set membership here
impl ClusterService {
    pub fn new(catalog: Arc<WorkerCatalog>) -> Self {
        ClusterService {
            catalog,
            registry: WorkerRegistry::default(),
            workers: HashMap::default(),
            pending: JoinSet::new(),
        }
    }

    pub fn registry_handle(&self) -> WorkerRegistryHandle {
        self.registry.handle()
    }

    pub async fn run(&mut self) -> Result<(), ClusterServiceErr> {
        let mut workers = self.catalog.subscribe();
        info!("Starting");

        loop {
            tokio::select! {
                _ = workers.changed() => self.on_client_request().await,
                Some(result) = self.pending.join_next() => match result {
                    Ok(progress) => self.on_progress_pending(progress).await,
                    Err(join_err) => {
                        error!("Worker connection task failed: {:?}", join_err);
                    }
                }
            }
        }
    }

    async fn on_client_request(&mut self) {
        self.add_workers().await;
        self.drop_workers().await;
    }

    async fn add_workers(&mut self) {
        let to_add = self
            .catalog
            .get_workers(&GetWorker::new().with_desired_state(WorkerState::Active))
            .await
            .unwrap();

        // Only add workers that are not present in the set
        for worker in to_add {
            let addr = GrpcAddr::new(worker.host_name, worker.grpc_port);
            if let Entry::Vacant(e) = self.workers.entry(addr.clone()) {
                let handle = self.pending.spawn(WorkerClient::connect(addr));
                e.insert(Pending { handle });
            }
        }
    }

    async fn drop_workers(&mut self) {
        let to_drop = self
            .catalog
            .get_workers(&GetWorker::new().with_desired_state(WorkerState::Removed))
            .await
            .unwrap();

        for worker in to_drop {
            let addr = GrpcAddr::new(worker.host_name, worker.grpc_port);
            if let Some(state) = self.workers.remove(&addr) {
                match state {
                    Pending { handle } => {
                        handle.abort();
                        debug!("Aborted pending connection for worker {}", addr);
                    }
                    Active { .. } => {
                        // Remove from registry when removing active worker
                        self.registry.unregister(&addr);
                        info!("Removed active worker {}", addr);
                    }
                    other => {
                        debug!("Removed worker {} in state {:?}", addr, other);
                    }
                }
            }
        }
    }

    async fn on_progress_pending(
        &mut self,
        client_or_err: Result<(flume::Sender<Rpc>, WorkerClient), ConnErr>,
    ) -> () {
        match client_or_err {
            Ok((sender, client)) => self.on_connect_success(sender, client).await,
            Err(ConnErr::Failed(err, addr)) => self.on_connect_err(err, addr).await,
        }
    }

    async fn on_connect_success(
        &mut self,
        rpc_sender: flume::Sender<Rpc>,
        client: WorkerClient,
    ) -> () {
        let addr = client.grpc_addr();
        self.catalog
            .mark_worker(&addr, WorkerState::Active)
            .await
            .unwrap();

        self.workers
            .entry(addr.clone())
            .and_modify(|state| *state = Active { client });

        self.registry.register(addr, rpc_sender);
    }

    async fn on_connect_err(&mut self, err: tonic::transport::Error, addr: GrpcAddr) -> () {
        self.catalog
            .mark_worker(&addr, WorkerState::Failed)
            .await
            .unwrap();

        self.workers.entry(addr.clone()).and_modify(|state| {
            *state = WorkerStateInternal::Failed {
                error: ClusterServiceErr::ConnectionErr {
                    addr,
                    msg: err.to_string(),
                },
            }
        });
        error!("Failed to connect to worker: {:?}", err);
    }
}
