use crate::catalog::notification::Notifier;
use crate::catalog::worker::endpoint::GrpcAddr;
use crate::catalog::worker::worker_catalog::WorkerCatalog;
use crate::catalog::worker::WorkerState;
use crate::cluster_controller::cluster_service::WorkerStateInternal::Connecting;
use crate::cluster_controller::poly_join_set::{AbortHandle, JoinSet};
use crate::cluster_controller::worker_client::{Rpc, WorkerClient, WorkerClientErr};
use crate::cluster_controller::worker_registry::{WorkerRegistry, WorkerRegistryHandle};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;
use tracing::{error, info};

const CLUSTER_SERVICE_POLLING_DURATION: tokio::time::Duration = tokio::time::Duration::from_secs(5);

enum WorkerStateInternal {
    Connecting {
        addr: GrpcAddr,
        handle: AbortHandle,
    },
    Active {
        addr: GrpcAddr,
        client: WorkerClient,
    },
}

pub(crate) struct ClusterService {
    worker_catalog: Arc<WorkerCatalog>,
    registry: WorkerRegistry,
    workers: HashMap<GrpcAddr, WorkerStateInternal>,
    connecting: JoinSet<Result<(flume::Sender<Rpc>, WorkerClient), WorkerClientErr>>,
}

impl ClusterService {
    pub fn new(worker_catalog: Arc<WorkerCatalog>) -> Self {
        ClusterService {
            worker_catalog,
            registry: WorkerRegistry::default(),
            workers: HashMap::default(),
            connecting: JoinSet::new(),
        }
    }

    pub fn registry_handle(&self) -> WorkerRegistryHandle {
        self.registry.handle()
    }

    pub async fn run(&mut self) {
        let mut workers = self.worker_catalog.subscribe();
        info!("Starting");
        self.reconcile().await;

        loop {
            if let Ok(result) =
                tokio::time::timeout(CLUSTER_SERVICE_POLLING_DURATION, workers.changed()).await
            {
                result.expect("Worker catalog notification channel closed unexpectedly")
            }
            self.reconcile().await;
        }
    }

    async fn reconcile(&mut self) {
        // 1. Fetch mismatches between the current state and desired state of queries from the catalog.
        let mismatched_workers = match self.worker_catalog.get_mismatch().await {
            Ok(queries) => queries,
            Err(e) => {
                error!("Failed to fetch workers: {:?}", e);
                return;
            }
        };

        // 2. Reconcile
        for mismatch in mismatched_workers {
            let addr = GrpcAddr::new(mismatch.host_name, mismatch.grpc_port);

            let should_delete = false;
            self.workers
                .entry(addr.clone())
                .and_modify(|state| {
                    // Task running for the mismatch
                    match state {
                        WorkerStateInternal::Connecting { addr, handle }
                            if mismatch.desired_state == WorkerState::Removed =>
                        {
                            handle.abort();
                            debug!("Aborted pending connection for worker {}", addr);
                        }
                        WorkerStateInternal::Connecting { .. }
                            if mismatch.desired_state == WorkerState::Active => {}
                        // Dropping the client here will lead to SendErr's in query reconcilers/worker registry
                        WorkerStateInternal::Active { addr, .. } => {
                            self.registry.unregister(&addr);
                            info!("Removed active worker {}", addr);
                        }
                        _ => panic!(
                            "Desired state should be one of (Active, Removed), but was {}",
                            mismatch.desired_state
                        ),
                    }
                })
                .or_insert_with(|| {
                    assert_eq!(
                        mismatch.desired_state,
                        WorkerState::Active,
                        "When no task associated with {} exists, only creation is valid",
                        &addr
                    );
                    Connecting {
                        addr: addr.clone(),
                        handle: self.connecting.spawn(WorkerClient::connect(addr.clone())),
                    }
                });

            if should_delete {
                self.worker_catalog.delete_worker(&addr).await.unwrap();
            }
        }
    }

    async fn on_progress_connecting(
        &mut self,
        client_or_err: Result<(flume::Sender<Rpc>, WorkerClient), WorkerClientErr>,
    ) -> () {
        match client_or_err {
            Ok((sender, client)) => self.on_connect_success(sender, client).await,
            Err(e) => self.on_connect_err(e).await,
        }
    }

    async fn on_connect_success(
        &mut self,
        rpc_sender: flume::Sender<Rpc>,
        client: WorkerClient,
    ) -> () {
        let addr = client.grpc_addr();
        // 1. Mark worker as ACTIVE to avoid polling it in the next reconciliation loop
        self.worker_catalog
            .mark_worker(&addr, WorkerState::Active)
            .await
            .unwrap();

        // 2. Move to active in internal state
        self.workers.entry(addr.clone()).and_modify(|state| {
            *state = WorkerStateInternal::Active {
                addr: addr.clone(),
                client,
            }
        });

        // 3. Register in worker registry to enable query reconcilers to send RPCs using their handles
        self.registry.register(addr, rpc_sender);
    }

    async fn on_connect_err(&mut self, err: WorkerClientErr) -> () {
        error!("Failed to connect to worker: {:?}", err);

        self.worker_catalog
            .mark_worker(err.addr(), WorkerState::Unreachable)
            .await
            .unwrap();

        self.workers.remove(err.addr());
    }
}
