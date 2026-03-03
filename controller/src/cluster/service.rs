use crate::cluster::poly_join_set::{AbortHandle, JoinSet};
use crate::cluster::service::WorkerStateInternal::{Active, Connecting};
use crate::cluster::worker_client::{Rpc, WorkerClient, WorkerClientErr};
use crate::cluster::worker_registry::{WorkerRegistry, WorkerRegistryHandle};
use catalog::Reconcilable;
use catalog::worker_catalog::WorkerCatalog;
use model::worker::endpoint::GrpcAddr;
use model::worker::{self, DesiredWorkerState, WorkerState};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

pub const CLUSTER_SERVICE_POLLING_DURATION: tokio::time::Duration = tokio::time::Duration::from_secs(5);

enum WorkerStateInternal {
    Connecting {
        model: worker::Model,
        handle: AbortHandle,
    },
    Active {
        #[allow(dead_code)]
        model: worker::Model,
    },
}

pub struct ClusterService {
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
        let mut intent_rx = self.worker_catalog.subscribe_intent();
        let mut state_rx = self.worker_catalog.subscribe_state();
        info!("Starting");
        self.reconcile().await;

        loop {
            tokio::select! {
                result = intent_rx.changed() => {
                    result.expect("Worker catalog notification channel closed unexpectedly");
                }
                Ok(()) = state_rx.changed() => {}
                Some(result) = self.connecting.join_next() => {
                    match result {
                        Ok(client_or_err) => self.on_progress_connecting(client_or_err).await,
                        Err(e) if e.is_cancelled() => {}
                        Err(e) => panic!("Worker connection task panicked: {:?}", e),
                    }
                    continue;
                }
                _ = tokio::time::sleep(CLUSTER_SERVICE_POLLING_DURATION) => {}
            }
            self.reconcile().await;
        }
    }

    async fn reconcile(&mut self) {
        let mismatched_workers = match self.worker_catalog.get_mismatch().await {
            Ok(workers) => workers,
            Err(e) => {
                warn!("Failed to fetch workers: {:?}", e);
                return;
            }
        };

        for mismatch in mismatched_workers {
            let addr = mismatch.grpc_addr.clone();
            match mismatch.desired_state {
                DesiredWorkerState::Active => match self.workers.get(&addr) {
                    Some(Active { .. })
                        if mismatch.current_state == WorkerState::Unreachable =>
                    {
                        self.workers.remove(&addr);
                        self.registry.unregister(&addr);
                        info!("Tearing down unreachable worker {}, will retry", addr);
                        let handle =
                            self.connecting.spawn(WorkerClient::connect(addr.clone()));
                        self.workers
                            .insert(addr, Connecting { model: mismatch, handle });
                    }
                    None => {
                        let handle =
                            self.connecting.spawn(WorkerClient::connect(addr.clone()));
                        self.workers
                            .insert(addr, Connecting { model: mismatch, handle });
                    }
                    _ => {}
                }
                // Worker should be removed, remove from internal state and
                // either cancel the connection task or just unregister and drop the client task
                DesiredWorkerState::Removed => {
                    if let Some(state) = self.workers.remove(&addr) {
                        match state {
                            Connecting { handle, .. } => {
                                handle.abort();
                                debug!("Aborted pending connection for worker {}", addr);
                            }
                            Active { .. } => {
                                self.registry.unregister(&addr);
                                info!("Removed active worker {}", addr);
                            }
                        }
                    }
                    // Actually delete the worker from the catalog
                    self.worker_catalog
                        .delete_worker(&mismatch.host_addr)
                        .await
                        .expect("No one else should delete a worker");
                }
            }
        }
    }

    async fn on_progress_connecting(
        &mut self,
        client_or_err: Result<(flume::Sender<Rpc>, WorkerClient), WorkerClientErr>,
    ) {
        match client_or_err {
            Ok((sender, client)) => self.on_connect_success(sender, client).await,
            Err(e) => self.on_connect_err(e).await,
        }
    }

    async fn on_connect_success(&mut self, rpc_sender: flume::Sender<Rpc>, client: WorkerClient) {
        let addr = client.grpc_addr();
        let model = match self.workers.remove(&addr) {
            Some(Connecting { model, .. }) => model,
            _ => {
                debug!(
                    "Connection succeeded for {} but worker was already removed",
                    addr
                );
                return;
            }
        };

        let updated = self
            .worker_catalog
            .update_worker_state(model.into(), WorkerState::Active)
            .await
            .unwrap();

        self.workers.insert(addr.clone(), Active { model: updated });
        self.registry.register(addr, rpc_sender);
        tokio::spawn(client.run());
    }

    async fn on_connect_err(&mut self, err: WorkerClientErr) {
        error!("Failed to connect to worker: {:?}", err);
        let addr = err.addr().clone();
        let model = match self.workers.remove(&addr) {
            Some(Connecting { model, .. }) => model,
            _ => {
                debug!(
                    "Connection failed for {} but worker was already removed",
                    addr
                );
                return;
            }
        };

        self.worker_catalog
            .update_worker_state(model.into(), WorkerState::Unreachable)
            .await
            .unwrap();
    }
}
