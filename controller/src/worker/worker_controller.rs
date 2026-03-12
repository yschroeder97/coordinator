use crate::worker::poly_join_set::{AbortHandle, JoinSet};
use crate::worker::worker_controller::WorkerStateInternal::{Active, Connecting};
use crate::worker::worker_client::{Rpc, WorkerClient, WorkerClientErr};
use crate::worker::worker_registry::WorkerRegistry;
use catalog::Reconcilable;
use catalog::worker_catalog::WorkerCatalog;
use model::worker::endpoint::GrpcAddr;
use model::worker::{self, DesiredWorkerState, WorkerState};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinError;
use tracing::{debug, error, info, warn};

const WORKER_SERVICE_POLL_INTERVAL: Duration = Duration::from_secs(5);

enum WorkerStateInternal {
    Connecting {
        model: worker::Model,
        handle: AbortHandle,
    },
    Active {
        #[allow(dead_code)]
        model: worker::Model,
        handle: AbortHandle,
    },
}

impl WorkerStateInternal {
    fn connect(
        model: worker::Model,
        connecting: &mut JoinSet<Result<(flume::Sender<Rpc>, WorkerClient), WorkerClientErr>>,
    ) -> Self {
        let handle = connecting.spawn(WorkerClient::connect(model.grpc_addr.clone()));
        Connecting { model, handle }
    }

    fn cleanup(self, addr: &GrpcAddr, registry: &WorkerRegistry) {
        match self {
            Connecting { handle, .. } => {
                handle.abort();
                debug!("aborted pending connection for worker {}", addr);
            }
            Active { .. } => {
                registry.unregister(addr);
                info!("removed active worker {}", addr);
            }
        }
    }
}

pub struct WorkerController {
    worker_catalog: Arc<WorkerCatalog>,
    registry: WorkerRegistry,
    workers: HashMap<GrpcAddr, WorkerStateInternal>,
    connecting: JoinSet<Result<(flume::Sender<Rpc>, WorkerClient), WorkerClientErr>>,
    active: JoinSet<GrpcAddr>,
}

impl WorkerController {
    pub fn new(worker_catalog: Arc<WorkerCatalog>, registry: WorkerRegistry) -> Self {
        WorkerController {
            worker_catalog,
            registry,
            workers: HashMap::default(),
            connecting: JoinSet::new(),
            active: JoinSet::new(),
        }
    }

    pub async fn run(mut self) {
        let mut intent_rx = self.worker_catalog.subscribe_intent();
        let mut state_rx = self.worker_catalog.subscribe_state();
        info!("starting");
        // Reconcile once
        self.reconcile().await;

        loop {
            // This select acts as a trigger for reconciliation
            tokio::select! {
                // Client requested addition/removal of a worker
                result = intent_rx.changed() => {
                    if result.is_err() {
                        info!("worker catalog notification channel closed, shutting down");
                        return;
                    }
                }
                // Health monitor marked a worker as unreachable
                Ok(()) = state_rx.changed() => {}
                // We made progress connecting to pending workers
                Some(connect_result) = self.connecting.join_next() => {
                    match connect_result {
                        Ok(Ok((sender, client))) => self.on_connect_ok(sender, client).await,
                        Ok(Err(e)) => self.on_connect_err(e).await,
                        // Connect task got aborted during reconciliation (i.e., worker removal intended), nothing to do
                        Err(e) if e.is_cancelled() => {}
                        // A panicked connect task: remove from internal state (might be added back in the next loop iteration)
                        Err(e) => {
                            error!("worker connection task failed: {:?}", e);
                            self.on_panic();
                        }
                    }
                    continue;
                }
                // A worker client exited on its own
                Some(result) = self.active.join_next() => {
                    self.on_client_exit(result);
                    continue;
                }
                _ = tokio::time::sleep(WORKER_SERVICE_POLL_INTERVAL) => {}
            }
            self.reconcile().await;
        }
    }

    async fn reconcile(&mut self) {
        let mismatched_workers = match self.worker_catalog.get_mismatch().await {
            Ok(workers) => workers,
            Err(e) => {
                warn!("failed to fetch workers: {:?}", e);
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
                        let old = self.workers.remove(&addr).unwrap();
                        old.cleanup(&addr, &self.registry);
                        info!("tearing down unreachable worker {}, will retry", addr);
                        self.workers.insert(
                            addr,
                            WorkerStateInternal::connect(mismatch, &mut self.connecting),
                        );
                    }
                    None => {
                        self.workers.insert(
                            addr,
                            WorkerStateInternal::connect(mismatch, &mut self.connecting),
                        );
                    }
                    _ => {}
                }
                DesiredWorkerState::Removed => {
                    if let Some(state) = self.workers.remove(&addr) {
                        state.cleanup(&addr, &self.registry);
                    }
                    self.worker_catalog
                        .remove_worker(mismatch.into())
                        .await
                        .expect("failed to mark worker as removed");
                }
            }
        }
    }

    async fn on_connect_ok(&mut self, rpc_sender: flume::Sender<Rpc>, client: WorkerClient) {
        let addr = client.grpc_addr();
        let model = match self.workers.remove(&addr) {
            Some(Connecting { model, .. }) => model,
            _ => {
                debug!(
                    "connection succeeded for {} but worker was already removed",
                    addr
                );
                return;
            }
        };

        let updated = self
            .worker_catalog
            .set_worker_state(model.into(), WorkerState::Active)
            .await
            .unwrap();
        self.registry.register(addr.clone(), rpc_sender);
        info!(%addr, "worker active");
        let task_addr = addr.clone();
        let handle = self.active.spawn(async move {
            client.run().await;
            task_addr
        });
        self.workers.insert(addr, Active { model: updated, handle });
    }

    fn on_client_exit(&mut self, result: Result<GrpcAddr, JoinError>) {
        match result {
            Ok(addr) => {
                info!(%addr, "worker client exited");
                self.registry.unregister(&addr);
                self.workers.remove(&addr);
            }
            Err(e) if e.is_cancelled() => {}
            Err(e) => {
                error!("worker client task failed: {e:?}");
                self.on_panic();
            }
        }
    }

    async fn on_connect_err(&mut self, err: WorkerClientErr) {
        error!("failed to connect to worker: {:?}", err);
        let addr = err.addr().clone();
        let model = match self.workers.remove(&addr) {
            Some(Connecting { model, .. }) => model,
            _ => {
                debug!(
                    "connection failed for {} but worker was already removed",
                    addr
                );
                return;
            }
        };

        self.worker_catalog
            .set_worker_state(model.into(), WorkerState::Unreachable)
            .await
            .unwrap();
    }

    fn on_panic(&mut self) {
        self.workers.retain(|addr, state| {
            let finished = match state {
                Connecting { handle, .. } | Active { handle, .. } => handle.is_finished(),
            };
            if finished {
                error!(%addr, "worker task panicked");
                if matches!(state, Active { .. }) {
                    self.registry.unregister(addr);
                }
                return false;
            }
            true
        });
    }
}
