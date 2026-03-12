use crate::worker::poly_join_set::{AbortHandle, JoinSet};
use catalog::Reconcilable;
use catalog::worker_catalog::WorkerCatalog;
use model::worker;
use model::worker::endpoint::GrpcAddr;
use model::worker::{GetWorker, WorkerState};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Endpoint;
use tokio_retry::Retry;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use common::supervised::supervised;
use crate::worker::worker_client::{CONNECT_TIMEOUT, CONNECT_INITIAL_BACKOFF_MS, CONNECT_MAX_RETRIES};
use tracing::{debug, info, instrument, warn};

pub mod health_proto {
    tonic::include_proto!("grpc.health.v1");
}

use health_proto::health_client::HealthClient;
use health_proto::HealthCheckRequest;

pub const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

pub struct HealthMonitor {
    catalog: Arc<WorkerCatalog>,
    tasks: JoinSet<GrpcAddr>,
    handles: HashMap<GrpcAddr, AbortHandle>,
}

impl HealthMonitor {
    pub fn new(catalog: Arc<WorkerCatalog>) -> Self {
        Self {
            catalog,
            tasks: JoinSet::new(),
            handles: HashMap::new(),
        }
    }

    #[instrument(skip(self))]
    pub async fn run(mut self) {
        let mut state_rx = self.catalog.subscribe_state();
        info!("starting");
        self.synchronize().await;

        loop {
            tokio::select! {
                result = state_rx.changed() => match result {
                    Ok(()) => self.synchronize().await,
                    Err(_) => {
                        info!("worker catalog notification channel closed, shutting down");
                        return;
                    }
                },
                Some(result) = self.tasks.join_next() => {
                    match result {
                        Ok(addr) => { self.handles.remove(&addr); }
                        Err(e) if e.is_cancelled() => {}
                        Err(e) => { warn!("health check task failed: {e:?}"); }
                    }
                }
            }
        }
    }

    async fn synchronize(&mut self) {
        let active = self
            .catalog
            .get_worker(GetWorker::all().with_current_state(WorkerState::Active))
            .await
            .unwrap_or_default();

        let active_addrs: HashSet<GrpcAddr> =
            active.iter().map(|w| w.grpc_addr.clone()).collect();

        // Abort and remove all health check tasks that do not belong to currently active workers
        // Reconnecting to unreachable workers does not belong in this service, but the worker client
        self.handles.retain(|addr, handle| {
            if active_addrs.contains(addr) {
                true
            } else {
                // This triggers removal from the join_set
                handle.abort();
                false
            }
        });

        for worker in active {
            // Skip workers that already have a health check task running
            if self.handles.contains_key(&worker.grpc_addr) {
                continue;
            }
            let addr = worker.grpc_addr.clone();
            let catalog = self.catalog.clone();
            let task_addr = addr.clone();
            // Spawn a health check task for an active worker that has no associated task
            let handle = self.tasks.spawn(async move {
                supervised(Self::check_worker(catalog, worker)).await;
                task_addr
            });
            self.handles.insert(addr, handle);
        }
    }

    async fn connect(addr: &GrpcAddr) -> Option<HealthClient<tonic::transport::Channel>> {
        let endpoint = Endpoint::from_shared(format!("http://{addr}"))
            .ok()?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(PROBE_TIMEOUT);

        let retry_strategy = ExponentialBackoff::from_millis(CONNECT_INITIAL_BACKOFF_MS)
            .map(jitter)
            .take(CONNECT_MAX_RETRIES);

        let channel = Retry::spawn(retry_strategy, || endpoint.connect()).await.ok()?;
        Some(HealthClient::new(channel))
    }

    async fn check_worker(catalog: Arc<WorkerCatalog>, worker: worker::Model) {
        let mut client = match Self::connect(&worker.grpc_addr).await {
            Some(c) => c,
            None => {
                Self::mark_unreachable(&catalog, worker).await;
                return;
            }
        };

        loop {
            tokio::time::sleep(HEALTH_CHECK_INTERVAL).await;

            let req = tonic::Request::new(HealthCheckRequest {
                service: String::new(),
            });

            if client.check(req).await.is_err() {
                Self::mark_unreachable(&catalog, worker).await;
                return;
            }
        }
    }

    async fn mark_unreachable(catalog: &WorkerCatalog, worker: worker::Model) {
        let addr = &worker.grpc_addr;
        debug!(%addr, "marking worker unreachable");
        catalog
            .set_worker_state(worker.into(), WorkerState::Unreachable)
            .await
            .ok();
    }
}
