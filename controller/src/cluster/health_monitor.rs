use catalog::Reconcilable;
use catalog::worker_catalog::WorkerCatalog;
use model::worker::endpoint::GrpcAddr;
use model::worker::{GetWorker, WorkerState};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, info, instrument, warn};

pub mod health_proto {
    tonic::include_proto!("grpc.health.v1");
}

use health_proto::health_check_response::ServingStatus;
use health_proto::health_client::HealthClient;
use health_proto::HealthCheckRequest;

const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(10);
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const FAILURE_THRESHOLD: u32 = 3;

pub struct HealthMonitor {
    catalog: Arc<WorkerCatalog>,
    clients: HashMap<GrpcAddr, HealthClient<Channel>>,
    failures: HashMap<GrpcAddr, u32>,
}

impl HealthMonitor {
    pub fn new(catalog: Arc<WorkerCatalog>) -> Self {
        Self {
            catalog,
            clients: HashMap::new(),
            failures: HashMap::new(),
        }
    }

    #[instrument(skip(self))]
    pub async fn run(mut self) {
        let mut state_rx = self.catalog.subscribe_state();
        info!("Starting");

        loop {
            tokio::select! {
                Ok(()) = state_rx.changed() => {
                    self.sync_tracked_workers().await;
                }
                _ = tokio::time::sleep(HEALTH_CHECK_INTERVAL) => {
                    self.check_all().await;
                }
            }
        }
    }

    async fn sync_tracked_workers(&mut self) {
        let active = self
            .catalog
            .get_worker(GetWorker::all().with_current_state(WorkerState::Active))
            .await
            .unwrap_or_default();

        let active_addrs: std::collections::HashSet<GrpcAddr> =
            active.iter().map(|w| w.grpc_addr.clone()).collect();

        self.clients.retain(|addr, _| active_addrs.contains(addr));
        self.failures.retain(|addr, _| active_addrs.contains(addr));
    }

    async fn check_all(&mut self) {
        let active = self
            .catalog
            .get_worker(GetWorker::all().with_current_state(WorkerState::Active))
            .await
            .unwrap_or_default();

        for worker in active {
            let addr = worker.grpc_addr.clone();
            if self.probe(&addr).await {
                self.failures.remove(&addr);
            } else {
                let count = self.failures.entry(addr.clone()).or_default();
                *count += 1;
                warn!(%addr, failures = *count, "Health check failed");

                if *count >= FAILURE_THRESHOLD {
                    debug!(%addr, "Marking worker unreachable");
                    self.catalog
                        .update_worker_state(worker.into(), WorkerState::Unreachable)
                        .await
                        .ok();
                    self.clients.remove(&addr);
                    self.failures.remove(&addr);
                }
            }
        }
    }

    async fn probe(&mut self, addr: &GrpcAddr) -> bool {
        let client = match self.get_or_connect(addr).await {
            Some(c) => c,
            None => return false,
        };

        let req = tonic::Request::new(HealthCheckRequest {
            service: String::new(),
        });

        match tokio::time::timeout(PROBE_TIMEOUT, client.check(req)).await {
            Ok(Ok(response)) => response.into_inner().status == ServingStatus::Serving as i32,
            _ => {
                self.clients.remove(addr);
                false
            }
        }
    }

    async fn get_or_connect(&mut self, addr: &GrpcAddr) -> Option<&mut HealthClient<Channel>> {
        if !self.clients.contains_key(addr) {
            let endpoint = Endpoint::from_shared(format!("http://{addr}"))
                .ok()?
                .connect_timeout(CONNECT_TIMEOUT)
                .timeout(PROBE_TIMEOUT);

            let channel = match tokio::time::timeout(CONNECT_TIMEOUT, endpoint.connect()).await {
                Ok(Ok(ch)) => ch,
                _ => return None,
            };

            self.clients.insert(addr.clone(), HealthClient::new(channel));
        }
        self.clients.get_mut(addr)
    }
}
