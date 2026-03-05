#![cfg(madsim)]
use crate::worker::{HealthServer, HealthServiceImpl, MockWorkerConfig, SingleNodeWorker, WorkerRpcServiceServer};
use anyhow::Result;
use controller::cluster::service::CLUSTER_SERVICE_POLLING_DURATION;
use controller::cluster::worker_client::{CONNECT_INITIAL_BACKOFF_MS, CONNECT_MAX_RETRIES, CONNECT_TIMEOUT, RPC_TIMEOUT};
use controller::query::MAX_RPC_ATTEMPTS;
use controller::query::service::QUERY_SERVICE_POLLING_DURATION;
use controller::request::Request;
use coordinator::coordinator::{CoordinatorRequest, start_for_test};
use futures::future::join_all;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::{Handle, NodeHandle};
use model::query::CreateQuery;
use model::query::arb_create_query;
use model::testing::arb_topology as arb_topology_strategy;
use model::worker::endpoint::NetworkAddr;
use model::worker::{CreateWorker, GetWorker, WorkerState};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{TestRng, TestRunner, RngAlgorithm};
use std::fmt::Debug;
use std::time::Duration;
use tonic::transport::Server;
use tracing::info;

const COORDINATOR_IP: &str = "192.168.1.1";

#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct KillOpts {
    pub kill_rate: f32,
    pub restart_delay_secs: u32,
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub kill_opts: KillOpts,
    pub worker_config: MockWorkerConfig,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            kill_opts: Default::default(),
            worker_config: Default::default(),
        }
    }
}

// Upper bound on how long a worker can take to recover after restart:
// - ClusterService must poll to initiate reconnection (CLUSTER_SERVICE_POLLING_DURATION)
// - Connection uses exponential backoff with jitter: each step is up to
//   2x the base delay, so worst-case total is 2 * CONNECT_INITIAL_BACKOFF_MS * (2^N - 1)
// - Each of (CONNECT_MAX_RETRIES + 1) attempts waits up to CONNECT_TIMEOUT
// - One final CONNECT_TIMEOUT for the successful attempt
pub const fn worker_recovery_deadline() -> Duration {
    let backoff_total_ms = 2 * CONNECT_INITIAL_BACKOFF_MS * ((1 << CONNECT_MAX_RETRIES) - 1);
    let connect_attempts_secs =
        (CONNECT_MAX_RETRIES as u64 + 1) * CONNECT_TIMEOUT.as_secs();
    Duration::from_secs(
        connect_attempts_secs
            + backoff_total_ms / 1000
            + CLUSTER_SERVICE_POLLING_DURATION.as_secs()
            + CONNECT_TIMEOUT.as_secs(),
    )
}

pub const POLL_INTERVAL: Duration = Duration::from_secs(2);

// Upper bound on how long a query can take to reach its target state:
// - A crashed reconciler may complete up to 1 broadcast before failing,
//   then the next reconciler does 2 broadcasts (register + start) = 3 total
// - Each broadcast takes up to MAX_RPC_ATTEMPTS * RPC_TIMEOUT if all but
//   the last attempt time out
// - 2x QUERY_SERVICE_POLLING_DURATION for the two reconciler polling cycles
pub const fn query_reconciliation_deadline() -> Duration {
    let max_broadcast_secs = MAX_RPC_ATTEMPTS as u64 * RPC_TIMEOUT.as_secs();
    Duration::from_secs(
        2 * QUERY_SERVICE_POLLING_DURATION.as_secs() + 3 * max_broadcast_secs,
    )
}

fn madsim_test_runner() -> TestRunner {
    let seed: [u8; 32] = thread_rng().r#gen();
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed);
    TestRunner::new_with_rng(Default::default(), rng)
}

pub fn arb_query() -> CreateQuery {
    use model::query::query_state::QueryState;
    arb_create_query()
        .new_tree(&mut madsim_test_runner())
        .unwrap()
        .current()
        .block_until(QueryState::Pending)
}

pub fn arb_topology() -> Vec<CreateWorker> {
    arb_topology_strategy(1)
        .new_tree(&mut madsim_test_runner())
        .unwrap()
        .current()
}

pub fn arb_topology_min(min: u8) -> Vec<CreateWorker> {
    arb_topology_strategy(min)
        .new_tree(&mut madsim_test_runner())
        .unwrap()
        .current()
}

pub fn arb_worker_config() -> MockWorkerConfig {
    let max_delay_ms = MAX_RPC_ATTEMPTS as u64 * RPC_TIMEOUT.as_millis() as u64;
    let max: u64 = thread_rng().gen_range(0..=max_delay_ms);
    MockWorkerConfig {
        max_rpc_delay: Some(Duration::from_millis(max)),
        internal_error_rate: 0.0,
    }
}

pub struct Cluster {
    pub config: ClusterConfig,
    pub handle: Handle,
    workers: Vec<CreateWorker>,
    coordinator: (flume::Sender<CoordinatorRequest>, NodeHandle),
}

impl Cluster {
    pub async fn start(workers: &[CreateWorker], config: ClusterConfig) -> Result<Self> {
        let handle = madsim::runtime::Handle::current();

        Self::start_workers(&handle, workers, &config.worker_config);
        let coordinator = Self::start_coordinator(&handle).await;

        Ok(Self {
            config,
            handle,
            workers: workers.to_vec(),
            coordinator,
        })
    }

    pub async fn setup(workers: Vec<CreateWorker>, worker_config: MockWorkerConfig) -> Self {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        let num_workers = workers.len();

        let cluster = Cluster::start(&workers, ClusterConfig {
            worker_config,
            ..Default::default()
        })
            .await
            .unwrap();

        for worker in &workers {
            let result: anyhow::Result<model::worker::Model> =
                cluster.send(worker.clone()).await;
            result.unwrap();
        }

        let deadline = tokio::time::Instant::now() + worker_recovery_deadline();
        loop {
            let workers: anyhow::Result<Vec<model::worker::Model>> =
                cluster.send(GetWorker::all()).await;
            let workers = workers.unwrap();

            let active_count = workers
                .iter()
                .filter(|w| w.current_state == WorkerState::Active)
                .count();

            if active_count == num_workers {
                break;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out waiting for workers to become Active ({active_count}/{num_workers})"
            );
            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        cluster
    }

    pub fn num_workers(&self) -> usize {
        self.workers.len()
    }

    pub fn worker_name(&self, index: usize) -> String {
        format!("worker-{}", index + 1)
    }

    pub fn worker_host(&self, index: usize) -> NetworkAddr {
        self.workers[index].host_addr.clone()
    }

    pub async fn send<P, R>(&self, payload: P) -> R
    where
        P: Debug,
        Request<P, R>: Into<CoordinatorRequest>,
    {
        let (rx, req) = Request::new(payload);
        self.coordinator.0.send_async(req.into()).await.unwrap();
        rx.await.expect("coordinator should respond")
    }

    async fn start_coordinator(net: &Handle) -> (flume::Sender<CoordinatorRequest>, NodeHandle) {
        let (tx, rx) = flume::bounded(1);

        let node_handle = net
            .create_node()
            .name("coordinator")
            .ip(COORDINATOR_IP.parse().unwrap())
            .init({
                let tx = tx.clone();
                move || {
                    let tx = tx.clone();
                    async move {
                        let handle = start_for_test().await;
                        tx.send_async(handle)
                            .await
                            .expect("Failed to send coordinator handle");
                        std::future::pending::<()>().await;
                    }
                }
            })
            .build();

        let coordinator_handle = rx
            .recv_async()
            .await
            .expect("Failed to receive coordinator handle");
        (coordinator_handle, node_handle)
    }

    fn start_workers(net: &Handle, workers: &[CreateWorker], worker_config: &MockWorkerConfig) {
        for (i, worker) in workers.iter().enumerate() {
            let idx = i + 1;
            let ip = worker.host_addr.host.clone();
            let wc = worker_config.clone();
            net.create_node()
                .name(format!("worker-{idx}"))
                .ip(ip.parse().unwrap())
                .init(move || {
                    let wc = wc.clone();
                    async move {
                        info!("worker-{idx} starting");
                        let worker = SingleNodeWorker::new(wc);
                        let svc = WorkerRpcServiceServer::new(worker);

                        Server::builder()
                            .add_service(svc)
                            .add_service(HealthServer::new(HealthServiceImpl))
                            .serve("0.0.0.0:8080".parse().unwrap())
                            .await
                            .expect("Worker could not be started");
                    }
                })
                .build();
        }
    }

    pub async fn kill_node(&self, opts: &KillOpts) {
        let mut nodes = vec![];
        for i in 0..self.workers.len() {
            if thread_rng().gen_bool(opts.kill_rate as f64) {
                nodes.push(self.worker_name(i));
            }
        }
        if !nodes.is_empty() {
            self.kill_nodes(nodes, opts.restart_delay_secs).await;
        }
    }

    pub async fn kill_nodes(
        &self,
        nodes: impl IntoIterator<Item = impl AsRef<str>>,
        restart_delay_secs: u32,
    ) {
        join_all(nodes.into_iter().map(|name| async move {
            let name = name.as_ref();

            let t = thread_rng().gen_range(Duration::from_secs(0)..Duration::from_secs(1));
            tokio::time::sleep(t).await;
            info!("kill {name}");
            Handle::current().kill(name);

            let mut t = thread_rng().gen_range(Duration::from_secs(0)..Duration::from_secs(1));
            if thread_rng().gen_bool(0.1) {
                t += Duration::from_secs(restart_delay_secs as u64);
            }
            tokio::time::sleep(t).await;
            info!("restart {name}");
            Handle::current().restart(name);
        }))
        .await;
    }

    pub async fn kill_nodes_and_restart(
        &self,
        nodes: impl IntoIterator<Item = impl AsRef<str>>,
        restart_delay_secs: u32,
    ) {
        join_all(nodes.into_iter().map(|name| async move {
            let name = name.as_ref();
            info!("kill {name}");
            Handle::current().kill(name);
            tokio::time::sleep(Duration::from_secs(restart_delay_secs as u64)).await;
            info!("restart {name}");
            Handle::current().restart(name);
        }))
        .await;
    }

    pub async fn simple_kill_nodes(&self, nodes: impl IntoIterator<Item = impl AsRef<str>>) {
        join_all(nodes.into_iter().map(|name| async move {
            let name = name.as_ref();
            info!("kill {name}");
            Handle::current().kill(name);
        }))
        .await;
    }

    pub async fn simple_restart_nodes(&self, nodes: impl IntoIterator<Item = impl AsRef<str>>) {
        join_all(nodes.into_iter().map(|name| async move {
            let name = name.as_ref();
            info!("restart {name}");
            Handle::current().restart(name);
        }))
        .await;
    }
}
