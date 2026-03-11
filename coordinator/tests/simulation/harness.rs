#![cfg(madsim)]
use crate::spec::NetworkConfig;
use crate::worker::{HealthServer, HealthServiceImpl, SingleNodeWorker, WorkerRpcServiceServer};
use anyhow::Result;
use common::request::Request;
use coordinator::coordinator::{CoordinatorRequest, start_for_sim};
use futures::future::join_all;
use madsim::net::NetSim;
use madsim::runtime::{Handle, NodeHandle};
use model::{query, worker};
use model::query::GetQuery;
use model::query::query_state::QueryState;
use model::worker::endpoint::NetworkAddr;
use model::worker::{CreateWorker, DesiredWorkerState, GetWorker, WorkerState};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{RngAlgorithm, TestRng, TestRunner};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio_retry::Retry;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tonic::transport::Server;
use tracing::{debug, info};

const COORDINATOR_NAME: &str = "coordinator";
const COORDINATOR_IP: &str = "192.168.1.1";
const DEFAULT_SEND_LATENCY_LO: Duration = Duration::from_millis(1);
const DEFAULT_SEND_LATENCY_HI: Duration = Duration::from_millis(100);

pub const POLL_INTERVAL: Duration = Duration::from_secs(2);
const SEND_TIMEOUT: Duration = Duration::from_secs(30);
const CHACHA_SEED_BYTES: usize = 32;
const SEED_CHUNK_SIZE: usize = 8;

thread_local! {
    static RUNNER: std::cell::RefCell<TestRunner> = std::cell::RefCell::new({
        let seed = Handle::current().seed();
        let seed_bytes = seed.to_le_bytes();
        let mut full_seed = [0u8; CHACHA_SEED_BYTES];
        for (i, chunk) in full_seed.chunks_exact_mut(SEED_CHUNK_SIZE).enumerate() {
            chunk.copy_from_slice(&seed_bytes);
            chunk[0] = chunk[0].wrapping_add(i as u8);
        }
        let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &full_seed);
        TestRunner::new_with_rng(Default::default(), rng)
    });
}

pub fn arb<S: Strategy>(strategy: S) -> S::Value {
    RUNNER.with(|r| strategy.new_tree(&mut r.borrow_mut()).unwrap().current())
}

pub struct TestHarness {
    workers: Vec<CreateWorker>,
    coordinator_sender: Arc<std::sync::Mutex<flume::Sender<CoordinatorRequest>>>,
    coordinator_node: NodeHandle,
    db_path: String,
}

impl TestHarness {
    pub async fn start(
        workers: &[CreateWorker],
        network: Option<NetworkConfig>,
    ) -> Result<Self> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_timer(tracing_subscriber::fmt::time::uptime())
            .with_target(false)
            .try_init();

        let handle = Handle::current();

        if let Some(ref net) = network {
            net.validate();
        }

        NetSim::current().update_config(|cfg| {
            if let Some(ref net) = network {
                let lo = net
                    .send_latency_lo_ms
                    .map(Duration::from_millis)
                    .unwrap_or(DEFAULT_SEND_LATENCY_LO);
                let hi = net
                    .send_latency_hi_ms
                    .map(Duration::from_millis)
                    .unwrap_or(DEFAULT_SEND_LATENCY_HI);
                cfg.send_latency = lo..hi;
                cfg.packet_loss_rate = net.packet_loss_rate.unwrap_or(0.0);
            } else {
                cfg.send_latency = DEFAULT_SEND_LATENCY_LO..DEFAULT_SEND_LATENCY_HI;
                cfg.packet_loss_rate = 0.0;
            }
        });

        let seed = Handle::current().seed();
        let db_path = format!("{}/{COORDINATOR_NAME}-{seed}.db", std::env::temp_dir().display());
        let _ = std::fs::remove_file(&db_path);

        Self::start_workers(&handle, workers);
        let (sender, node) = Self::start_coordinator(&handle, &db_path).await;
        info!("started {} workers + coordinator", workers.len());

        Ok(Self {
            workers: workers.to_vec(),
            coordinator_sender: sender,
            coordinator_node: node,
            db_path,
        })
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

    pub fn worker_config(&self, index: usize) -> CreateWorker {
        self.workers[index].clone()
    }

    pub fn coordinator_name(&self) -> &str {
        COORDINATOR_NAME
    }

    pub async fn send<P, R>(&self, payload: P) -> R
    where
        P: Debug,
        Request<P, R>: Into<CoordinatorRequest>,
    {
        let (rx, req) = Request::new(payload);
        let sender = self.coordinator_sender.lock().unwrap().clone();
        sender.send_async(req.into()).await.unwrap();
        tokio::time::timeout(SEND_TIMEOUT, rx)
            .await
            .expect("coordinator did not respond within timeout")
            .expect("coordinator dropped the request")
    }

    async fn start_coordinator(
        net: &Handle,
        db_path: &str,
    ) -> (Arc<std::sync::Mutex<flume::Sender<CoordinatorRequest>>>, NodeHandle) {
        let shared_sender: Arc<std::sync::Mutex<flume::Sender<CoordinatorRequest>>> =
            Arc::new(std::sync::Mutex::new(flume::bounded(0).0));

        let (ready_tx, ready_rx) = flume::bounded(1);

        let node_handle = net
            .create_node()
            .name(COORDINATOR_NAME)
            .ip(COORDINATOR_IP.parse().unwrap())
            .init({
                let shared = shared_sender.clone();
                let ready_tx = ready_tx.clone();
                let db_path = db_path.to_string();
                move || {
                    let shared = shared.clone();
                    let ready_tx = ready_tx.clone();
                    let db_path = db_path.clone();
                    async move {
                        let sender = start_for_sim(&db_path).await;
                        *shared.lock().unwrap() = sender;
                        let _ = ready_tx.send_async(()).await;
                        std::future::pending::<()>().await;
                    }
                }
            })
            .build();

        ready_rx
            .recv_async()
            .await
            .expect("failed to receive coordinator ready signal");
        (shared_sender, node_handle)
    }

    fn start_workers(net: &Handle, workers: &[CreateWorker]) {
        workers.iter().enumerate().for_each(|(i, worker)| {
            let idx = i + 1;
            let ip = worker.host_addr.host.clone();
            net.create_node()
                .name(format!("worker-{idx}"))
                .ip(ip.parse().unwrap())
                .init(move || async move {
                    debug!("worker-{idx} starting");
                    let worker = SingleNodeWorker::new();

                    Server::builder()
                        .add_service(WorkerRpcServiceServer::new(worker))
                        .add_service(HealthServer::new(HealthServiceImpl))
                        .serve("0.0.0.0:8080".parse().unwrap())
                        .await
                        .expect("worker could not be started");
                })
                .build();
        });
    }

    pub async fn register_workers(&self) {
        let strategy = ExponentialBackoff::from_millis(100).map(jitter).take(10);
        for worker in &self.workers {
            Retry::spawn(strategy.clone(), || async {
                let result: anyhow::Result<worker::Model> =
                    self.send(worker.clone()).await;
                result
            })
            .await
            .expect("worker registration failed after retries");
        }

        let num_workers = self.workers.len();
        loop {
            let workers: Vec<worker::Model> = self.send(GetWorker::all()).await.unwrap();

            let active_count = workers
                .iter()
                .filter(|w| w.current_state == WorkerState::Active)
                .count();

            if active_count == num_workers {
                info!("all {num_workers} workers active");
                return;
            }

            info!("waiting for workers to become active ({active_count}/{num_workers})");
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    pub fn reset_network(&self) {
        let net = NetSim::current();
        let rt = Handle::current();

        let all_names: Vec<String> = std::iter::once(COORDINATOR_NAME.to_string())
            .chain((0..self.workers.len()).map(|i| self.worker_name(i)))
            .collect();

        let node_ids: Vec<_> = all_names
            .iter()
            .filter_map(|name| rt.get_node(name).map(|n| n.id()))
            .collect();

        for &src in &node_ids {
            net.unclog_node(src);
            for &dst in &node_ids {
                if src != dst {
                    net.unclog_link(src, dst);
                }
            }
        }

        net.update_config(|cfg| {
            cfg.send_latency = DEFAULT_SEND_LATENCY_LO..DEFAULT_SEND_LATENCY_HI;
            cfg.packet_loss_rate = 0.0;
        });
    }

    pub fn restart_worker(&self, index: usize) {
        let name = self.worker_name(index);
        Handle::current().restart(&name);
    }

    pub async fn wait_for_convergence(&self, query_ids: &[i64], max_wait: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + max_wait;
        loop {
            if tokio::time::Instant::now() >= deadline {
                info!("convergence timeout after {max_wait:?}");
                return false;
            }
            let queries: Vec<query::Model> = self
                .send(GetQuery::all().with_ids(query_ids.to_vec()))
                .await
                .unwrap();

            let all_settled = queries.iter().all(|q| {
                q.current_state == QueryState::Running || q.current_state.is_terminal()
            });

            let workers: Vec<worker::Model> = self.send(GetWorker::all()).await.unwrap();

            let workers_settled = workers.iter().all(|w| {
                w.desired_state != DesiredWorkerState::Active
                    || w.current_state == WorkerState::Active
            });

            if all_settled && workers_settled {
                return true;
            }

            info!(
                "waiting for convergence: queries={:?}, workers={:?}",
                queries
                    .iter()
                    .filter(|q| q.current_state != QueryState::Running && !q.current_state.is_terminal())
                    .map(|q| (q.id, q.current_state))
                    .collect::<Vec<_>>(),
                workers
                    .iter()
                    .filter(|w| w.desired_state == DesiredWorkerState::Active && w.current_state != WorkerState::Active)
                    .map(|w| (&w.host_addr, w.current_state))
                    .collect::<Vec<_>>()
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
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
