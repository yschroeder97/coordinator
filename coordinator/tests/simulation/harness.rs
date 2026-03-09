#![cfg(madsim)]
use crate::spec::NetworkConfig;
use crate::worker::{HealthServer, HealthServiceImpl, SingleNodeWorker, WorkerRpcServiceServer};
use anyhow::Result;
use common::request::Request;
use coordinator::coordinator::{CoordinatorRequest, start_for_test};
use futures::future::join_all;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::{Handle, NodeHandle};
use model::Generate;
use model::query::GetQuery;
use model::query::query_state::QueryState;
use model::worker::endpoint::NetworkAddr;
use model::worker::{CreateWorker, GetWorker, WorkerState};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{RngAlgorithm, TestRng, TestRunner};
use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;
use tracing::info;

const COORDINATOR_IP: &str = "192.168.1.1";
const DEFAULT_SEND_LATENCY_LO: Duration = Duration::from_millis(1);
const DEFAULT_SEND_LATENCY_HI: Duration = Duration::from_millis(10);

pub const POLL_INTERVAL: Duration = Duration::from_secs(2);

fn madsim_test_runner() -> TestRunner {
    let seed: [u8; 32] = thread_rng().r#gen();
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed);
    TestRunner::new_with_rng(Default::default(), rng)
}

pub fn arb<S: Strategy>(strategy: S) -> S::Value {
    strategy
        .new_tree(&mut madsim_test_runner())
        .unwrap()
        .current()
}

pub fn arb_query() -> model::query::CreateQuery {
    model::query::CreateQuery::generate()
        .new_tree(&mut madsim_test_runner())
        .unwrap()
        .current()
        .block_until(QueryState::Pending)
}

pub fn arb_topology_min(min: u8) -> Vec<CreateWorker> {
    CreateWorker::topology(min)
        .new_tree(&mut madsim_test_runner())
        .unwrap()
        .current()
}

pub struct TestHarness {
    workers: Vec<CreateWorker>,
    coordinator: (flume::Sender<CoordinatorRequest>, NodeHandle),
}

impl TestHarness {
    pub async fn start(
        workers: &[CreateWorker],
        network: Option<NetworkConfig>,
    ) -> Result<Self> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        let handle = Handle::current();

        madsim::net::NetSim::current().update_config(|cfg| {
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

        Self::start_workers(&handle, workers);
        let coordinator = Self::start_coordinator(&handle).await;

        Ok(Self {
            workers: workers.to_vec(),
            coordinator,
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

    fn start_workers(net: &Handle, workers: &[CreateWorker]) {
        workers.iter().enumerate().for_each(|(i, worker)| {
            let idx = i + 1;
            let ip = worker.host_addr.host.clone();
            net.create_node()
                .name(format!("worker-{idx}"))
                .ip(ip.parse().unwrap())
                .init(move || async move {
                    info!("worker-{idx} starting");
                    let worker = SingleNodeWorker::new(Arc::new(AtomicBool::new(false)));
                    let svc = WorkerRpcServiceServer::new(worker);

                    Server::builder()
                        .add_service(svc)
                        .add_service(HealthServer::new(HealthServiceImpl))
                        .serve("0.0.0.0:8080".parse().unwrap())
                        .await
                        .expect("Worker could not be started");
                })
                .build();
        });
    }

    pub async fn register_workers(&self) {
        for worker in &self.workers {
            let result: anyhow::Result<model::worker::Model> = self.send(worker.clone()).await;
            result.unwrap();
        }

        let num_workers = self.workers.len();
        loop {
            let workers: Vec<model::worker::Model> = self.send(GetWorker::all()).await.unwrap();

            let active_count = workers
                .iter()
                .filter(|w| w.current_state == WorkerState::Active)
                .count();

            if active_count == num_workers {
                return;
            }

            info!("waiting for workers to become Active ({active_count}/{num_workers})");
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    pub fn reset_network(&self) {
        let net = madsim::net::NetSim::current();
        let rt = Handle::current();

        let all_names: Vec<String> = std::iter::once("coordinator".to_string())
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

    pub fn ensure_node_running(&self, index: usize) {
        let name = self.worker_name(index);
        Handle::current().restart(&name);
    }

    pub async fn wait_for_convergence(&self, query_ids: &[i64]) {
        loop {
            let queries: Vec<model::query::Model> = self
                .send(GetQuery::all().with_ids(query_ids.to_vec()))
                .await
                .unwrap();

            let all_settled = queries.iter().all(|q| {
                q.current_state == QueryState::Running || q.current_state.is_terminal()
            });

            let workers: Vec<model::worker::Model> = self.send(GetWorker::all()).await.unwrap();

            let workers_settled = workers.iter().all(|w| {
                w.desired_state != model::worker::DesiredWorkerState::Active
                    || w.current_state == WorkerState::Active
            });

            if all_settled && workers_settled {
                return;
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
                    .filter(|w| w.desired_state == model::worker::DesiredWorkerState::Active && w.current_state != WorkerState::Active)
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
