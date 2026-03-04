#![cfg(madsim)]
use crate::worker::{HealthServer, HealthServiceImpl, MockWorkerConfig, SingleNodeWorker, WorkerRpcServiceServer};
use anyhow::Result;
use controller::cluster::service::CLUSTER_SERVICE_POLLING_DURATION;
use controller::cluster::worker_client::{CONNECT_INITIAL_BACKOFF_MS, CONNECT_MAX_RETRIES, CONNECT_TIMEOUT};
use controller::request::Request;
use coordinator::coordinator::{CoordinatorRequest, start_for_test};
use futures::future::join_all;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::{Handle, NodeHandle};
use model::worker::endpoint::NetworkAddr;
use model::worker::{CreateWorker, GetWorker, WorkerState};
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
    pub num_workers: usize,
    pub kill_opts: KillOpts,
    pub worker_config: MockWorkerConfig,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            num_workers: 1,
            kill_opts: Default::default(),
            worker_config: Default::default(),
        }
    }
}

pub const fn worker_recovery_deadline() -> Duration {
    let backoff_total_ms = CONNECT_INITIAL_BACKOFF_MS * ((1 << CONNECT_MAX_RETRIES) - 1);
    let connect_attempts_secs =
        (CONNECT_MAX_RETRIES as u64 + 1) * CONNECT_TIMEOUT.as_secs();
    Duration::from_secs(
        connect_attempts_secs
            + backoff_total_ms / 1000
            + CLUSTER_SERVICE_POLLING_DURATION.as_secs()
            + CONNECT_TIMEOUT.as_secs(),
    )
}

pub struct Cluster {
    pub config: ClusterConfig,
    pub handle: Handle,
    coordinator: (flume::Sender<CoordinatorRequest>, NodeHandle),
}

impl Cluster {
    pub async fn start(config: ClusterConfig) -> Result<Self> {
        let handle = madsim::runtime::Handle::current();

        Self::start_workers(&handle, &config);
        let coordinator = Self::start_coordinator(&handle).await;

        Ok(Self {
            config,
            handle,
            coordinator,
        })
    }

    pub async fn setup(num_workers: usize, worker_capacity: i32) -> Self {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        let cluster = Cluster::start(ClusterConfig {
            num_workers,
            ..Default::default()
        })
            .await
            .unwrap();

        for i in 1..=num_workers {
            let host = NetworkAddr::new(format!("192.168.2.{i}"), 9090);
            let grpc = NetworkAddr::new(format!("192.168.2.{i}"), 8080);
            let result: anyhow::Result<model::worker::Model> =
                cluster.send(CreateWorker::new(host, grpc, worker_capacity)).await;
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

    fn start_workers(net: &Handle, config: &ClusterConfig) {
        for i in 1..=config.num_workers {
            let worker_config = config.worker_config.clone();
            net.create_node()
                .name(format!("worker-{i}"))
                .ip(format!("192.168.2.{i}").parse().unwrap())
                .init(move || {
                    let wc = worker_config.clone();
                    async move {
                        info!("worker-{i} starting");
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
        for i in 1..=self.config.num_workers {
            if thread_rng().gen_bool(opts.kill_rate as f64) {
                nodes.push(format!("worker-{}", i));
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
