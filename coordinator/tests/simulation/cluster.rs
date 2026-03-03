#![cfg(madsim)]
use crate::worker::{HealthServer, HealthServiceImpl, SingleNodeWorker, WorkerRpcServiceServer};
use anyhow::Result;
use controller::request::Request;
use coordinator::coordinator::{CoordinatorRequest, start_for_test};
use futures::future::join_all;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::{Handle, NodeHandle};
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
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            num_workers: 1,
            kill_opts: Default::default(),
        }
    }
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
            net.create_node()
                .name(format!("worker-{i}"))
                .ip(format!("192.168.2.{i}").parse().unwrap())
                .init(move || async move {
                    info!("worker-{i} starting");
                    let worker = SingleNodeWorker::default();
                    let svc = WorkerRpcServiceServer::new(worker);

                    Server::builder()
                        .add_service(svc)
                        .add_service(HealthServer::new(HealthServiceImpl))
                        .serve("0.0.0.0:8080".parse().unwrap())
                        .await
                        .expect("Worker could not be started");
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
