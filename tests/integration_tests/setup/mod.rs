#![cfg(madsim)]
use crate::worker::worker_rpc_service::worker_rpc_service_server::WorkerRpcServiceServer;
use crate::worker::SingleNodeWorker;
use anyhow::Result;
use coordinator::coordinator::CoordinatorRequest;
use coordinator::coordinator::{start_test_coordinator, CoordinatorHandle};
use madsim::runtime::{Handle, NodeHandle};
use serde::Deserialize;
use std::fs;
use std::path::Path;
use tonic::transport::Server;

const COORDINATOR_IP: &str = "192.168.1.1";

#[derive(Deserialize, Debug)]
pub struct TestConfig {
    pub cluster_config: ClusterConfig,
}

impl TestConfig {
    #[allow(dead_code)]
    pub fn from_file(path: impl AsRef<Path>) -> Self {
        let content = fs::read_to_string(path).unwrap();
        toml::from_str(content.as_str()).unwrap()
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct ClusterConfig {
    pub num_workers: usize,
}

pub struct Cluster {
    pub config: ClusterConfig,
    pub handle: Handle,
    pub coordinator: (CoordinatorHandle, NodeHandle),
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

    pub async fn request(&self, req: CoordinatorRequest) {
        self.coordinator.0.send_async(req).await.unwrap();
    }

    async fn start_coordinator(net: &Handle) -> (CoordinatorHandle, NodeHandle) {
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
                        let handle = start_test_coordinator().await;
                        tx.send_async(handle)
                            .await
                            .expect("Failed to send coordinator handle");
                        // Keep the coordinator alive
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
                    let worker = SingleNodeWorker::default();
                    let svc = WorkerRpcServiceServer::new(worker);

                    Server::builder()
                        .add_service(svc)
                        .serve("0.0.0.0:8080".parse().unwrap())
                        .await
                        .expect("Worker could not be started");
                })
                .build();
        }
    }
}
