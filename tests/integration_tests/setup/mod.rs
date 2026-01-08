use crate::worker::worker_rpc_service::worker_rpc_service_server::WorkerRpcServiceServer;
use crate::worker::SingleNodeWorker;
use madsim::runtime::{Handle, NodeHandle};
use std::net::SocketAddr;
use tonic::transport::Server;
use tracing::info;

pub struct TestConfig {
    pub num_workers: usize,
}

async fn start_mock_workers_on_host(addrs: Vec<(SocketAddr, String)>) -> Vec<NodeHandle> {
    let mut handles = Vec::new();
    for (addr, name) in addrs {
        let worker_node = handle.create_node().name(name).ip(addr.ip()).build();

        let node_handle = worker_node.spawn(async move {
            let worker = SingleNodeWorker::default();
            let svc = WorkerRpcServiceServer::new(worker);

            Server::builder()
                .add_service(svc)
                .serve(addr)
                .await
                .expect("Worker could not be started");

            info!("Worker server listening on {}", addr);
        });
        handles.push(node_handle);
    }
    handles
}
