#![cfg(madsim)]
use crate::worker::worker_rpc_service::worker_rpc_service_server::WorkerRpcServiceServer;
use crate::worker::SingleNodeWorker;
use coordinator::coordinator::start_test_coordinator;
use coordinator::coordinator::CoordinatorRequest;
use coordinator::errors::CoordinatorErr;
use coordinator::request::{CreateWorker, CreateWorkerRequest, DropWorkerRequest, WorkerQuery};

use madsim::runtime::Handle;
use madsim::time::Duration;
use std::net::SocketAddr;
use tonic::transport::Server;
use tracing::info;

type FragmentId = u64;

#[madsim::test]
async fn simple() {
    std::env::set_var("DATABASE_URL", "sqlite::memory:");

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .without_time()
        .init();

    let handle = Handle::current();

    let worker_addr: SocketAddr = "10.0.0.1:8080".parse().unwrap();
    let worker_node = handle
        .create_node()
        .name("worker")
        .ip(worker_addr.ip())
        .build();

    let _ = worker_node.spawn(async move {
        let worker = SingleNodeWorker::default();
        let svc = WorkerRpcServiceServer::new(worker);

        info!("Starting worker server on {}", worker_addr);

        Server::builder().add_service(svc).serve(worker_addr).await;
    });

    let coordinator_node = handle
        .create_node()
        .name("coordinator")
        .ip("10.0.0.2".parse().unwrap())
        .build();

    let (tx_create, rx_create) = flume::bounded::<Result<(), CoordinatorErr>>(1);

    let _ = coordinator_node.spawn(async move {
        let sender = start_test_coordinator().await;

        sender
            .send(CoordinatorRequest::CreateWorker(CreateWorkerRequest {
                payload: CreateWorker {
                    host_name: "10.0.0.1".to_string(),
                    grpc_port: 8080,
                    data_port: 9090,
                    capacity: 1,
                    peers: vec![],
                },
                respond_to: tx_create,
            }))
            .unwrap();
    });

    let response_create = rx_create.recv_async().await.unwrap();
    info!("CreateWorkerResponse: {:?}", response_create);
}
