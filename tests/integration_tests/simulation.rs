// #![cfg(madsim)]
use crate::worker::worker_rpc_service::worker_rpc_service_server::WorkerRpcServiceServer;
use crate::worker::SingleNodeWorker;
use coordinator::coordinator::start_test_coordinator;
use coordinator::coordinator::CoordinatorRequest;

use coordinator::catalog::query::{CreateQuery, CreateQueryRequest, GetQuery, GetQueryRequest};
use coordinator::catalog::worker::{
    CreateWorker, CreateWorkerRequest, GetWorker, GetWorkerRequest,
};
use madsim::runtime::Handle;
use std::net::SocketAddr;
use std::time::Duration;
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

        Server::builder()
            .add_service(svc)
            .serve(worker_addr)
            .await
            .unwrap();
    });

    let coordinator_node = handle
        .create_node()
        .name("coordinator")
        .ip("10.0.0.2".parse().unwrap())
        .build();

    let (rx_worker, req_worker) = CreateWorkerRequest::new(CreateWorker {
        host_name: "10.0.0.1".to_string(),
        grpc_port: 8080,
        data_port: 9090,
        capacity: 1,
        peers: vec![],
    });

    let (rx_query, req_query) = CreateQueryRequest::new(CreateQuery {
        name: "my_query".into(),
        stmt: "SELECT * FROM poo".into(),
        on_workers: vec!["10.0.0.1:8080".into()],
    });

    let (tx, rx) = flume::bounded(1);
    let _ = coordinator_node.spawn(async move {
        let sender = start_test_coordinator().await;
        tx.send(sender.clone()).unwrap();

        sender
            .send(CoordinatorRequest::CreateWorker(req_worker))
            .unwrap();
        sender
            .send(CoordinatorRequest::CreateQuery(req_query))
            .unwrap();
    });

    let sender = rx.recv_async().await.unwrap();
    let response_worker = rx_worker.await.unwrap();
    info!("CreateWorkerResponse: {:?}", response_worker);

    let response_query = rx_query.await.unwrap();
    info!("CreateQueryResponse: {:?}", response_query);

    for _ in 0..2 {
        let (rx_get_worker, req_get_worker) =
            GetWorkerRequest::new(GetWorker::new().with_id("10.0.0.1:8080".into()));
        sender
            .send(CoordinatorRequest::GetWorker(req_get_worker))
            .unwrap();
        info!("GetWorkerResponse: {:?}", rx_get_worker.await.unwrap());

        let (rx_get_query, req_get_query) =
            GetQueryRequest::new(GetQuery::new().with_id("my_query".into()));
        sender
            .send(CoordinatorRequest::GetQuery(req_get_query))
            .unwrap();
        info!("GetQueryResponse: {:?}", rx_get_query.await.unwrap());
        madsim::time::sleep(Duration::from_secs(5)).await;
    }
}
