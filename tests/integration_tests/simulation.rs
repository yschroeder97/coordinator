#![cfg(madsim)]
use coordinator::coordinator::CoordinatorRequest;

use crate::setup::{Cluster, ClusterConfig};
use coordinator::catalog::query::{CreateQuery, CreateQueryRequest, GetQuery, GetQueryRequest};
use coordinator::catalog::worker::{
    CreateWorker, CreateWorkerRequest, GetWorker, GetWorkerRequest,
};
use std::time::Duration;
use tracing::info;

type FragmentId = u64;

#[madsim::test]
async fn simple() {
    tracing_subscriber::fmt().init();
    std::env::set_var("DATABASE_URL", "sqlite::memory:");

    let cluster = Cluster::start(ClusterConfig { num_workers: 4 })
        .await
        .unwrap();

    let (rx_create_worker, create_worker) = CreateWorkerRequest::new(CreateWorker {
        host_name: "192.168.2.1".to_string(),
        grpc_port: 8080,
        data_port: 9090,
        capacity: 1,
        peers: vec![],
    });

    let (rx_create_query, create_query) = CreateQueryRequest::new(CreateQuery {
        name: "my_query".into(),
        stmt: "SELECT * FROM poo".into(),
        on_workers: vec!["0.0.0.0:8080".into()],
    });

    cluster.request(create_worker.into()).await;
    let response_worker = rx_create_worker.await.unwrap();
    info!("CreateWorkerResponse: {:?}", response_worker);

    cluster.request(create_query.into()).await;
    let response_query = rx_create_query.await.unwrap();
    info!("CreateQueryResponse: {:?}", response_query);

    for _ in 0..2 {
        let (rx_get_worker, req_get_worker) =
            GetWorkerRequest::new(GetWorker::new().with_id("10.0.0.1:8080".into()));
        cluster.request(CoordinatorRequest::GetWorker(req_get_worker)).await;
        info!("GetWorkerResponse: {:?}", rx_get_worker.await.unwrap());

        let (rx_get_query, req_get_query) =
            GetQueryRequest::new(GetQuery::new().with_id("my_query".into()));
        cluster.request(CoordinatorRequest::GetQuery(req_get_query)).await;
        info!("GetQueryResponse: {:?}", rx_get_query.await.unwrap());
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
