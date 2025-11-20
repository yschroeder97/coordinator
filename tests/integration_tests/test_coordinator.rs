use madsim::{
    net::NetSim,
    rand::{thread_rng, Rng},
    runtime::Handle,
    time::sleep,
};
use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};
use tonic::transport::Server;
use crate::worker::SingleNodeWorker;
use crate::worker::worker_rpc_service::worker_rpc_service_server::WorkerRpcServiceServer;

#[madsim::test]
async fn basic() {
    tracing_subscriber::fmt::init();

    let handle = Handle::current();
    let addr0 = "10.0.0.1:50051".parse::<SocketAddr>().unwrap();
    let ip1 = "10.0.0.2".parse().unwrap();
    let ip2 = "10.0.0.3".parse().unwrap();
    let node0 = handle.create_node().name("worker").ip(addr0.ip()).build();
    let node1 = handle.create_node().name("coordinator").ip(ip1).build();

    NetSim::current().add_dns_record("worker", addr0.ip());

    node0.spawn(async move {
        Server::builder()
            .add_service(WorkerRpcServiceServer::new(SingleNodeWorker::default()))
            .serve(addr0)
            .await
            .unwrap();
    });

    // unary
    let task1 = node1.spawn(async move {
        sleep(Duration::from_secs(1)).await;
        let mut client = GreeterClient::connect("http://worker:50051").await.unwrap();
        let response = client.say_hello(request()).await.unwrap();
        assert_eq!(response.into_inner().message, "Hello Tonic! (10.0.0.2)");

        let request = tonic::Request::new(HelloRequest {
            name: "error".into(),
        });
        let response = client.say_hello(request).await.unwrap_err();
        assert_eq!(response.code(), tonic::Code::InvalidArgument);
    });

    task1.await.unwrap();
}
