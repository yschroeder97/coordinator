#![cfg(madsim)]

#[madsim::test]
async fn basic() {
    tracing_subscriber::fmt::init();

    let handle = Handle::current();
    let addr0 = "10.0.0.1:50051".parse::<SocketAddr>().unwrap();
    let ip1 = "10.0.0.2".parse().unwrap();
    let ip2 = "10.0.0.3".parse().unwrap();
    let ip3 = "10.0.0.4".parse().unwrap();
    let ip4 = "10.0.0.5".parse().unwrap();
    let ip5 = "10.0.0.6".parse().unwrap();
    let node0 = handle.create_node().name("server").ip(addr0.ip()).build();
    let node1 = handle.create_node().name("client1").ip(ip1).build();
    let node2 = handle.create_node().name("client2").ip(ip2).build();
    let node3 = handle.create_node().name("client3").ip(ip3).build();
    let node4 = handle.create_node().name("client4").ip(ip4).build();
    let node5 = handle.create_node().name("client5").ip(ip5).build();

    NetSim::current().add_dns_record("server", addr0.ip());

    node0.spawn(async move {
        Server::builder()
            .add_service(GreeterServer::new(MyGreeter::default()))
            .add_service(AnotherGreeterServer::new(MyGreeter::default()))
            .serve(addr0)
            .await
            .unwrap();
    });

    // unary
    let task1 = node1.spawn(async move {
        sleep(Duration::from_secs(1)).await;
        let mut client = GreeterClient::connect("http://server:50051").await.unwrap();
        let response = client.say_hello(request()).await.unwrap();
        assert_eq!(response.into_inner().message, "Hello Tonic! (10.0.0.2)");

        let request = tonic::Request::new(HelloRequest {
            name: "error".into(),
        });
        let response = client.say_hello(request).await.unwrap_err();
        assert_eq!(response.code(), tonic::Code::InvalidArgument);
    });

    // another service
    let task2 = node2.spawn(async move {
        sleep(Duration::from_secs(1)).await;
        let mut client = AnotherGreeterClient::connect("http://server:50051")
            .await
            .unwrap();
        let response = client.say_hello(request()).await.unwrap();
        assert_eq!(response.into_inner().message, "Hi Tonic!");
    });

    // server stream
    let task3 = node3.spawn(async move {
        sleep(Duration::from_secs(1)).await;
        let mut client = GreeterClient::connect("http://server:50051").await.unwrap();
        let response = client.lots_of_replies(request()).await.unwrap();
        let mut stream = response.into_inner();
        for i in 0..3 {
            let reply = stream.message().await.unwrap().unwrap();
            assert_eq!(reply.message, format!("{i}: Hello Tonic! (10.0.0.4)"));
        }
        let error = stream.message().await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unknown);
    });

    // client stream
    let task4 = node4.spawn(async move {
        sleep(Duration::from_secs(1)).await;
        let mut client = GreeterClient::connect("http://server:50051").await.unwrap();
        let response = client.lots_of_greetings(hello_stream()).await.unwrap();
        assert_eq!(
            response.into_inner().message,
            "Hello Tonic0 Tonic1 Tonic2! (10.0.0.5)"
        );
    });

    // bi-directional stream
    let task5 = node5.spawn(async move {
        sleep(Duration::from_secs(1)).await;
        let mut client = GreeterClient::connect("http://server:50051").await.unwrap();
        let response = client.bidi_hello(hello_stream()).await.unwrap();
        let mut stream = response.into_inner();
        let mut i = 0;
        while let Some(reply) = stream.message().await.unwrap() {
            assert_eq!(reply.message, format!("Hello Tonic{i}! (10.0.0.6)"));
            i += 1;
        }
        assert_eq!(i, 3);
    });

    task1.await.unwrap();
    task2.await.unwrap();
    task3.await.unwrap();
    task4.await.unwrap();
    task5.await.unwrap();
}
