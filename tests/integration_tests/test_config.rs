use coordinator::catalog::worker::GrpcAddr;

#[derive(Clone)]
pub struct TestConfig {
    pub workers: Vec<GrpcAddr>,
    pub message_bus_batch_size: Option<usize>,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            workers: (0..10).map(|_| GrpcAddr::next_local()).collect(),
            message_bus_batch_size: None,
        }
    }
}
