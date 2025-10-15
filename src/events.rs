
#[derive(Clone)]
pub struct Heartbeat {
    pub worker_id: String
}

#[derive(Clone)]
pub struct RegisterWorker {
    pub worker_id: String,
    pub host_port: u16,
    pub grpc_port: u16,
    pub num_slots: Vec<u32>,
}
