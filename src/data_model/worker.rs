use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Formatter;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};

pub type HostName = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Worker {
    pub host_name: HostName,
    pub grpc_port: u16,
    pub data_port: u16,
    pub num_slots: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkLink {
    pub source_host: HostName,
    pub target_host: HostName,
}

#[derive(Debug, Clone)]
pub struct CreateWorker {
    pub host_name: HostName,
    pub grpc_port: u16,
    pub data_port: u16,
    pub num_slots: u32,
    pub peers: Vec<HostName>,
}

pub struct GetWorker {
    pub host_name: Option<HostName>,
}

pub struct DropWorker {
    pub host_name: HostName,
}

const GRPC_PORT: u16 = 50051;
const BASE_IP: &str = "10.0.0.";
static IP_SUFFIX: AtomicU16 = AtomicU16::new(1);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GrpcAddr {
    host: HostName,
    port: u16,
}

impl GrpcAddr {
    pub fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }

    pub fn next_local() -> Self {
        let suffix = IP_SUFFIX.fetch_add(1, Ordering::Relaxed);
        // Ensure we stay within valid IP range (1-254)
        let valid_suffix = ((suffix - 1) % 254) + 1;
        let host = format!("{}{}", BASE_IP, valid_suffix);
        Self {
            host,
            port: GRPC_PORT,
        }
    }
}

impl From<&GrpcAddr> for SocketAddr {
    fn from(value: &GrpcAddr) -> Self {
        value.to_string().parse::<SocketAddr>().unwrap()
    }
}

impl fmt::Display for GrpcAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}
