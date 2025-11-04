use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::hash::{Hash, Hasher};
use crate::db_errors::{DatabaseError, ErrorTranslation};

pub type HostName = String;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Worker {
    pub host_name: HostName,
    pub grpc_port: u16,
    pub data_port: u16,
    pub num_slots: u32,
}

impl PartialEq for Worker {
    fn eq(&self, other: &Self) -> bool {
        self.host_name == other.host_name
    }
}

impl Eq for Worker {}

impl Hash for Worker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.host_name.hash(state);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct NetworkLink {
    pub source_host: HostName,
    pub target_host: HostName,
}

pub struct CreateWorker {
    pub host_name: HostName,
    pub grpc_port: u16,
    pub data_port: u16,
    pub num_slots: u32,
}

impl PartialEq for CreateWorker {
    fn eq(&self, other: &Self) -> bool {
        self.host_name == other.host_name
    }
}

impl Eq for CreateWorker {}

impl Hash for CreateWorker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.host_name.hash(state);
    }
}

impl ErrorTranslation for CreateWorker {
    fn unique_violation(&self, _err: sqlx::Error) -> DatabaseError {
        DatabaseError::WorkerAlreadyExists {
            host_name: self.host_name.clone(),
        }
    }
}

pub struct ShowWorkers {
    pub host_name: Option<HostName>,
}

pub struct DropWorker {
    pub host_name: HostName,
}

impl PartialEq for DropWorker {
    fn eq(&self, other: &Self) -> bool {
        self.host_name == other.host_name
    }
}

impl Eq for DropWorker {}

impl Hash for DropWorker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.host_name.hash(state);
    }
}

