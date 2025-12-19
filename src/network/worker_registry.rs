use crate::catalog::worker::worker_endpoint::GrpcAddr;
use crate::network::worker_client::Rpc;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub(crate) enum WorkerRegistryErr {
    SendErr(flume::SendError<Rpc>),
    AddrNotFound,
}

pub(crate) type WorkerRegistryResult = Result<(), WorkerRegistryErr>;

/// Read-only handle for sending RPCs to workers
pub(crate) struct WorkerRegistryHandle {
    shared: Arc<RwLock<HashMap<GrpcAddr, flume::Sender<Rpc>>>>,
}

impl WorkerRegistryHandle {
    pub async fn send_rpc(&self, addr: &GrpcAddr, rpc: Rpc) -> WorkerRegistryResult {
        let sender = {
            let workers = self.shared.read().unwrap();
            workers.get(addr).cloned()
        };

        if let Some(sender) = sender {
            sender
                .send_async(rpc)
                .await
                .map_err(WorkerRegistryErr::SendErr)
        } else {
            Err(WorkerRegistryErr::AddrNotFound)
        }
    }
}

/// Writer interface for the worker registry - only the cluster service should own this
#[derive(Default)]
pub(crate) struct WorkerRegistry {
    shared: Arc<RwLock<HashMap<GrpcAddr, flume::Sender<Rpc>>>>,
}

impl WorkerRegistry {
    pub fn handle(&self) -> WorkerRegistryHandle {
        WorkerRegistryHandle {
            shared: self.shared.clone(),
        }
    }

    pub fn register(&self, addr: GrpcAddr, sender: flume::Sender<Rpc>) {
        self.shared
            .write()
            .unwrap()
            .insert(addr, sender);
    }

    pub fn unregister(&self, addr: &GrpcAddr) {
        self.shared
            .write()
            .unwrap()
            .remove(addr);
    }
}
