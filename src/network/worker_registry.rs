use crate::catalog::worker::endpoint::GrpcAddr;
use crate::network::worker_client::Rpc;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use thiserror::Error;

/// Read-only handle for sending RPCs to workers
pub(crate) struct WorkerRegistryHandle {
    shared: Arc<RwLock<HashMap<GrpcAddr, flume::Sender<Rpc>>>>,
}

#[derive(Error, Debug)]
pub enum WorkerRegistryErr {
    #[error("Worker at address '{0}' not found in registry")]
    AddrNotFound(GrpcAddr),

    #[error("Worker client receiver actor for address '{0}' has been dropped")]
    ClientUnavailable(GrpcAddr),
}

impl WorkerRegistryHandle {
    pub(crate) async fn send_rpc(&self, addr: &GrpcAddr, rpc: Rpc) -> Result<(), WorkerRegistryErr> {
        let sender = {
            let workers = self.shared.read().unwrap();
            workers.get(&addr).cloned()
        };

        if let Some(sender) = sender {
            sender
                .send_async(rpc)
                .await
                .map_err(|_| WorkerRegistryErr::ClientUnavailable(addr.clone()))
        } else {
            Err(WorkerRegistryErr::AddrNotFound(addr.clone()))
        }
    }
}

/// Writer interface for the worker registry - only the cluster service should own this
#[derive(Default)]
pub(crate) struct WorkerRegistry {
    shared: Arc<RwLock<HashMap<GrpcAddr, flume::Sender<Rpc>>>>,
}

impl WorkerRegistry {
    pub(crate) fn handle(&self) -> WorkerRegistryHandle {
        WorkerRegistryHandle {
            shared: self.shared.clone(),
        }
    }

    pub(crate) fn register(&self, addr: GrpcAddr, sender: flume::Sender<Rpc>) {
        self.shared.write().unwrap().insert(addr, sender);
    }

    pub(crate) fn unregister(&self, addr: &GrpcAddr) {
        self.shared.write().unwrap().remove(addr);
    }
}
