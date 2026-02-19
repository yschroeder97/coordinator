use crate::cluster::poly_join_set::JoinSet;
use crate::cluster::worker_client::{Rpc, WorkerClientErr};
use model::worker::endpoint::GrpcAddr;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use thiserror::Error;
use tokio::sync::oneshot;

/// Read-only handle for sending RPCs to workers
pub(crate) struct WorkerRegistryHandle {
    shared: Arc<RwLock<HashMap<GrpcAddr, flume::Sender<Rpc>>>>,
}

impl Clone for WorkerRegistryHandle {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

#[derive(Error, Debug)]
pub enum WorkerCommunicationError {
    #[error("Worker client '{0}' unavailable")]
    ClientUnavailable(GrpcAddr),

    #[error("Internal worker error")]
    ClientError(#[from] WorkerClientErr),
}

impl WorkerRegistryHandle {
    pub(crate) async fn send(
        &self,
        addr: &GrpcAddr,
        rpc: Rpc,
    ) -> Result<(), WorkerCommunicationError> {
        let sender: Result<flume::Sender<Rpc>, WorkerCommunicationError> = {
            let workers = self
                .shared
                .read()
                .expect("No one should panic while holding this lock");

            workers
                .get(addr)
                .cloned()
                .ok_or_else(|| WorkerCommunicationError::ClientUnavailable(addr.clone()))
        };

        sender?
            .send_async(rpc)
            .await
            // Client with addr has been removed
            .map_err(|_| WorkerCommunicationError::ClientUnavailable(addr.clone()))
    }

    pub(crate) async fn broadcast<I, Rsp>(
        &self,
        requests: I,
    ) -> Vec<Result<Rsp, WorkerCommunicationError>>
    where
        I: IntoIterator<
            Item = (
                GrpcAddr,
                Rpc,
                oneshot::Receiver<Result<Rsp, WorkerClientErr>>,
            ),
        >,
        Rsp: Send + 'static,
    {
        // Spawn a set of tasks for the RPCs
        let mut join_set: JoinSet<Result<Rsp, WorkerCommunicationError>> = JoinSet::new();
        for (addr, rpc, rx) in requests {
            let registry = self.clone();
            join_set.spawn(async move {
                // Try to send a single RPC to a client via the registry
                registry.send(&addr, rpc).await?;
                // Await the result of the RPC
                let rpc_reply = rx
                    .await
                    .map_err(|_| WorkerCommunicationError::ClientUnavailable(addr))??;
                Ok(rpc_reply)
            });
        }
        join_set.join_all().await
    }
}

/// Writer interface for the worker registry
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
        self.shared
            .write()
            .expect("No one should panic while holding this lock")
            .insert(addr, sender);
    }

    pub(crate) fn unregister(&self, addr: &GrpcAddr) {
        self.shared
            .write()
            .expect("No one should panic while holding this lock")
            .remove(addr);
    }
}
