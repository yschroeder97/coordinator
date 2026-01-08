use crate::catalog::worker::endpoint::GrpcAddr;
use crate::network::poly_join_set::JoinSet;
use crate::network::worker_client::{Rpc, WorkerClientErr};
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
pub enum WorkerRegistryErr {
    #[error("Worker client '{0}' unavailable")]
    ClientUnavailable(GrpcAddr),

    #[error(transparent)]
    Client(#[from] WorkerClientErr),
}

impl WorkerRegistryHandle {
    pub(crate) async fn send(&self, addr: &GrpcAddr, rpc: Rpc) -> Result<(), WorkerRegistryErr> {
        let sender = {
            let workers = self.shared.read().unwrap();
            workers.get(addr).cloned()
        };

        if let Some(sender) = sender {
            sender
                .send_async(rpc)
                .await
                .map_err(|_| WorkerRegistryErr::ClientUnavailable(addr.clone()))
        } else {
            Err(WorkerRegistryErr::ClientUnavailable(addr.clone()))
        }
    }

    pub(crate) async fn broadcast<I, Rsp>(&self, requests: I) -> Result<(), Vec<WorkerRegistryErr>>
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
        let mut join_set = JoinSet::new();

        for (addr, rpc, rx) in requests {
            let registry = self.clone();
            join_set.spawn(async move {
                registry.send(&addr, rpc).await?;
                rx.await.map_err(|_| WorkerRegistryErr::ClientUnavailable(addr))??;
                Ok(())
            });
        }

        let mut errors = Vec::new();
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Err(e)) => errors.push(e),
                Err(e) => tracing::error!("Task join error: {:?}", e),
                _ => {}
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
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
        self.shared.write().unwrap().insert(addr, sender);
    }

    pub(crate) fn unregister(&self, addr: &GrpcAddr) {
        self.shared.write().unwrap().remove(addr);
    }
}
