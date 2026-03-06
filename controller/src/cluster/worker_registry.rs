use crate::cluster::worker_client::{Rpc, WorkerClientErr};
use common::error::Retryable;
use model::query::fragment::FragmentError;
use model::worker::endpoint::{GrpcAddr, HostAddr};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum WorkerError {
    #[error("Worker client '{0}' unavailable")]
    ClientUnavailable(GrpcAddr),

    #[error("RPC error")]
    ClientError(#[from] WorkerClientErr),

    #[error("Worker '{0}' has been removed")]
    WorkerRemoved(HostAddr),
}

impl Retryable for WorkerError {
    fn retryable(&self) -> bool {
        match self {
            Self::ClientUnavailable(_) | Self::ClientError(WorkerClientErr::Connection(..)) => {
                true
            }
            Self::ClientError(WorkerClientErr::Communication { status, .. }) => matches!(
                status.code(),
                tonic::Code::Unavailable
                    | tonic::Code::DeadlineExceeded
                    | tonic::Code::Unknown
                    | tonic::Code::Aborted
            ),
            Self::WorkerRemoved(_) => false,
        }
    }
}

fn meta_str(status: &tonic::Status, key: &str) -> String {
    status
        .metadata()
        .get(key)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

impl From<WorkerError> for FragmentError {
    fn from(e: WorkerError) -> Self {
        match e {
            WorkerError::ClientUnavailable(addr) => FragmentError::WorkerCommunication {
                msg: format!("Worker '{addr}' unavailable"),
            },
            WorkerError::WorkerRemoved(addr) => FragmentError::WorkerCommunication {
                msg: format!("Worker '{addr}' removed"),
            },
            WorkerError::ClientError(WorkerClientErr::Connection(err, addr)) => {
                FragmentError::WorkerCommunication {
                    msg: format!("Connection to '{addr}' failed: {err}"),
                }
            }
            WorkerError::ClientError(WorkerClientErr::Communication { addr, status })
                if matches!(
                    status.code(),
                    tonic::Code::Unavailable
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::Cancelled
                ) =>
            {
                FragmentError::WorkerCommunication {
                    msg: format!("gRPC error at '{addr}': {status}"),
                }
            }
            WorkerError::ClientError(WorkerClientErr::Communication { status, .. }) => {
                FragmentError::WorkerInternal {
                    code: meta_str(&status, "code").parse().unwrap_or(0),
                    msg: status.message().to_string(),
                    trace: meta_str(&status, "trace"),
                }
            }
        }
    }
}

pub struct WorkerRegistryHandle {
    shared: Arc<RwLock<HashMap<GrpcAddr, flume::Sender<Rpc>>>>,
}

impl Clone for WorkerRegistryHandle {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl WorkerRegistryHandle {
    pub(crate) async fn send(
        &self,
        addr: &GrpcAddr,
        rpc: Rpc,
    ) -> Result<(), WorkerError> {
        let sender: Result<flume::Sender<Rpc>, WorkerError> = {
            let workers = self
                .shared
                .read()
                .expect("No one should panic while holding this lock");

            workers
                .get(addr)
                .cloned()
                .ok_or_else(|| WorkerError::ClientUnavailable(addr.clone()))
        };

        sender?
            .send_async(rpc)
            .await
            .map_err(|_| WorkerError::ClientUnavailable(addr.clone()))
    }

}

#[derive(Default)]
pub struct WorkerRegistry {
    shared: Arc<RwLock<HashMap<GrpcAddr, flume::Sender<Rpc>>>>,
}

impl Clone for WorkerRegistry {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl WorkerRegistry {
    pub fn handle(&self) -> WorkerRegistryHandle {
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
