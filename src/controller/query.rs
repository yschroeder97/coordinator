use crate::catalog::query::FragmentId;
use crate::catalog::worker::endpoint::GrpcAddr;
use crate::controller::query::worker_rpc_service::RegisterQueryReply;
use crate::network::poly_join_set::JoinSet;
use crate::network::worker_client::{RegisterFragmentRequest, Rpc, WorkerClientError};
use crate::network::worker_registry::{WorkerRegistryErr, WorkerRegistryHandle};
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use tokio_util::sync::CancellationToken;
use tracing::error;

mod worker_rpc_service {
    tonic::include_proto!("worker_rpc");
}

#[derive(Debug, Error)]
pub(super) enum QueryReconciliationErr {
    #[error("{0}")]
    WorkerRegistry(#[from] WorkerRegistryErr),

    #[error("Client returned an error: {0}")]
    WorkerClient(#[from] WorkerClientError),

    #[error("Task cancelled from supervisor")]
    CancelledByQueryService,

    #[error("WorkerClient was dropped before replying: {0}")]
    SenderDropped(#[from] RecvError),

    #[error("")]
    Failed,
}

pub(super) trait QueryState {}

struct Pending {
    plan: HashMap<GrpcAddr, FragmentId>,
}
struct Registered {
    cancel_token: CancellationToken,
}
struct Started {
    cancel_token: CancellationToken,
}
struct Running;
struct Stopped;

pub struct Query<S: QueryState> {
    state: S,
    worker_registry: WorkerRegistryHandle,
}

impl QueryState for Pending {}
impl QueryState for Registered {}
impl QueryState for Started {}
impl QueryState for Running {}
impl QueryState for Stopped {}

impl Query<Pending> {
    pub fn new(
        registry: WorkerRegistryHandle,
        plan: HashMap<GrpcAddr, FragmentId>,
    ) -> Query<Pending> {
        Query {
            state: Pending { plan },
            worker_registry: registry,
        }
    }

    pub async fn register(
        self,
        cancellation_token: CancellationToken,
    ) -> Result<Query<Registered>, QueryReconciliationErr> {
        let mut register_send_tasks = JoinSet::new();

        let receivers: Vec<oneshot::Receiver<Result<RegisterQueryReply, WorkerClientError>>> = self
            .state
            .plan
            .into_iter()
            .map(|(addr, id)| {
                let (rx, req) = RegisterFragmentRequest::new(id);
                (addr, rx, req)
            })
            .map(|(addr, rx, req)| {
                let cancel_token = cancellation_token.clone();
                register_send_tasks.spawn(self.send_register_fragment(&addr, req, cancel_token));
                rx
            })
            .collect::<Vec<_>>();
        
        
        if register_send_tasks.join_all().await.into_iter().all(|success: bool| success) {
            let mut await_recv_tasks = JoinSet::new();
            receivers.into_iter().map(|receiver| await_recv_tasks.spawn(cancellation_token.run_until_cancelled(async move {
                receiver.await
            })));
        } else {
            
        }

        Ok(Query {
            state: Registered {
                cancel_token: cancellation_token,
            },
            worker_registry: self.worker_registry,
        })
    }

    async fn send_register_fragment(
        &self,
        addr: &GrpcAddr,
        req: RegisterFragmentRequest,
        cancel_registration: CancellationToken,
    ) -> bool {
        let child_token = cancel_registration.clone();
        match cancel_registration
            .run_until_cancelled(async move {
                self.worker_registry
                    .send_rpc(addr, Rpc::RegisterFragment(req))
                    .await
            })
            .await
        {
            Some(Ok(())) => true, // RPC successfully sent to worker client actor (not worker!)
            Some(Err(_)) => {
                child_token.cancel();
                false
            }, // Client unavailable
            None => false // Cancelled by supervisor (QueryService), or another registration task that failed
        }
    }
}
