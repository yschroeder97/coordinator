use crate::cluster::worker_client::{
    RegisterFragmentRequest, Rpc, StartFragmentPayload, StartFragmentRequest, StopFragmentRequest,
    UnregisterFragmentPayload, UnregisterFragmentRequest, WorkerClientErr,
};
use crate::cluster::worker_registry::{WorkerCommunicationError, WorkerRegistryHandle};
use crate::query::pending::Pending;
use crate::query::planned::Planned;
use crate::query::registered::Registered;
use crate::query::running::Running;
use crate::query::terminated::Terminated;
use catalog::Catalog;
use model::query;
use model::query::fragment::FragmentId;
use model::query::*;
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::info;

pub(crate) enum State {
    Pending(Pending),
    Planned(Planned),
    Registered(Registered),
    Running(Running),
    Completed(Terminated),
    Stopped(Terminated),
    Failed(Terminated),
}

pub trait Transition {
    async fn transition(self, ctx: QueryContext) -> State;
}

pub struct QueryContext {
    pub query: query::Model,
    pub catalog: Arc<Catalog>,
    pub worker_registry: WorkerRegistryHandle,
    pub stop_listener: flume::Receiver<StopMode>,
}

impl QueryContext {
    /// Broadcast an RPC to all fragments
    pub(crate) async fn broadcast_rpc<F, Rsp>(
        &self,
        fragments: &[fragment::Model],
        mk_rpc: F,
    ) -> Vec<Result<Rsp, WorkerCommunicationError>>
    where
        F: Fn(FragmentId) -> (oneshot::Receiver<Result<Rsp, WorkerClientErr>>, Rpc),
        Rsp: Send + 'static,
    {
        let requests = fragments.iter().map(|fragment| {
            let (rx, rpc) = mk_rpc(fragment.id);
            (fragment.grpc_addr.clone(), rpc, rx)
        });

        self.worker_registry.broadcast(requests).await
    }

    pub async fn register_fragments(
        &self,
        fragments: &[fragment::Model],
    ) -> Vec<Result<(), WorkerCommunicationError>> {
        self.broadcast_rpc(fragments, |id| {
            let (rx, req) = RegisterFragmentRequest::new(id);
            (rx, Rpc::RegisterFragment(req))
        })
        .await
        .into_iter()
        .map(|r| r.map(|_| ()))
        .collect()
    }

    pub async fn start_fragments(
        &self,
        fragments: &[fragment::Model],
    ) -> Vec<Result<(), WorkerCommunicationError>> {
        self.broadcast_rpc(fragments, |id| {
            let (rx, req) = StartFragmentRequest::new(StartFragmentPayload(id));
            (rx, Rpc::StartFragment(req))
        })
        .await
    }

    pub async fn stop_fragments(
        &self,
        stop_mode: StopMode,
        fragments: &[fragment::Model],
    ) -> Vec<Result<(), WorkerCommunicationError>> {
        self.broadcast_rpc(fragments, |id| {
            let (rx, req) = StopFragmentRequest::new((id, stop_mode));
            (rx, Rpc::StopFragment(req))
        })
        .await
    }

    pub async fn unregister_fragments(
        &self,
        fragments: &[fragment::Model],
    ) -> Vec<Result<(), WorkerCommunicationError>> {
        self.broadcast_rpc(fragments, |id| {
            let (rx, req) = UnregisterFragmentRequest::new(UnregisterFragmentPayload(id));
            (rx, Rpc::UnregisterFragment(req))
        })
        .await
    }
}

/// A QueryReconciler is a task that controls the lifecycle of a query by driving it towards its desired state.
pub struct QueryReconciler;

impl QueryReconciler {
    pub async fn run(query: query::Model, ctx: QueryContext) {
        info!("Starting");
    }
}
