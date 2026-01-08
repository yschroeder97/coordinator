use crate::catalog::query::query_catalog::QueryCatalog;
use crate::catalog::query::{FragmentId, QueryId, StopMode};
use crate::catalog::worker::endpoint::GrpcAddr;
use crate::network::worker_client::{
    RegisterFragmentRequest, Rpc, StartFragmentPayload, StartFragmentRequest, StopFragmentRequest,
    UnregisterFragmentPayload, UnregisterFragmentRequest, WorkerClientErr,
};
use crate::network::worker_registry::WorkerRegistryErr;
use crate::network::worker_registry::WorkerRegistryHandle;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::{debug, error, info, instrument};

mod worker_rpc_service {
    tonic::include_proto!("worker_rpc");
}

#[derive(Debug, Error)]
pub(super) enum ReconcilerErr {
    #[error("Worker registry error: {0}")]
    Registry(#[from] WorkerRegistryErr),
    #[error("Catalog error")]
    Catalog,
}

struct Reconciler {
    id: QueryId,
    query_catalog: Arc<QueryCatalog>,
    fragments: HashMap<GrpcAddr, FragmentId>,
    worker_registry: WorkerRegistryHandle,
}

impl Reconciler {
    async fn deploy(&mut self) -> Result<(), Vec<ReconcilerErr>> {
        info!("Deploying");
        self.register_fragments().await?;
        self.start_fragments().await
    }

    async fn terminate(&mut self, stop_mode: StopMode) -> Result<(), Vec<ReconcilerErr>> {
        debug!("Terminating");
        self.stop_fragments(stop_mode).await?;
        self.unregister_fragments().await
    }

    async fn broadcast_rpc<F, Rsp>(&self, make_rpc: F) -> Result<(), Vec<ReconcilerErr>>
    where
        F: Fn(FragmentId) -> (oneshot::Receiver<Result<Rsp, WorkerClientErr>>, Rpc),
        Rsp: Send + 'static,
    {
        let requests = self.fragments.iter().map(|(addr, &id)| {
            let (rx, rpc) = make_rpc(id);
            (addr.clone(), rpc, rx)
        });

        self.worker_registry
            .broadcast(requests)
            .await
            .map_err(|errs| errs.into_iter().map(ReconcilerErr::from).collect())
    }

    async fn register_fragments(&mut self) -> Result<(), Vec<ReconcilerErr>> {
        self.broadcast_rpc(|id| {
            let (rx, req) = RegisterFragmentRequest::new(id);
            (rx, Rpc::RegisterFragment(req))
        })
        .await
    }

    async fn start_fragments(&mut self) -> Result<(), Vec<ReconcilerErr>> {
        self.broadcast_rpc(|id| {
            let (rx, req) = StartFragmentRequest::new(StartFragmentPayload(id));
            (rx, Rpc::StartFragment(req))
        })
        .await
    }

    async fn stop_fragments(&mut self, stop_mode: StopMode) -> Result<(), Vec<ReconcilerErr>> {
        self.broadcast_rpc(|id| {
            let (rx, req) = StopFragmentRequest::new((id, stop_mode));
            (rx, Rpc::StopFragment(req))
        })
        .await
    }

    async fn unregister_fragments(&mut self) -> Result<(), Vec<ReconcilerErr>> {
        self.broadcast_rpc(|id| {
            let (rx, req) = UnregisterFragmentRequest::new(UnregisterFragmentPayload(id));
            (rx, Rpc::UnregisterFragment(req))
        })
        .await
    }

    async fn poll_status(&mut self) {
        // Implementation placeholder
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

pub struct QueryReconciler {
    inner: Reconciler,
    stop_listener: mpsc::Receiver<StopMode>,
}

impl QueryReconciler {
    pub fn new(
        id: QueryId,
        catalog: Arc<QueryCatalog>,
        worker_registry: WorkerRegistryHandle,
        stop_listener: mpsc::Receiver<StopMode>,
    ) -> Self {
        QueryReconciler {
            inner: Reconciler {
                id,
                query_catalog: catalog,
                fragments: HashMap::new(),
                worker_registry,
            },
            stop_listener,
        }
    }

    #[instrument(skip(self), fields(query_id = %self.inner.id))]
    pub async fn run(mut self) {
        info!("Starting reconciliation task");

        // We split the struct to satisfy the borrow checker.
        // `stop_listener` is owned by the loop (extracted from self).
        // `inner` is borrowed mutably in the loop.
        let mut stop_listener = self.stop_listener;

        // Deployment, can be interrupted by `stop` signal
        tokio::select! {
            stop_mode = stop_listener.recv() => {
                if let Some(stop_mode) = stop_mode {
                    self.inner.terminate(stop_mode).await;
                }
                return;
            },
            _ = self.inner.deploy() => {}
        }

        // Query is now running
        // Status polling, can be interrupted by `stop` signal
        loop {
            tokio::select! {
                stop_mode = stop_listener.recv() => {
                    if let Some(stop_mode) = stop_mode {
                        self.inner.terminate(stop_mode).await;
                    }
                    return;
                },
                _ = self.inner.poll_status() => {}
            }
        }
    }
}
