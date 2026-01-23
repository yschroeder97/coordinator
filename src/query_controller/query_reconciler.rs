use crate::catalog::query::fragment::{FragmentId, QueryFragment};
use crate::catalog::query::query_catalog::QueryCatalog;
use crate::catalog::query::query_state::QueryState;
use crate::catalog::query::{ActiveQuery, QueryId, StopMode};
use crate::catalog::worker::endpoint::GrpcAddr;
use crate::cluster_controller::worker_client::{
    RegisterFragmentRequest, Rpc, StartFragmentPayload, StartFragmentRequest, StopFragmentRequest,
    UnregisterFragmentPayload, UnregisterFragmentRequest, WorkerClientErr,
};
use crate::cluster_controller::worker_registry::{WorkerCommunicationError, WorkerRegistryHandle};
use crate::query_controller::pending_query::Pending;
use crate::query_controller::planned_query::Planned;
use crate::query_controller::registered_query::Registered;
use crate::query_controller::running_query::Running;
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::{error, info, warn};

pub trait Transition<NextState, E> {
    async fn try_transition(&mut self) -> Result<NextState, E>;
    async fn on_transition_ok(self, next_state: NextState) -> Query<NextState>;
    async fn on_transition_stopped(self, stop_mode: StopMode);
    async fn on_transition_failed(self, error: E);
}

pub struct Query<S> {
    pub id: QueryId,
    pub query_catalog: Arc<QueryCatalog>,
    pub worker_registry: WorkerRegistryHandle,
    pub state: S,
}

impl Query<Pending> {
    pub fn create(
        query: ActiveQuery,
        query_catalog: Arc<QueryCatalog>,
        worker_registry: WorkerRegistryHandle,
    ) -> Query<Pending> {
        Query {
            id: query.id,
            query_catalog,
            worker_registry,
            state: Pending::from(query.statement),
        }
    }
}

impl<S> Query<S> {
    pub fn transition_to<NextState>(self, new_state: NextState) -> Query<NextState> {
        Query {
            id: self.id,
            query_catalog: self.query_catalog,
            worker_registry: self.worker_registry,
            state: new_state,
        }
    }

    /// Broadcast an RPC to all fragments
    pub(crate) async fn broadcast_rpc<F, Rsp>(
        &self,
        fragments: &[QueryFragment],
        create_rpc: F,
    ) -> Vec<Result<Rsp, WorkerCommunicationError>>
    where
        F: Fn(FragmentId) -> (oneshot::Receiver<Result<Rsp, WorkerClientErr>>, Rpc),
        Rsp: Send + 'static,
    {
        let requests = fragments.iter().map(|fragment| {
            let (rx, rpc) = create_rpc(fragment.id);
            (
                GrpcAddr::new(fragment.host_name.clone(), fragment.grpc_port),
                rpc,
                rx,
            )
        });

        self.worker_registry.broadcast(requests).await
    }

    pub async fn register_fragments(
        &self,
        fragments: &[QueryFragment],
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
        fragments: &[QueryFragment],
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
        fragments: &[QueryFragment],
    ) -> Vec<Result<(), WorkerCommunicationError>> {
        self.broadcast_rpc(fragments, |id| {
            let (rx, req) = StopFragmentRequest::new((id, stop_mode));
            (rx, Rpc::StopFragment(req))
        })
        .await
    }

    pub async fn unregister_fragments(
        &self,
        fragments: &[QueryFragment],
    ) -> Vec<Result<(), WorkerCommunicationError>> {
        self.broadcast_rpc(fragments, |id| {
            let (rx, req) = UnregisterFragmentRequest::new(UnregisterFragmentPayload(id));
            (rx, Rpc::UnregisterFragment(req))
        })
        .await
    }
}

/// Macro to handle a state transition with stop signal racing.
/// On success, calls `on_transition_ok` which updates the catalog and returns the new Query.
/// On stop signal or failure, handles cleanup and returns from the function.
macro_rules! try_transition {
    ($query:ident, $stop_rx:expr) => {
        tokio::select! {
            biased;
            Ok(stop_mode) = $stop_rx.recv_async() => {
                info!("Stop requested");
                return $query.on_transition_stopped(stop_mode).await;
            },
            result = $query.try_transition() => match result {
                Ok(next_state) => $query.on_transition_ok(next_state).await,
                Err(e) => {
                    error!("Transition failed: {e:?}");
                    return $query.on_transition_failed(e).await;
                }
            }
        }
    };
}

/// A QueryReconciler is a task that controls the lifecycle of a query by driving it towards its desired state.
pub struct QueryReconciler;

impl QueryReconciler {
    pub async fn run(
        query: ActiveQuery,
        query_catalog: Arc<QueryCatalog>,
        worker_registry: WorkerRegistryHandle,
        stop_listener: flume::Receiver<StopMode>,
    ) {
        info!("Starting reconciliation task for query {}", query.id);

        match query.current_state {
            QueryState::Pending => {
                let mut query = Query::<Pending>::create(query, query_catalog, worker_registry);
                let mut query = try_transition!(query, stop_listener);
                let mut query = try_transition!(query, stop_listener);
                let mut query = try_transition!(query, stop_listener);
                let _ = try_transition!(query, stop_listener);
            }
            QueryState::Planned => {
                let mut query =
                    Query::<Planned>::resume(query, query_catalog, worker_registry).await;
                let mut query = try_transition!(query, stop_listener);
                let mut query = try_transition!(query, stop_listener);
                let _ = try_transition!(query, stop_listener);
            }
            QueryState::Registered => {
                let mut query =
                    Query::<Registered>::resume(query, query_catalog, worker_registry).await;
                let mut query = try_transition!(query, stop_listener);
                let _ = try_transition!(query, stop_listener);
            }
            QueryState::Running => {
                let mut query =
                    Query::<Running>::resume(query, query_catalog, worker_registry).await;
                let _ = try_transition!(query, stop_listener);
            }
            _ => {
                warn!("Query in terminal state must not be reconciled");
            }
        }
    }
}
