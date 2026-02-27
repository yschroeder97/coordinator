use crate::cluster::worker_client::{
    RegisterFragmentRequest, Rpc, StartFragmentRequest, StopFragmentRequest,
    UnregisterFragmentRequest, WorkerClientErr,
};
use crate::cluster::worker_registry::{WorkerError, WorkerRegistryHandle};
use crate::query::Completed;
use crate::query::pending::Pending;
use crate::query::planned::Planned;
use crate::query::registered::Registered;
use crate::query::running::Running;
use catalog::Catalog;
use futures_util::future;
use model::query;
use model::Set;
use model::query::fragment::{self, FragmentError, FragmentId, FragmentState};
use model::query::StopMode;
use model::query::query_state::QueryState;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_retry::RetryIf;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tracing::{debug, error, info, warn};

const MAX_RPC_ATTEMPTS: usize = 5;
const RPC_RETRY_BASE_MS: u64 = 50;

pub(crate) enum QueryStateInternal {
    Pending(Pending),
    Planned(Planned),
    Registered(Registered),
    Running(Running),
    Completed,
    Stopped,
    Failed,
}

impl QueryStateInternal {
    async fn from_current(query: &query::Model, catalog: &Catalog) -> Self {
        match query.current_state {
            QueryState::Pending => QueryStateInternal::Pending(Pending),
            QueryState::Planned | QueryState::Registered | QueryState::Running => {
                let fragments = catalog.query.get_fragments(query.id).await.unwrap();
                match query.current_state {
                    QueryState::Planned => QueryStateInternal::Planned(Planned { fragments }),
                    QueryState::Registered => QueryStateInternal::Registered(Registered { fragments }),
                    QueryState::Running => QueryStateInternal::Running(Running { fragments }),
                    _ => unreachable!(),
                }
            }
            QueryState::Completed | QueryState::Stopped | QueryState::Failed => {
                panic!("Terminal state should not be reconciled")
            }
        }
    }
}

impl From<Planned> for QueryStateInternal {
    fn from(s: Planned) -> Self {
        QueryStateInternal::Planned(s)
    }
}

impl From<Registered> for QueryStateInternal {
    fn from(s: Registered) -> Self {
        QueryStateInternal::Registered(s)
    }
}

impl From<Running> for QueryStateInternal {
    fn from(s: Running) -> Self {
        QueryStateInternal::Running(s)
    }
}

impl From<Completed> for QueryStateInternal {
    fn from(_: Completed) -> Self {
        QueryStateInternal::Completed
    }
}

pub trait Transition: Sized {
    type Next: Into<QueryStateInternal>;

    async fn transition(&mut self, ctx: &mut QueryContext) -> anyhow::Result<Self::Next>;
    async fn rollback(self, ctx: &mut QueryContext, mode: StopMode);
}

pub struct QueryContext {
    pub query: query::Model,
    pub catalog: Arc<Catalog>,
    pub worker_registry: WorkerRegistryHandle,
}

impl QueryContext {
    pub(crate) async fn broadcast_rpc<F, Rsp>(
        &self,
        fragments: &[fragment::Model],
        mk_rpc: F,
    ) -> Vec<Result<Rsp, WorkerError>>
    where
        F: Fn(FragmentId) -> (oneshot::Receiver<Result<Rsp, WorkerClientErr>>, Rpc),
        Rsp: Send + 'static,
    {
        let mk_rpc = &mk_rpc;
        let futures = fragments.iter().map(|fragment| {
            let registry = self.worker_registry.clone();
            let addr = fragment.grpc_addr.clone();
            async move {
                let strategy = ExponentialBackoff::from_millis(RPC_RETRY_BASE_MS)
                    .map(jitter)
                    .take(MAX_RPC_ATTEMPTS - 1);
                
                RetryIf::spawn(
                    strategy,
                    || {
                        let registry = registry.clone();
                        let addr = addr.clone();
                        async move {
                            let (rx, rpc) = mk_rpc(fragment.id);
                            registry.send(&addr, rpc).await?;
                            let rsp: Rsp = rx
                                .await
                                .map_err(|_| WorkerError::ClientUnavailable(addr))?
                                .map_err(WorkerError::from)?;
                            Ok(rsp)
                        }
                    },
                    |e: &WorkerError| e.is_retryable(),
                )
                .await
            }
        });
        future::join_all(futures).await
    }

    pub(crate) async fn register_fragments(
        &self,
        fragments: &[fragment::Model],
    ) -> Vec<Result<(), WorkerError>> {
        self.broadcast_rpc(fragments, |id| {
            let (rx, req) = RegisterFragmentRequest::new(id);
            (rx, Rpc::RegisterFragment(req))
        })
        .await
        .into_iter()
        .map(|r| r.map(|_| ()))
        .collect()
    }

    pub(crate) async fn start_fragments(
        &self,
        fragments: &[fragment::Model],
    ) -> Vec<Result<(), WorkerError>> {
        self.broadcast_rpc(fragments, |id| {
            let (rx, req) = StartFragmentRequest::new(id);
            (rx, Rpc::StartFragment(req))
        })
        .await
    }

    pub(crate) async fn stop_fragments(
        &self,
        stop_mode: StopMode,
        fragments: &[fragment::Model],
    ) -> Vec<Result<(), WorkerError>> {
        self.broadcast_rpc(fragments, |id| {
            let (rx, req) = StopFragmentRequest::new((id, stop_mode));
            (rx, Rpc::StopFragment(req))
        })
        .await
    }

    pub(crate) async fn unregister_fragments(
        &self,
        fragments: &[fragment::Model],
    ) -> Vec<Result<(), WorkerError>> {
        self.broadcast_rpc(fragments, |id| {
            let (rx, req) = UnregisterFragmentRequest::new(id);
            (rx, Rpc::UnregisterFragment(req))
        })
        .await
    }

    /// Map RPC results to fragment active model updates, persist them, and return the
    /// updated fragments. Bails if at least one fragment failed and the query reached
    /// a terminal state.
    pub(crate) async fn apply_rpc_results(
        &mut self,
        fragments: &[fragment::Model],
        results: Vec<Result<(), WorkerError>>,
    ) -> anyhow::Result<Vec<fragment::Model>> {
        debug_assert!(!fragments.is_empty(), "More than one fragment expected");
        debug_assert!(
            fragments
                .iter()
                .all(|f| f.current_state == fragments.last().unwrap().current_state),
            "All fragments should be in the same state"
        );
        let current_state = fragments.last().unwrap().current_state;
        let target_state = current_state.next().expect("Cannot advance a terminal fragment state");

        let updates = fragments
            .iter()
            .zip(results)
            .map(|(fragment, rpc_result)| {
                let mut am: fragment::ActiveModel = fragment.clone().into();
                match rpc_result {
                    Ok(_) => am.current_state = Set(target_state),
                    Err(e) => {
                        let error = FragmentError::from(e);
                        warn!(fragment_id = fragment.id, %error, "Fragment transition failed");
                        am.current_state = Set(FragmentState::Failed);
                        am.error = Set(Some(error));
                    }
                }
                am
            })
            .collect();

        let (query, fragments) = self
            .catalog
            .query
            .update_fragment_states(self.query.id, updates)
            .await?;
        self.query = query;

        if self.query.current_state.is_terminal() {
            anyhow::bail!("Transition to {target_state} failed");
        }
        Ok(fragments)
    }

    pub(crate) async fn rollback_stop(&self, mode: StopMode, fragments: &[fragment::Model]) {
        for e in self
            .stop_fragments(mode, fragments)
            .await
            .into_iter()
            .filter_map(Result::err)
        {
            error!("Failed to stop fragment: {e}");
        }
    }

    pub(crate) async fn rollback_unregister(&self, fragments: &[fragment::Model]) {
        for e in self
            .unregister_fragments(fragments)
            .await
            .into_iter()
            .filter_map(Result::err)
        {
            error!("Failed to unregister fragment: {e}");
        }
    }

    pub(crate) async fn persist_stopped(&mut self) {
        match self.catalog.query.stop_query(&self.query).await {
            Ok(query) => self.query = query,
            Err(e) => error!("Failed to stop query: {e}"),
        }
    }

    pub(crate) async fn persist_failed(&self, error: String) {
        if let Err(e) = self
            .catalog
            .query
            .fail_query(self.query.clone(), error)
            .await
        {
            error!("Failed to set query to Failed: {e}");
        }
    }
}

async fn try_transition<T: Transition>(
    mut state: T,
    ctx: &mut QueryContext,
    stop_rx: &mut flume::Receiver<StopMode>,
) -> QueryStateInternal {
    tokio::select! {
        result = state.transition(ctx) => match result {
            Ok(next) => next.into(),
            Err(e) => {
                state.rollback(ctx, StopMode::Forceful).await;
                if ctx.query.error.is_none() {
                    warn!("Transition failed: {e:#}");
                    ctx.persist_failed(format!("{e:#}")).await;
                }
                QueryStateInternal::Failed
            }
        },
        Ok(mode) = stop_rx.recv_async() => {
            info!(?mode, "Stopping query");
            state.rollback(ctx, mode).await;
            ctx.persist_stopped().await;
            QueryStateInternal::Stopped
        }
    }
}

pub struct QueryReconciler;

impl QueryReconciler {
    pub async fn run(mut ctx: QueryContext, mut stop_rx: flume::Receiver<StopMode>) {
        info!("Starting reconciliation");
        let mut state = QueryStateInternal::from_current(&ctx.query, &ctx.catalog).await;
        loop {
            state = match state {
                QueryStateInternal::Pending(s) => try_transition(s, &mut ctx, &mut stop_rx).await,
                QueryStateInternal::Planned(s) => try_transition(s, &mut ctx, &mut stop_rx).await,
                QueryStateInternal::Registered(s) => try_transition(s, &mut ctx, &mut stop_rx).await,
                QueryStateInternal::Running(s) => try_transition(s, &mut ctx, &mut stop_rx).await,
                terminal => {
                    let label = match terminal {
                        QueryStateInternal::Completed => "Completed",
                        QueryStateInternal::Stopped => "Stopped",
                        QueryStateInternal::Failed => "Failed",
                        _ => unreachable!(),
                    };
                    info!("Reconciliation finished: {label}");
                    break;
                }
            };
        }
    }
}
