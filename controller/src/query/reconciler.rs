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
use model::query::fragment::{self, FragmentError, FragmentId, FragmentState, FragmentUpdate};
use model::query::query_state::QueryState;
use model::query::*;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_retry::RetryIf;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tracing::{debug, error, info, warn};

const MAX_RPC_ATTEMPTS: usize = 3;
const RPC_RETRY_BASE_MS: u64 = 50;

pub(crate) enum State {
    Pending(Pending),
    Planned(Planned),
    Registered(Registered),
    Running(Running),
    Completed,
    Stopped,
    Failed,
}

impl State {
    async fn from_current(query: &query::Model, catalog: &Catalog) -> Self {
        match query.current_state {
            QueryState::Pending => State::Pending(Pending),
            QueryState::Planned | QueryState::Registered | QueryState::Running => {
                let fragments = catalog.query.get_fragments(query.id).await.unwrap();
                match query.current_state {
                    QueryState::Planned => State::Planned(Planned { fragments }),
                    QueryState::Registered => State::Registered(Registered { fragments }),
                    QueryState::Running => State::Running(Running { fragments }),
                    _ => unreachable!(),
                }
            }
            QueryState::Completed | QueryState::Stopped | QueryState::Failed => {
                panic!("Terminal state should not be reconciled")
            }
        }
    }
}

impl From<Planned> for State {
    fn from(s: Planned) -> Self {
        State::Planned(s)
    }
}

impl From<Registered> for State {
    fn from(s: Registered) -> Self {
        State::Registered(s)
    }
}

impl From<Running> for State {
    fn from(s: Running) -> Self {
        State::Running(s)
    }
}

impl From<Completed> for State {
    fn from(_: Completed) -> Self {
        State::Completed
    }
}

pub trait Transition: Sized {
    type Next: Into<State>;
    type Error: Into<QueryError>;

    async fn advance(&mut self, ctx: &mut QueryContext) -> Result<Self::Next, Self::Error>;
    async fn cleanup(self, ctx: &mut QueryContext, mode: StopMode);
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
        let futs = fragments.iter().map(|f| {
            let registry = self.worker_registry.clone();
            let addr = f.grpc_addr.clone();
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
                            let (rx, rpc) = mk_rpc(f.id);
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
        future::join_all(futs).await
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

    /// Map RPC results to fragment updates, persist them, and bail if the query reached a
    /// terminal state (i.e. at least one fragment failed).
    pub(crate) async fn apply_rpc_results(
        &mut self,
        fragments: &[fragment::Model],
        results: Vec<Result<(), WorkerError>>,
        success_state: FragmentState,
    ) -> anyhow::Result<()> {
        let updates = fragments
            .iter()
            .zip(results)
            .map(|(f, r)| match r {
                Ok(_) => FragmentUpdate {
                    id: f.id,
                    state: success_state,
                    ..Default::default()
                },
                Err(e) => {
                    let error = FragmentError::from(e);
                    warn!(fragment_id = f.id, %error, "Fragment failed during RPC");
                    FragmentUpdate {
                        id: f.id,
                        state: FragmentState::Failed,
                        error: Some(error),
                        ..Default::default()
                    }
                }
            })
            .collect();
        self.query = self
            .catalog
            .query
            .update_fragment_states(self.query.id, updates)
            .await?;
        if self.query.current_state.is_terminal() {
            anyhow::bail!("Transition to {success_state} failed");
        }
        Ok(())
    }

    pub(crate) async fn cleanup_stop(&self, mode: StopMode, fragments: &[fragment::Model]) {
        for e in self
            .stop_fragments(mode, fragments)
            .await
            .into_iter()
            .filter_map(Result::err)
        {
            error!("Failed to stop fragment: {e}");
        }
    }

    pub(crate) async fn cleanup_unregister(&self, fragments: &[fragment::Model]) {
        for e in self
            .unregister_fragments(fragments)
            .await
            .into_iter()
            .filter_map(Result::err)
        {
            error!("Failed to unregister fragment: {e}");
        }
    }

    pub(crate) async fn persist_stopped(&self) {
        if let Err(e) = self
            .catalog
            .query
            .set_query_state(&self.query, |_| QueryState::Stopped)
            .await
        {
            error!("Failed to set query to Stopped: {e}");
        }
    }

    pub(crate) async fn persist_failed(&self, error: QueryError) {
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
) -> State {
    tokio::select! {
        result = state.advance(ctx) => match result {
            Ok(next) => next.into(),
            Err(e) => {
                state.cleanup(ctx, StopMode::Forceful).await;
                if ctx.query.error.is_none() {
                    let query_error: QueryError = e.into();
                    warn!("Transition failed: {query_error:?}");
                    ctx.persist_failed(query_error).await;
                }
                State::Failed
            }
        },
        Ok(mode) = stop_rx.recv_async() => {
            info!(?mode, "Stopping query");
            state.cleanup(ctx, mode).await;
            ctx.persist_stopped().await;
            State::Stopped
        }
    }
}

pub struct QueryReconciler;

impl QueryReconciler {
    pub async fn run(mut ctx: QueryContext, mut stop_rx: flume::Receiver<StopMode>) {
        info!("Starting reconciliation");
        let mut state = State::from_current(&ctx.query, &ctx.catalog).await;
        loop {
            state = match state {
                State::Pending(s) => try_transition(s, &mut ctx, &mut stop_rx).await,
                State::Planned(s) => try_transition(s, &mut ctx, &mut stop_rx).await,
                State::Registered(s) => try_transition(s, &mut ctx, &mut stop_rx).await,
                State::Running(s) => try_transition(s, &mut ctx, &mut stop_rx).await,
                terminal => {
                    let label = match terminal {
                        State::Completed => "Completed",
                        State::Stopped => "Stopped",
                        State::Failed => "Failed",
                        _ => unreachable!(),
                    };
                    info!("Reconciliation finished: {label}");
                    break;
                }
            };
        }
    }
}
