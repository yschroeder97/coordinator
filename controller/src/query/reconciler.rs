use crate::query::Completed;
use crate::query::context::QueryContext;
use crate::query::lifecycle::pending::Pending;
use crate::query::lifecycle::planned::Planned;
use crate::query::lifecycle::registered::Registered;
use crate::query::lifecycle::running::Running;
use catalog::Catalog;
use model::query;
use model::query::StopMode;
use model::query::query_state::QueryState;
use tracing::{info, info_span, warn};

pub(crate) enum QueryStateInternal {
    Pending(Pending),
    Planned(Planned),
    Registered(Registered),
    Running(Running),
    Completed,
    Stopped,
    Failed,
}

impl From<QueryStateInternal> for QueryState {
    fn from(value: QueryStateInternal) -> Self {
        QueryState::from(&value)
    }
}

impl From<&QueryStateInternal> for QueryState {
    fn from(value: &QueryStateInternal) -> Self {
        match value {
            QueryStateInternal::Pending(_) => QueryState::Pending,
            QueryStateInternal::Planned(_) => QueryState::Planned,
            QueryStateInternal::Registered(_) => QueryState::Registered,
            QueryStateInternal::Running(_) => QueryState::Running,
            QueryStateInternal::Completed => QueryState::Completed,
            QueryStateInternal::Stopped => QueryState::Stopped,
            QueryStateInternal::Failed => QueryState::Failed,
        }
    }
}

impl QueryStateInternal {
    async fn from_current(query: &query::Model, catalog: &Catalog) -> Self {
        match query.current_state {
            QueryState::Pending => QueryStateInternal::Pending(Pending),
            QueryState::Planned | QueryState::Registered | QueryState::Running => {
                let fragments = catalog.query.get_fragments(query.id).await.unwrap();
                match query.current_state {
                    QueryState::Planned => QueryStateInternal::Planned(Planned { fragments }),
                    QueryState::Registered => {
                        QueryStateInternal::Registered(Registered { fragments })
                    }
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

    const STATE: QueryState;

    async fn transition(&mut self, ctx: &mut QueryContext) -> anyhow::Result<Self::Next>;
    async fn rollback(self, ctx: &mut QueryContext, mode: StopMode);
}

async fn try_transition<T: Transition>(
    mut state: T,
    ctx: &mut QueryContext,
    stop_rx: &mut flume::Receiver<StopMode>,
    span: &tracing::Span,
) -> QueryStateInternal {
    tokio::select! {
        result = state.transition(ctx) => {
            let _guard = span.enter();
            match result {
                Ok(next) => {
                    let next = next.into();
                    info!(from = %T::STATE, to = %QueryState::from(&next), "Transition succeeded");
                    next
                }
                Err(e) => {
                    drop(_guard);
                    state.rollback(ctx, StopMode::Forceful).await;
                    let _guard = span.enter();
                    if ctx.query.error.is_none() {
                        warn!("Transition failed: {e:#}");
                        ctx.persist_failed(format!("{e:#}")).await;
                    }
                    QueryStateInternal::Failed
                }
            }
        },
        Ok(mode) = stop_rx.recv_async() => {
            let _guard = span.enter();
            info!(?mode, "Stopping query");
            drop(_guard);
            state.rollback(ctx, mode).await;
            ctx.persist_stopped().await;
            QueryStateInternal::Stopped
        }
    }
}

pub struct QueryReconciler;

impl QueryReconciler {
    pub async fn run(mut ctx: QueryContext, mut stop_rx: flume::Receiver<StopMode>) {
        let span = info_span!(
            "query",
            id = ctx.query.id,
            name = %ctx.query.name,
            statement = %ctx.query.statement,
        );
        {
            let _guard = span.enter();
            info!("Starting reconciliation");
        }
        let mut state = QueryStateInternal::from_current(&ctx.query, &ctx.catalog).await;
        loop {
            state = match state {
                QueryStateInternal::Pending(s) => {
                    try_transition(s, &mut ctx, &mut stop_rx, &span).await
                }
                QueryStateInternal::Planned(s) => {
                    try_transition(s, &mut ctx, &mut stop_rx, &span).await
                }
                QueryStateInternal::Registered(s) => {
                    try_transition(s, &mut ctx, &mut stop_rx, &span).await
                }
                QueryStateInternal::Running(s) => {
                    try_transition(s, &mut ctx, &mut stop_rx, &span).await
                }
                terminal => {
                    let _guard = span.enter();
                    info!("Reconciliation finished: {}", QueryState::from(terminal));
                    break;
                }
            };
        }
    }
}
