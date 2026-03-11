use crate::query::Completed;
use crate::query::context::QueryContext;
use crate::query::lifecycle::pending::Pending;
use crate::query::lifecycle::planned::Planned;
use crate::query::lifecycle::registered::Registered;
use crate::query::lifecycle::running::Running;
use catalog::Catalog;
use madsim::buggify::buggify;
use model::query;
use model::query::StopMode;
use model::query::query_state::QueryState;
use std::time::Duration;
use tracing::{debug, info, info_span, warn};

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

impl From<Pending> for QueryStateInternal {
    fn from(s: Pending) -> Self {
        QueryStateInternal::Pending(s)
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

pub(crate) trait Transition: Sized {
    type Next: Into<QueryStateInternal>;

    async fn transition(&mut self, ctx: &mut QueryContext) -> anyhow::Result<Self::Next>;
    async fn rollback(self, ctx: &mut QueryContext, mode: StopMode);
}

async fn try_transition<T: Transition>(
    mut state: T,
    ctx: &mut QueryContext,
    stop_rx: &mut flume::Receiver<StopMode>,
    span: &tracing::Span,
) -> QueryStateInternal {
    if buggify() {
        panic!("buggify: reconciler_try_transition_panic");
    }
    let from_state = ctx.query.current_state;
    tokio::select! {
        result = state.transition(ctx) => {
            let _guard = span.enter();
            match result {
                Ok(next) => {
                    let next = next.into();
                    debug!(from = %from_state, to = %QueryState::from(&next), "transition succeeded");
                    next
                }
                Err(e) => {
                    drop(_guard);
                    state.rollback(ctx, StopMode::Forceful).await;
                    let _guard = span.enter();
                    let root = e.root_cause().to_string();
                    warn!(from = %from_state, "transition failed: {root}");
                    if ctx.query.error.is_none() {
                        ctx.persist_failed(root).await;
                    }
                    QueryStateInternal::Failed
                }
            }
        },
        Ok(mode) = stop_rx.recv_async() => {
            if buggify() {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            let _guard = span.enter();
            info!(?mode, "Stopping query");
            drop(_guard);
            state.rollback(ctx, mode).await;
            ctx.persist_stopped().await;
            QueryStateInternal::Stopped
        }
    }
}

pub(crate) struct QueryReconciler;

impl QueryReconciler {
    pub(crate) async fn run(mut ctx: QueryContext, mut stop_rx: flume::Receiver<StopMode>) {
        let span = info_span!(
            "query",
            id = ctx.query.id,
            name = %ctx.query.name,
        );
        {
            let _guard = span.enter();
            info!("Starting reconciliation");
        }
        let mut state = QueryStateInternal::from_current(&ctx.query, &ctx.catalog).await;
        loop {
            state = match state {
                QueryStateInternal::Pending(pending) => {
                    try_transition(pending, &mut ctx, &mut stop_rx, &span).await
                }
                QueryStateInternal::Planned(planned) => {
                    try_transition(planned, &mut ctx, &mut stop_rx, &span).await
                }
                QueryStateInternal::Registered(registered) => {
                    try_transition(registered, &mut ctx, &mut stop_rx, &span).await
                }
                QueryStateInternal::Running(running) => {
                    try_transition(running, &mut ctx, &mut stop_rx, &span).await
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
