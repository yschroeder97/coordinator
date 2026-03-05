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
use tracing::{info, warn};

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

    async fn transition(&mut self, ctx: &mut QueryContext) -> anyhow::Result<Self::Next>;
    async fn rollback(self, ctx: &mut QueryContext, mode: StopMode);
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
                QueryStateInternal::Registered(s) => {
                    try_transition(s, &mut ctx, &mut stop_rx).await
                }
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
