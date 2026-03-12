use anyhow::Result;
use crate::query::context::QueryContext;
use crate::query::running;
use model::query::StopMode;
use model::query::fragment::FragmentState;
use model::query::query_state::QueryState;
use tracing::{debug, error, info, info_span, warn};

async fn plan_query(ctx: &mut QueryContext) -> Result<()> {
    #[cfg(feature = "testing")]
    let requests = {
        use model::query::fragment;
        use model::worker::GetWorker;
        let workers = ctx.catalog.worker.get_worker(GetWorker::all()).await?;
        fragment::CreateFragment::for_models(&ctx.query, &workers)
    };
    #[cfg(not(feature = "testing"))]
    let requests: Vec<model::query::fragment::CreateFragment> = Vec::new();

    let (query, fragments) = ctx
        .catalog
        .query
        .create_fragments(&ctx.query, requests)
        .await?;
    ctx.query = query;
    ctx.fragments = fragments;
    Ok(())
}

async fn transition(ctx: &mut QueryContext) -> Result<()> {
    match ctx.query.current_state {
        QueryState::Pending => plan_query(ctx).await,
        QueryState::Planned => {
            let results = ctx.register_fragments().await;
            ctx.apply_transition_results(results, FragmentState::Registered)
                .await
        }
        QueryState::Registered => {
            let results = ctx.start_fragments().await;
            ctx.apply_transition_results(results, FragmentState::Started)
                .await
        }
        QueryState::Running => running::poll_until_complete(ctx).await,
        _ => unreachable!(),
    }
}

async fn try_transition(
    ctx: &mut QueryContext,
    stop_rx: &mut flume::Receiver<StopMode>,
    span: &tracing::Span,
) -> bool {
    let from_state = ctx.query.current_state;
    tokio::select! {
        result = transition(ctx) => {
            let _guard = span.enter();
            match result {
                Ok(()) => {
                    debug!(from = %from_state, to = %ctx.query.current_state, "transition succeeded");
                    !ctx.query.current_state.is_terminal()
                }
                Err(e) => {
                    warn!(from = %from_state, "transition failed: {e:#}");
                    drop(_guard);
                    if from_state == QueryState::Pending {
                        if let Err(e) = ctx.catalog.query.fail_pending_query(ctx.query.clone(), e.to_string()).await {
                            error!("failed to set query to failed: {e}");
                        }
                    } else {
                        ctx.rollback_fragments(StopMode::Forceful).await;
                    }
                    false
                }
            }
        },
        Ok(mode) = stop_rx.recv_async() => {
            let _guard = span.enter();
            info!(?mode, "stopping query");
            drop(_guard);
            if from_state == QueryState::Pending {
                if let Err(e) = ctx.catalog.query.stop_pending_query(&ctx.query).await {
                    error!("failed to stop query: {e}");
                }
                false
            } else if mode == StopMode::Graceful && from_state == QueryState::Running {
                ctx.stop_source_fragments().await;
                !ctx.query.current_state.is_terminal()
            } else {
                ctx.rollback_fragments(mode).await;
                false
            }
        }
    }
}

pub(crate) async fn run(mut ctx: QueryContext, mut stop_rx: flume::Receiver<StopMode>) {
    let span = info_span!(
        "query",
        id = ctx.query.id,
        name = %ctx.query.name,
    );
    {
        let _guard = span.enter();
        info!("starting reconciliation");
    }
    if ctx.query.current_state != QueryState::Pending {
        ctx.fragments = ctx.catalog.query.get_fragments(ctx.query.id).await.unwrap();
    }
    while try_transition(&mut ctx, &mut stop_rx, &span).await {}
    {
        let _guard = span.enter();
        info!("reconciliation finished: {}", ctx.query.current_state);
    }
}
