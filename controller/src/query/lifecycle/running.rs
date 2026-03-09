use crate::worker::worker_client::{
    FragmentError as ProtoError, GetFragmentStatusRequest, QueryStatusReply, Rpc,
};
use crate::worker::worker_registry::WorkerError;
use crate::query::Completed;
use crate::query::context::QueryContext;
use crate::query::query_task::Transition;
use crate::query::retry::RetryPolicy;
use common::error::Retryable;
use model::Set;
use model::query::StopMode;
use model::query::fragment::{self, FragmentError, FragmentState};
use model::query::query_state::QueryState;
use std::time::Duration;
use tracing::{info, warn};

const QUERY_POLL_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) struct Running {
    pub(crate) fragments: Vec<fragment::Model>,
}

impl From<&ProtoError> for FragmentError {
    fn from(err: &ProtoError) -> Self {
        FragmentError::WorkerInternal {
            code: err.code,
            msg: err.message.clone(),
            trace: err.stack_trace.clone(),
        }
    }
}

fn unix_ms_to_datetime(ms: u64) -> chrono::DateTime<chrono::Local> {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .unwrap_or_default()
        .with_timezone(&chrono::Local)
}

impl Running {
    async fn poll_fragment_status(
        &self,
        ctx: &QueryContext,
    ) -> Vec<Result<QueryStatusReply, WorkerError>> {
        ctx.broadcast(
            &self.fragments,
            |id| {
                let (rx, req) = GetFragmentStatusRequest::new(id);
                (rx, Rpc::GetFragmentStatus(req))
            },
            &RetryPolicy::Transition,
        )
        .await
    }
}

fn apply_status_reply(
    fragment: &fragment::Model,
    reply: QueryStatusReply,
) -> Option<fragment::ActiveModel> {
    let state = match FragmentState::try_from(reply.state) {
        Ok(state) => state,
        Err(tag) => {
            warn!("Fragment {} returned unknown state tag {tag}", fragment.id);
            return None;
        }
    };
    let start_timestamp = reply
        .metrics
        .as_ref()
        .and_then(|m| m.start_unix_time_in_ms)
        .map(unix_ms_to_datetime);
    let stop_timestamp = reply
        .metrics
        .as_ref()
        .and_then(|m| m.stop_unix_time_in_ms)
        .map(unix_ms_to_datetime);
    let error = if state == FragmentState::Failed {
        let err = reply
            .metrics
            .as_ref()
            .and_then(|m| m.error.as_ref())
            .map(FragmentError::from)
            .unwrap_or_else(|| FragmentError::WorkerCommunication {
                msg: "Fragment failed without error details".to_string(),
            });
        warn!(fragment_id = fragment.id, %err, "Fragment failed");
        Some(err)
    } else {
        None
    };

    let mut am: fragment::ActiveModel = fragment.clone().into();
    am.current_state = Set(state);
    if let Some(ts) = start_timestamp {
        am.start_timestamp = Set(Some(ts));
    }
    if let Some(ts) = stop_timestamp {
        am.stop_timestamp = Set(Some(ts));
    }
    if let Some(err) = error {
        am.error = Set(Some(err));
    }
    Some(am)
}

impl Transition for Running {
    type Next = Completed;

    async fn transition(&mut self, ctx: &mut QueryContext) -> anyhow::Result<Completed> {
        loop {
            let results = self.poll_fragment_status(ctx).await;

            let updates: Vec<_> = self
                .fragments
                .iter()
                .zip(results)
                .filter_map(|(fragment, rpc_result)| match rpc_result {
                    Ok(reply) => apply_status_reply(fragment, reply),
                    Err(e) if e.retryable() => {
                        warn!("Failed to poll fragment {} status: {e}", fragment.id);
                        None
                    }
                    Err(e) => {
                        let error = FragmentError::from(e);
                        warn!(fragment_id = fragment.id, %error, "Fragment poll failed permanently");
                        let mut am: fragment::ActiveModel = fragment.clone().into();
                        am.current_state = Set(FragmentState::Failed);
                        am.error = Set(Some(error));
                        Some(am)
                    }
                })
                .collect();

            match ctx
                .catalog
                .query
                .update_fragment_states(ctx.query.id, updates)
                .await
            {
                Ok((query, fragments)) => {
                    ctx.query = query;
                    self.fragments = fragments;
                }
                Err(e) => {
                    warn!("Failed to update fragment states: {e}");
                    continue;
                }
            }

            if ctx.query.current_state.is_terminal() {
                if ctx.query.current_state == QueryState::Failed {
                    return Err(anyhow::anyhow!("Query failed during execution"));
                }
                info!("All fragments completed");
                return Ok(Completed);
            }

            // Poll again in 5s
            tokio::time::sleep(QUERY_POLL_INTERVAL).await;
        }
    }

    async fn rollback(self, ctx: &mut QueryContext, mode: StopMode) {
        ctx.rollback_stop(mode, &self.fragments).await;
        ctx.rollback_unregister(&self.fragments).await;
    }
}
