use crate::catalog::query::fragment::{FragmentState, GetFragment, QueryFragment};
use crate::catalog::query::query_catalog::QueryCatalog;
use crate::catalog::query::{ActiveQuery, StopMode};
use crate::cluster_controller::worker_client::{
    FragmentError, GetFragmentStatusRequest, QueryStatusReply, Rpc,
};
use crate::cluster_controller::worker_registry::{WorkerCommunicationError, WorkerRegistryHandle};
use crate::query_controller::query_reconciler::{Query, Transition};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::info;

const QUERY_POLLING_INTERVAL: Duration = Duration::from_secs(5);

/// Wrapper to make FragmentError implement std::error::Error
#[derive(Debug)]
pub struct FragmentErrorWrapper(pub FragmentError);

impl fmt::Display for FragmentErrorWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] {} (at {})\n{}",
            self.0.code, self.0.message, self.0.location, self.0.stack_trace
        )
    }
}

impl std::error::Error for FragmentErrorWrapper {}

pub struct Running {
    pub fragments: Vec<QueryFragment>,
}

impl Query<Running> {
    pub async fn resume(
        query: ActiveQuery,
        query_catalog: Arc<QueryCatalog>,
        worker_registry: WorkerRegistryHandle,
    ) -> Query<Running> {
        let fragments = query_catalog
            .get_fragments(&GetFragment::new().of_query(query.id.clone()))
            .await
            .unwrap();

        Query {
            id: query.id,
            query_catalog,
            worker_registry,
            state: Running { fragments },
        }
    }
}

/// Terminal state representing a completed query
pub struct Completed;

#[derive(Error, Debug)]
pub enum RunningError {
    #[error("RPC error while polling fragment status")]
    Rpc(Vec<WorkerCommunicationError>),
    #[error("Fragment failed")]
    FragmentFailed(Vec<FragmentErrorWrapper>),
}

impl Transition<Completed, RunningError> for Query<Running> {
    async fn try_transition(&mut self) -> Result<Completed, RunningError> {
        info!("Query {} is running, polling fragment status", self.id);

        loop {
            tokio::time::sleep(QUERY_POLLING_INTERVAL).await;

            let fragment_updates = self.poll_fragment_status().await;

            // Partition results into successes and errors
            let (successes, errors): (Vec<_>, Vec<_>) =
                fragment_updates.into_iter().partition(|r| r.is_ok());

            // Check for RPC errors
            let rpc_errors: Vec<_> = errors.into_iter().filter_map(|r| r.err()).collect();
            if !rpc_errors.is_empty() {
                return Err(RunningError::Rpc(rpc_errors));
            }

            let replies: Vec<_> = successes.into_iter().filter_map(|r| r.ok()).collect();

            // Check for failed fragments
            let fragment_errors: Vec<_> = replies
                .iter()
                .filter(|reply| FragmentState::from(reply.state) == FragmentState::Failed)
                .filter_map(|reply| {
                    reply
                        .metrics
                        .as_ref()
                        .and_then(|m| m.error.clone())
                        .map(FragmentErrorWrapper)
                })
                .collect();

            if !fragment_errors.is_empty() {
                return Err(RunningError::FragmentFailed(fragment_errors));
            }

            // Check if all fragments are stopped (completed)
            let all_stopped = replies
                .iter()
                .all(|reply| FragmentState::from(reply.state) == FragmentState::Stopped);

            if all_stopped {
                info!("All fragments stopped, query {} completed", self.id);
                return Ok(Completed);
            }
        }
    }

    async fn on_transition_ok(self, completed: Completed) -> Query<Completed> {
        let _ = self.query_catalog.move_to_terminated(&self.id).await;
        self.transition_to(completed)
    }

    async fn on_transition_stopped(self, stop_mode: StopMode) {
        info!("Stopping: stopping and unregistering fragments");
        match stop_mode {
            StopMode::Graceful => {
                // TODO: only stop the fragments of sources and wait for the query to terminate itself
            }
            StopMode::Forceful => {
                let _ = self.stop_fragments(stop_mode, &self.state.fragments).await;
            }
        }
        let _ = self.unregister_fragments(&self.state.fragments).await;
        let _ = self.query_catalog.move_to_terminated(&self.id).await;
    }

    async fn on_transition_failed(self, _error: RunningError) {
        let _ = self
            .stop_fragments(StopMode::Forceful, &self.state.fragments)
            .await;
        let _ = self.unregister_fragments(&self.state.fragments).await;
        let _ = self.query_catalog.move_to_terminated(&self.id).await;
    }
}

impl Query<Running> {
    async fn poll_fragment_status(&self) -> Vec<Result<QueryStatusReply, WorkerCommunicationError>> {
        self.broadcast_rpc(&self.state.fragments, |id| {
            let (rx, req) = GetFragmentStatusRequest::new(id);
            (rx, Rpc::GetFragmentStatus(req))
        })
        .await
    }
}
