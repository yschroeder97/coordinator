use crate::cluster::worker_registry::{WorkerCommunicationError, WorkerRegistryHandle};
use crate::query::reconciler::{Query, Transition};
use crate::query::running::Running;
use catalog::query_catalog::QueryCatalog;
use model::query::query_state::QueryState;
use model::query::{active_query, fragment, StopMode};
use std::sync::Arc;
use thiserror::Error;
use tracing::info;

pub struct Registered {
    pub registered_query: active_query::Model,
    pub fragments: Vec<fragment::Model>,
}

impl Query<Registered> {
    pub async fn resume(
        query: ActiveQuery,
        query_catalog: Arc<QueryCatalog>,
        worker_registry: WorkerRegistryHandle,
    ) -> Query<Registered> {
        let fragments = query_catalog
            .get_fragments(&GetFragment::new().of_query(query.id.clone()))
            .await
            .unwrap();

        Query {
            id: query.id,
            query_catalog,
            worker_registry,
            state: Registered { fragments },
        }
    }
}

#[derive(Error, Debug)]
pub enum StartingError {
    #[error("RPC error during fragment start")]
    Rpc(Vec<WorkerCommunicationError>),
}

impl Transition<Running, StartingError> for Query<Registered> {
    async fn try_transition(&mut self) -> Result<Running, StartingError> {
        info!("Starting fragments for query {}", self.id);

        let results = self.start_fragments(&self.state.fragments).await;

        let errors: Vec<_> = results.into_iter().filter_map(|r| r.err()).collect();
        if !errors.is_empty() {
            return Err(StartingError::Rpc(errors));
        }

        Ok(Running {
            fragments: std::mem::take(&mut self.state.fragments),
        })
    }

    async fn on_transition_ok(self, running: Running) -> Query<Running> {
        self.query_catalog
            .move_to_next_state(&MarkQuery::new(self.id.clone(), QueryState::Running))
            .await
            .unwrap();

        self.transition_to(running)
    }

    async fn on_transition_stopped(self, stop_mode: StopMode) {
        info!("Stopping: stopping and unregistering fragments");
        let _ = self.stop_fragments(stop_mode, &self.state.fragments).await;
        let _ = self.unregister_fragments(&self.state.fragments).await;
        let _ = self
            .query_catalog
            .move_to_next_state(&MarkQuery::stopped(self.id, stop_mode))
            .await;
    }

    async fn on_transition_failed(self, _error: StartingError) {
        let _ = self
            .stop_fragments(StopMode::Forceful, &self.state.fragments)
            .await;
        let _ = self.unregister_fragments(&self.state.fragments).await;
        // TODO: Convert StartingError to QueryError for better error tracking
        let _ = self
            .query_catalog
            .move_to_next_state(&MarkQuery::failed(self.id, None))
            .await;
    }
}
