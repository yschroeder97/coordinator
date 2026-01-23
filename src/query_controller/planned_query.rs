use crate::catalog::query::fragment::{GetFragment, QueryFragment};
use crate::catalog::query::query_catalog::{QueryCatalog, QueryCatalogErr};
use crate::catalog::query::query_state::QueryState;
use crate::catalog::query::{ActiveQuery, MarkQuery, StopMode};
use crate::cluster_controller::worker_registry::{WorkerCommunicationError, WorkerRegistryHandle};
use crate::query_controller::query_reconciler::{Query, Transition};
use crate::query_controller::registered_query::Registered;
use std::sync::Arc;
use thiserror::Error;
use tracing::info;

pub struct Planned {
    pub fragments: Vec<QueryFragment>,
}

#[derive(Error, Debug)]
pub enum RegisteringError {
    #[error("RPC error during fragment registration")]
    Rpc(Vec<WorkerCommunicationError>),
    #[error("Error in Query Catalog")]
    QueryCatalog(#[from] QueryCatalogErr),
}

impl Query<Planned> {
    pub async fn resume(
        query: ActiveQuery,
        query_catalog: Arc<QueryCatalog>,
        worker_registry: WorkerRegistryHandle,
    ) -> Query<Planned> {
        let fragments = query_catalog
            .get_fragments(&GetFragment::new().of_query(query.id.clone()))
            .await
            .unwrap();

        Query {
            id: query.id,
            query_catalog,
            worker_registry,
            state: Planned { fragments },
        }
    }
}

impl Transition<Registered, RegisteringError> for Query<Planned> {
    async fn try_transition(&mut self) -> Result<Registered, RegisteringError> {
        info!("Registering fragments for query {}", self.id);

        let results = self.register_fragments(&self.state.fragments).await;

        let errors: Vec<_> = results.into_iter().filter_map(|r| r.err()).collect();
        if !errors.is_empty() {
            return Err(RegisteringError::Rpc(errors));
        }

        Ok(Registered {
            fragments: std::mem::take(&mut self.state.fragments),
        })
    }

    async fn on_transition_ok(self, registered: Registered) -> Query<Registered> {
        self.query_catalog
            .move_to_next_state(&MarkQuery::new(self.id.clone(), QueryState::Registered))
            .await
            .unwrap();

        self.transition_to(registered)
    }

    async fn on_transition_stopped(self, _stop_mode: StopMode) {
        info!("Stopping: unregistering fragments");
        let _ = self.unregister_fragments(&self.state.fragments).await;
        let _ = self.query_catalog.move_to_terminated(&self.id).await;
    }

    async fn on_transition_failed(self, _error: RegisteringError) {
        let _ = self.unregister_fragments(&self.state.fragments).await;
        let _ = self.query_catalog.move_to_terminated(&self.id).await;
    }
}
