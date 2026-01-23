use crate::catalog::query::fragment::{CreateQueryFragment, FragmentId, QueryFragment};
use crate::catalog::query::query_catalog::QueryCatalogErr;
use crate::catalog::query::{MarkQuery, StopMode};
use crate::query_controller::planned_query::Planned;
use crate::catalog::query::query_state::QueryState;
use crate::query_controller::query_reconciler::{Query, Transition};
use thiserror::Error;
use tracing::info;

pub struct Pending {
    sql_stmt: String,
}

impl Pending {
    pub fn from(sql: String) -> Self {
        Pending { sql_stmt: sql }
    }
}

#[derive(Error, Debug)]
pub enum PlanningError {
    #[error("Error during Planning")]
    Planner,
    #[error("Error in Query Catalog")]
    QueryCatalog(#[from] QueryCatalogErr),
}

impl Transition<Planned, PlanningError> for Query<Pending> {
    async fn try_transition(&mut self) -> Result<Planned, PlanningError> {
        info!("Planning query {}", self.id);

        // TODO: Actually parse and plan the SQL statement
        let fragments: Vec<CreateQueryFragment> = tokio::task::spawn_blocking(Vec::new)
            .await
            .map_err(|_| PlanningError::Planner)?;

        let ids: Vec<FragmentId> = self
            .query_catalog
            .insert_fragments(&fragments)
            .await
            .map_err(PlanningError::QueryCatalog)?;

        let fragments_with_id: Vec<QueryFragment> = fragments
            .into_iter()
            .zip(ids)
            .map(|(f, id)| QueryFragment {
                id,
                query_id: f.query_id,
                host_name: f.host_name,
                grpc_port: f.grpc_port,
                current_state: f.current_state,
                desired_state: f.desired_state,
                plan: f.plan,
                used_capacity: f.used_capacity,
            })
            .collect();

        Ok(Planned {
            fragments: fragments_with_id,
        })
    }

    async fn on_transition_ok(self, planned: Planned) -> Query<Planned> {
        self.query_catalog
            .move_to_next_state(&MarkQuery::new(self.id.clone(), QueryState::Planned))
            .await
            .unwrap();

        self.transition_to(planned)
    }

    async fn on_transition_stopped(self, _stop_mode: StopMode) {
        let _ = self.query_catalog.move_to_terminated(&self.id).await;
    }

    async fn on_transition_failed(self, _error: PlanningError) {
        let _ = self.query_catalog.move_to_terminated(&self.id).await;
    }
}
