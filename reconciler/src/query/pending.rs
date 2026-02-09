use crate::query::planned::Planned;
use crate::query::reconciler::{QueryContext, State, Transition};
use model::query::*;
#[cfg(feature = "testing")]
use model::testing;
use tracing::info;

pub struct Pending {
    pending_query: active_query::Model,
}

impl Pending {
    pub fn new(pending_query: active_query::Model) -> Self {
        Pending { pending_query }
    }
}

impl Transition for Pending {
    async fn transition(self, ctx: QueryContext) -> State {
        info!("Planning query {}", self.pending_query.id);

        #[cfg(feature = "testing")]
        let fragments = arb_create_fragments();
        #[cfg(not(feature = "testing"))]
        let fragments: Vec<fragment::Model> = Vec::new();

        State::Planned(Planned {
            planned_query: ctx
                .query_catalog
                .advance_query_state(self.pending_query)
                .await
                .unwrap(),
            fragments,
        })
    }
}
