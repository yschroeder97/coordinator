use crate::query::planned::Planned;
use crate::query::reconciler::{QueryContext, State, Transition};
use model::query::fragment::CreateFragment;
use model::query::*;
#[cfg(feature = "testing")]
use model::testing;
use model::worker;
use model::worker::GetWorker;
use tracing::info;

pub struct Pending;

impl Transition for Pending {
    async fn transition(self, mut ctx: QueryContext) -> State {
        info!("Planning query {}", ctx.query.id);
        // Query is already there

        // Fetch the workers required for planning
        let workers: Vec<worker::Model> = ctx
            .catalog
            .worker
            .get_worker(GetWorker::all())
            .await
            .unwrap();

        #[cfg(feature = "testing")]
        let requests = arb_create_fragments(ctx.query, workers);
        #[cfg(not(feature = "testing"))]
        let requests: Vec<CreateFragment> = Vec::new();

        let fragments = ctx.catalog.query.create_fragments(requests).await.unwrap();

        ctx.query = ctx
            .catalog
            .query
            .advance_query_state(ctx.query)
            .await
            .unwrap();

        State::Planned(Planned { fragments })
    }
}
