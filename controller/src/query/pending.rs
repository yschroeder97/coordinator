use crate::query::planned::Planned;
use crate::query::reconciler::{QueryContext, Transition};
use model::query::fragment::CreateFragment;
use model::query::*;
use tracing::info;

pub struct Pending;

impl Transition for Pending {
    type Next = Planned;
    type Error = anyhow::Error;

    async fn advance(&mut self, ctx: &mut QueryContext) -> Result<Planned, Self::Error> {
        info!("Planning");

        #[cfg(feature = "testing")]
        let requests = {
            use model::worker::GetWorker;
            let workers = ctx.catalog.worker.get_worker(GetWorker::all()).await?;
            model::testing::arb_create_fragments(&ctx.query, &workers)
        };
        #[cfg(not(feature = "testing"))]
        let requests: Vec<CreateFragment> = Vec::new();

        let (query, fragments) = ctx
            .catalog
            .query
            .create_fragments(&ctx.query, requests)
            .await?;
        ctx.query = query;
        Ok(Planned { fragments })
    }

    async fn cleanup(self, _ctx: &mut QueryContext, _mode: StopMode) {}
}
