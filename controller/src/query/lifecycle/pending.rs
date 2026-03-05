use crate::query::context::QueryContext;
use crate::query::lifecycle::planned::Planned;
use crate::query::reconciler::Transition;
use model::query::fragment::CreateFragment;
use model::query::StopMode;
use tracing::info;

pub struct Pending;

impl Transition for Pending {
    type Next = Planned;

    async fn transition(&mut self, ctx: &mut QueryContext) -> anyhow::Result<Planned> {
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

    async fn rollback(self, _ctx: &mut QueryContext, _mode: StopMode) {}
}
