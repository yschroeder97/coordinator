use crate::query::context::QueryContext;
use crate::query::lifecycle::planned::Planned;
use crate::query::query_task::Transition;
use madsim::buggify::buggify;
use model::query::StopMode;

pub(crate) struct Pending;

impl Transition for Pending {
    type Next = Planned;

    async fn transition(&mut self, ctx: &mut QueryContext) -> anyhow::Result<Planned> {
        #[cfg(feature = "testing")]
        let requests = {
            use model::worker::GetWorker;
            let workers = ctx.catalog.worker.get_worker(GetWorker::all()).await?;
            model::query::fragment::CreateFragment::for_models(&ctx.query, &workers)
        };
        #[cfg(not(feature = "testing"))]
        let requests: Vec<model::query::fragment::CreateFragment> = Vec::new();

        if buggify() {
            return Err(anyhow::anyhow!("buggify: pending_create_fragments"));
        }
        let (query, fragments) = ctx
            .catalog
            .query
            .create_fragments(&ctx.query, requests)
            .await?;
        ctx.query = query;
        if buggify() && fragments.is_empty() {
            panic!("buggify: pending_corrupt_fragment_count");
        }
        Ok(Planned { fragments })
    }

    async fn rollback(self, _ctx: &mut QueryContext, _mode: StopMode) {}
}