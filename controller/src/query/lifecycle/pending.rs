use crate::query::context::QueryContext;
use crate::query::lifecycle::planned::Planned;
use crate::query::reconciler::Transition;
use fail::fail_point;
use model::query::StopMode;
use model::query::query_state::QueryState;

pub(crate) struct Pending;

impl Transition for Pending {
    type Next = Planned;
    const STATE: QueryState = QueryState::Pending;

    async fn transition(&mut self, ctx: &mut QueryContext) -> anyhow::Result<Planned> {
        #[cfg(feature = "testing")]
        let requests = {
            use model::worker::GetWorker;
            let workers = ctx.catalog.worker.get_worker(GetWorker::all()).await?;
            model::testing::arb_create_fragments(&ctx.query, &workers)
        };
        #[cfg(not(feature = "testing"))]
        let requests: Vec<model::query::fragment::CreateFragment> = Vec::new();

        fail_point!("reconciler_create_fragments", |_| Err(anyhow::anyhow!(
            "injected: reconciler_create_fragments"
        )));
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
