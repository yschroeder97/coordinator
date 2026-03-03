use crate::query::context::QueryContext;
use crate::query::lifecycle::running::Running;
use crate::query::reconciler::Transition;
use model::query::StopMode;
use model::query::fragment;
use model::query::query_state::QueryState;

pub(crate) struct Registered {
    pub(crate) fragments: Vec<fragment::Model>,
}

impl Transition for Registered {
    type Next = Running;
    const STATE: QueryState = QueryState::Registered;

    async fn transition(&mut self, ctx: &mut QueryContext) -> anyhow::Result<Running> {
        let results = ctx.start_fragments(&self.fragments).await;
        Ok(Running {
            fragments: ctx.apply_rpc_results(&self.fragments, results).await?,
        })
    }

    async fn rollback(self, ctx: &mut QueryContext, mode: StopMode) {
        ctx.rollback_stop(mode, &self.fragments).await;
        ctx.rollback_unregister(&self.fragments).await;
    }
}
