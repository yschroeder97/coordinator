use crate::query::context::QueryContext;
use crate::query::lifecycle::registered::Registered;
use crate::query::reconciler::Transition;
use fail::fail_point;
use model::query::StopMode;
use model::query::fragment;
use model::query::query_state::QueryState;

pub(crate) struct Planned {
    pub(crate) fragments: Vec<fragment::Model>,
}

impl Transition for Planned {
    type Next = Registered;
    const STATE: QueryState = QueryState::Planned;

    async fn transition(&mut self, ctx: &mut QueryContext) -> anyhow::Result<Registered> {
        fail_point!("reconciler_pre_register");
        let results = ctx.register_fragments(&self.fragments).await;
        Ok(Registered {
            fragments: ctx.apply_rpc_results(&self.fragments, results).await?,
        })
    }

    async fn rollback(self, ctx: &mut QueryContext, _mode: StopMode) {
        ctx.rollback_unregister(&self.fragments).await;
    }
}
