use crate::query::context::QueryContext;
use crate::query::lifecycle::running::Running;
use crate::query::query_task::Transition;
use fail::fail_point;
use model::query::StopMode;
use model::query::fragment;

pub(crate) struct Registered {
    pub(crate) fragments: Vec<fragment::Model>,
}

impl Transition for Registered {
    type Next = Running;

    async fn transition(&mut self, ctx: &mut QueryContext) -> anyhow::Result<Running> {
        fail_point!("reconciler_pre_start");
        Ok(Running {
            fragments: ctx
                .apply_rpc_results(&self.fragments, ctx.start_fragments(&self.fragments).await)
                .await?,
        })
    }

    async fn rollback(self, ctx: &mut QueryContext, mode: StopMode) {
        ctx.rollback_stop(mode, &self.fragments).await;
        ctx.rollback_unregister(&self.fragments).await;
    }
}
