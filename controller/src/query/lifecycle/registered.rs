use crate::query::context::QueryContext;
use crate::query::lifecycle::running::Running;
use crate::query::query_task::Transition;
use madsim::buggify::buggify;
use model::query::StopMode;
use model::query::fragment;

pub(crate) struct Registered {
    pub(crate) fragments: Vec<fragment::Model>,
}

impl Transition for Registered {
    type Next = Running;

    async fn transition(&mut self, ctx: &mut QueryContext) -> anyhow::Result<Running> {
        if buggify() {
            return Err(anyhow::anyhow!("buggify: registered_pre_start"));
        }
        Ok(Running {
            fragments: ctx
                .apply_rpc_results(&self.fragments, ctx.start_fragments(&self.fragments).await)
                .await?,
        })
    }

    async fn rollback(self, ctx: &mut QueryContext, mode: StopMode) {
        if !buggify() {
            ctx.rollback_stop(mode, &self.fragments).await;
        }
    }
}
