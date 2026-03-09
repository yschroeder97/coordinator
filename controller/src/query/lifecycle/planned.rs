use crate::query::context::QueryContext;
use crate::query::lifecycle::registered::Registered;
use crate::query::query_task::Transition;
use madsim::buggify::buggify;
use model::query::StopMode;
use model::query::fragment;

pub(crate) struct Planned {
    pub(crate) fragments: Vec<fragment::Model>,
}

impl Transition for Planned {
    type Next = Registered;

    async fn transition(&mut self, ctx: &mut QueryContext) -> anyhow::Result<Registered> {
        if buggify() {
            return Err(anyhow::anyhow!("buggify: planned_pre_register"));
        }
        Ok(Registered {
            fragments: ctx
                .apply_rpc_results(
                    &self.fragments,
                    ctx.register_fragments(&self.fragments).await,
                )
                .await?,
        })
    }

    async fn rollback(self, ctx: &mut QueryContext, _mode: StopMode) {
        if buggify() {
            panic!("buggify: planned_rollback_panic");
        }
        ctx.rollback_unregister(&self.fragments).await;
    }
}
