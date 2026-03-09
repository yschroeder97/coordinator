use crate::query::context::QueryContext;
use crate::query::lifecycle::registered::Registered;
use crate::query::query_task::Transition;
use fail::fail_point;
use model::query::StopMode;
use model::query::fragment;

pub(crate) struct Planned {
    pub(crate) fragments: Vec<fragment::Model>,
}

impl Transition for Planned {
    type Next = Registered;

    async fn transition(&mut self, ctx: &mut QueryContext) -> anyhow::Result<Registered> {
        fail_point!("reconciler_pre_register");
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
        ctx.rollback_unregister(&self.fragments).await;
    }
}
