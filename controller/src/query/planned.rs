use crate::query::reconciler::{QueryContext, Transition};
use crate::query::registered::Registered;
use model::query::StopMode;
use model::query::fragment;
use tracing::info;

pub struct Planned {
    pub fragments: Vec<fragment::Model>,
}

impl Transition for Planned {
    type Next = Registered;

    async fn transition(&mut self, ctx: &mut QueryContext) -> anyhow::Result<Registered> {
        info!("Registering fragments");
        let results = ctx.register_fragments(&self.fragments).await;
        Ok(Registered {
            fragments: ctx.apply_rpc_results(&self.fragments, results).await?,
        })
    }

    async fn rollback(self, ctx: &mut QueryContext, _mode: StopMode) {
        ctx.rollback_unregister(&self.fragments).await;
    }
}
