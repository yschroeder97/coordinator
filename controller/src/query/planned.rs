use crate::query::reconciler::{QueryContext, Transition};
use crate::query::registered::Registered;
use model::query::fragment::{self, FragmentState};
use model::query::StopMode;
use tracing::info;

pub struct Planned {
    pub fragments: Vec<fragment::Model>,
}

impl Transition for Planned {
    type Next = Registered;
    type Error = anyhow::Error;

    async fn advance(&mut self, ctx: &mut QueryContext) -> Result<Registered, Self::Error> {
        info!("Registering fragments");
        let results = ctx.register_fragments(&self.fragments).await;

        ctx.apply_rpc_results(&self.fragments, results, FragmentState::Registered)
            .await?;
        
        Ok(Registered {
            fragments: std::mem::take(&mut self.fragments),
        })
    }

    async fn cleanup(self, ctx: &mut QueryContext, _mode: StopMode) {
        ctx.cleanup_unregister(&self.fragments).await;
    }
}
