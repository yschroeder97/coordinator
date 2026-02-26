use crate::query::reconciler::{QueryContext, Transition};
use crate::query::running::Running;
use model::query::StopMode;
use model::query::fragment::{self, FragmentState};
use tracing::info;

pub struct Registered {
    pub fragments: Vec<fragment::Model>,
}

impl Transition for Registered {
    type Next = Running;
    type Error = anyhow::Error;

    async fn advance(&mut self, ctx: &mut QueryContext) -> Result<Running, Self::Error> {
        info!("Starting fragments");
        let results = ctx.start_fragments(&self.fragments).await;

        ctx.apply_rpc_results(&self.fragments, results, FragmentState::Started)
            .await?;

        Ok(Running {
            fragments: std::mem::take(&mut self.fragments),
        })
    }

    async fn cleanup(self, ctx: &mut QueryContext, mode: StopMode) {
        ctx.cleanup_stop(mode, &self.fragments).await;
        ctx.cleanup_unregister(&self.fragments).await;
    }
}
