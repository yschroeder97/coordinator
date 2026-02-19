use crate::query::reconciler::{QueryContext, State, Transition};
use crate::query::registered::Registered;
use model::query::{fragment};
use tracing::info;

pub struct Planned {
    pub fragments: Vec<fragment::Model>,
}

impl Transition for Planned {
    async fn transition(self, ctx: QueryContext) -> State {
        info!("Registering fragments");

        let results = ctx.register_fragments(&self.fragments).await;

        let errors: Vec<_> = results.into_iter().filter_map(|r| r.err()).collect();
        if !errors.is_empty() {
            return Err(RegistrationError::Rpc(errors));
        }

        State::Registered(Registered {
            registered_query:,
            fragments: self.fragments,
        })
    }
}
