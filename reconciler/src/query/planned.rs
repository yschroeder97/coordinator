use crate::cluster::worker_registry::{WorkerCommunicationError, WorkerRegistryHandle};
use crate::query::reconciler::{QueryContext, State, Transition};
use crate::query::registered::Registered;
use model::query::{StopMode, active_query, fragment};
use tracing::info;

pub struct Planned {
    pub planned_query: active_query::Model,
    pub fragments: Vec<fragment::Model>,
}

impl Planned {
    pub async fn load(active_query: active_query::Model, ctx: QueryContext) -> Planned {
        let fragments = ctx.query_catalog
            .get_fragments(&active_query.id)
            .await
            .unwrap();

        Planned {
            planned_query: active_query,
            fragments,
        }
    }
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
            registered_query: ,
            fragments: self.fragments,
        })
    }
}
