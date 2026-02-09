pub mod database;
pub mod notification;
pub mod query_catalog;
pub mod sink_catalog;
pub mod source_catalog;
pub mod worker_catalog;

#[cfg(test)]
mod test_utils;

pub use notification::NotifiableCatalog;

use database::State;
use model::query::active_query;
use model::worker;
use query_catalog::QueryCatalog;
use sink_catalog::SinkCatalog;
use source_catalog::SourceCatalog;
use std::sync::Arc;
use tokio::sync::mpsc;
use worker_catalog::WorkerCatalog;

/// Receivers for entity state change notifications (catalog → request listener).
///
/// Each receiver delivers every state change as a discrete event via mpsc,
/// ensuring no notifications are lost.
pub struct StateReceivers {
    pub query: mpsc::UnboundedReceiver<active_query::Model>,
    pub worker: mpsc::UnboundedReceiver<worker::Model>,
}

/// Facade providing access to all catalog types.
///
/// Each catalog manages a specific domain (sources, sinks, workers, queries)
/// while sharing the same underlying database connection.
#[derive(Clone)]
pub struct Catalog {
    pub source: Arc<SourceCatalog>,
    pub sink: Arc<SinkCatalog>,
    pub worker: Arc<WorkerCatalog>,
    pub query: Arc<QueryCatalog>,
}

impl Catalog {
    pub fn from(db: State) -> (Self, StateReceivers) {
        let (query_state_tx, query_state_rx) = mpsc::unbounded_channel();
        let (worker_state_tx, worker_state_rx) = mpsc::unbounded_channel();

        let catalog = Self {
            source: SourceCatalog::from(db.clone()),
            sink: SinkCatalog::from(db.clone()),
            worker: WorkerCatalog::new(db.clone(), worker_state_tx),
            query: QueryCatalog::new(db, query_state_tx),
        };

        let receivers = StateReceivers {
            query: query_state_rx,
            worker: worker_state_rx,
        };

        (catalog, receivers)
    }

    #[cfg(test)]
    pub async fn for_test() -> (Self, StateReceivers) {
        Self::from(State::for_test().await)
    }
}
