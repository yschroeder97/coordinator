pub mod database;
pub mod notification;
pub mod query_catalog;
pub mod sink_catalog;
pub mod source_catalog;
pub mod worker_catalog;

#[cfg(test)]
pub mod test_utils;

pub use notification::NotifiableCatalog;

use database::Database;
use query_catalog::QueryCatalog;
use sink_catalog::SinkCatalog;
use source_catalog::SourceCatalog;
use std::sync::Arc;
use worker_catalog::WorkerCatalog;

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
    pub fn from(db: Database) -> Arc<Self> {
        Arc::new(Self {
            source: SourceCatalog::from(db.clone()),
            sink: SinkCatalog::from(db.clone()),
            worker: WorkerCatalog::new(db.clone()),
            query: QueryCatalog::new(db),
        })
    }

    #[cfg(test)]
    pub async fn for_test() -> Arc<Self> {
        Self::from(Database::for_test().await)
    }
}
