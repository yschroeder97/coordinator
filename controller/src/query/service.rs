use crate::cluster::worker_registry::WorkerRegistryHandle;
use crate::query::QueryId;
use crate::query::context::QueryContext;
use crate::query::reconciler::QueryReconciler;
use catalog::Catalog;
use catalog::Reconcilable;
use model::query::*;

use model::query;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{Instrument, debug, error, info, info_span, warn};

#[cfg(madsim)]
async fn supervised<F: std::future::Future<Output = ()>>(fut: F) -> bool {
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;
    AssertUnwindSafe(fut).catch_unwind().await.is_err()
}

#[cfg(not(madsim))]
async fn supervised<F: std::future::Future<Output = ()>>(fut: F) -> bool {
    fut.await;
    false
}

pub const QUERY_SERVICE_POLLING_DURATION: Duration = Duration::from_secs(10);

pub struct QueryService {
    catalog: Arc<Catalog>,
    worker_registry: WorkerRegistryHandle,
    tasks: HashMap<QueryId, (flume::Sender<StopMode>, JoinHandle<bool>)>,
}

impl QueryService {
    pub fn new(catalog: Arc<Catalog>, worker_registry: WorkerRegistryHandle) -> Self {
        QueryService {
            catalog,
            worker_registry,
            tasks: HashMap::default(),
        }
    }

    pub async fn run(mut self) {
        let mut intent = self.catalog.query.subscribe_intent();
        info!("Starting");
        self.reconcile().await;

        loop {
            tokio::select! {
                result = intent.changed() => {
                    if result.is_err() {
                        info!("Query catalog notification channel closed, shutting down");
                        return;
                    }
                }
                _ = tokio::time::sleep(QUERY_SERVICE_POLLING_DURATION) => {}
            }
            self.reconcile().await;
        }
    }

    async fn reconcile(&mut self) {
        let mismatches = match self.catalog.query.get_mismatch().await {
            Ok(m) => m,
            Err(e) => {
                warn!("Failed to fetch query mismatches: {e}");
                return;
            }
        };

        let finished: Vec<QueryId> = self
            .tasks
            .iter()
            .filter(|(_, (_, h))| h.is_finished())
            .map(|(&id, _)| id)
            .collect();
        for id in finished {
            if let Some((_, handle)) = self.tasks.remove(&id) {
                match handle.await {
                    Ok(true) => error!(query_id = id, "Reconciler panicked"),
                    Err(e) if e.is_panic() => {
                        error!(query_id = id, "Reconciler panicked: {e:?}")
                    }
                    Err(e) => error!(query_id = id, "Reconciler task error: {e:?}"),
                    Ok(false) => {}
                }
            }
        }

        for mismatch in mismatches {
            if mismatch.current_state.is_terminal() {
                continue;
            }

            match self.tasks.get(&mismatch.id) {
                Some((stop_channel, _)) => match mismatch.desired_state {
                    query_state::DesiredQueryState::Completed => {
                        debug!(query_id = mismatch.id, "Reconciliation already running, skipping");
                    }
                    query_state::DesiredQueryState::Stopped => {
                        self.send_stop_signal(&mismatch, stop_channel).await;
                    }
                },
                None => {
                    self.spawn_reconciliation_task(mismatch);
                }
            }
        }
    }

    fn spawn_reconciliation_task(&mut self, mismatch: query::Model) {
        let (stop_tx, stop_rx) = flume::bounded(1);
        let query_id = mismatch.id;
        info!(query_id, name = %mismatch.name, "Spawning reconciliation task");

        let ctx = QueryContext {
            query: mismatch,
            catalog: self.catalog.clone(),
            worker_registry: self.worker_registry.clone(),
        };

        let handle = tokio::spawn(supervised(QueryReconciler::run(ctx, stop_rx)));
        self.tasks.insert(query_id, (stop_tx, handle));
    }

    async fn send_stop_signal(
        &self,
        query_to_stop: &query::Model,
        stop_channel: &flume::Sender<StopMode>,
    ) {
        let mode = query_to_stop
            .stop_mode
            .expect("If desired_state == 'Stopped', StopMode must be set");
        match stop_channel.send_async(mode).await {
            Ok(_) => debug!(query_id = query_to_stop.id, ?mode, "Sent stop signal"),
            Err(_) => debug!(query_id = query_to_stop.id, "Reconciliation task already finished"),
        }
    }
}

impl crate::Supervisable for QueryService {
    fn start(self) -> impl std::future::Future<Output = ()> + Send {
        self.run().instrument(info_span!("query_service"))
    }
}
