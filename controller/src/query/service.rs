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
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;
use tracing::{debug, error, info, warn};

const QUERY_SERVICE_POLLING_DURATION: Duration = Duration::from_secs(10);

pub struct QueryService {
    catalog: Arc<Catalog>,
    worker_registry: WorkerRegistryHandle,
    tasks: HashMap<QueryId, (flume::Sender<StopMode>, JoinHandle<()>)>,
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
                    result.expect("Query catalog notification channel closed unexpectedly");
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

        self.tasks.retain(|_, (_, handle)| !handle.is_finished());

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

        let handle = tokio::spawn(async move {
            if let Err(e) = AssertUnwindSafe(QueryReconciler::run(ctx, stop_rx))
                .catch_unwind()
                .await
            {
                error!(query_id, "Reconciler panicked: {e:?}");
            }
        });
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
