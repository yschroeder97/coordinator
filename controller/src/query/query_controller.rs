use crate::worker::poly_join_set::JoinSet;
use crate::worker::worker_registry::WorkerRegistryHandle;
use crate::query::QueryId;
use crate::query::context::QueryContext;
use crate::query::query_task::QueryReconciler;
use catalog::Catalog;
use catalog::Reconcilable;
use model::query::*;

use model::query;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use common::supervised::supervised;
use tracing::{debug, error, info, warn};

const QUERY_CONTROLLER_POLL_INTERVAL: Duration = Duration::from_secs(5);

pub struct QueryController {
    catalog: Arc<Catalog>,
    worker_registry: WorkerRegistryHandle,
    reconcilers: JoinSet<QueryId>,
    stop_channels: HashMap<QueryId, flume::Sender<StopMode>>,
}

impl QueryController {
    pub fn new(catalog: Arc<Catalog>, worker_registry: WorkerRegistryHandle) -> Self {
        QueryController {
            catalog,
            worker_registry,
            reconcilers: JoinSet::new(),
            stop_channels: HashMap::default(),
        }
    }

    pub async fn run(mut self) {
        let mut intent = self.catalog.query.subscribe_intent();
        info!("Starting");
        // Reconcile once
        self.reconcile().await;

        loop {
            // Select triggers reconciliation
            tokio::select! {
                // Query create/drop requested by the client
                result = intent.changed() => {
                    if result.is_err() {
                        info!("Query catalog notification channel closed, shutting down");
                        return;
                    }
                }
                // An inner reconciliation task exited
                Some(result) = self.reconcilers.join_next() => {
                    self.on_reconciler_finished(result);
                }
                // Polling interval expired
                _ = tokio::time::sleep(QUERY_CONTROLLER_POLL_INTERVAL) => {}
            }
            self.reconcile().await;
        }
    }

    async fn reconcile(&mut self) {
        // Acquire state mismatches from the catalog
        let mismatches = match self.catalog.query.get_mismatch().await {
            Ok(m) => m,
            Err(e) => {
                warn!("Failed to fetch query mismatches: {e}");
                return;
            }
        };

        // Check for each mismatch if it has a corresponding reconciliation task running
        for mismatch in mismatches {
            match self.stop_channels.get(&mismatch.id) {
                Some(stop_channel) => match mismatch.desired_state {
                    query_state::DesiredQueryState::Completed => {
                        debug!(query_id = mismatch.id, "Reconciliation already running, skipping");
                    }
                    query_state::DesiredQueryState::Stopped => {
                        self.send_stop_signal(&mismatch, stop_channel);
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

        self.reconcilers.spawn(async move {
            supervised(QueryReconciler::run(ctx, stop_rx)).await;
            query_id
        });
        self.stop_channels.insert(query_id, stop_tx);
    }

    fn on_reconciler_finished(&mut self, result: Result<QueryId, tokio::task::JoinError>) {
        match result {
            Ok(query_id) => {
                self.stop_channels.remove(&query_id);
            }
            Err(e) if e.is_cancelled() => {
                self.cleanup();
            }
            Err(e) => {
                error!("Reconciler task failed: {e:?}");
                self.cleanup();
            }
        }
    }

    fn cleanup(&mut self) {
        self.stop_channels.retain(|id, sender| {
            let alive = !sender.is_disconnected();
            if !alive {
                error!(query_id = id, "Reconciler panicked");
            }
            alive
        });
    }

    // Uses try_send on a bounded(1) channel: if the buffer already holds a stop signal
    // that the reconciler hasn't consumed yet, we skip rather than block the reconcile loop.
    fn send_stop_signal(
        &self,
        query_to_stop: &query::Model,
        stop_channel: &flume::Sender<StopMode>,
    ) {
        let mode = query_to_stop
            .stop_mode
            .expect("If desired_state == 'Stopped', the stop mode must be set");
        match stop_channel.try_send(mode) {
            Ok(_) => debug!(query_id = query_to_stop.id, ?mode, "Sent stop signal"),
            Err(flume::TrySendError::Full(_)) => {}
            Err(flume::TrySendError::Disconnected(_)) => {
                debug!(query_id = query_to_stop.id, "Reconciliation task already finished")
            }
        }
    }
}

