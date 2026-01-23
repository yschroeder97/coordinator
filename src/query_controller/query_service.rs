use crate::catalog::notification::Notifier;
use crate::catalog::query::query_catalog::QueryCatalog;
use crate::catalog::query::{ActiveQuery, DesiredQueryState, QueryId, StopMode};
use crate::cluster_controller::worker_registry::WorkerRegistryHandle;
use crate::query_controller::query_reconciler::QueryReconciler;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, info};

const QUERY_SERVICE_POLLING_DURATION: Duration = Duration::from_secs(10);

pub(crate) struct QueryService {
    query_catalog: Arc<QueryCatalog>,
    worker_registry: WorkerRegistryHandle,
    tasks: HashMap<QueryId, (flume::Sender<StopMode>, JoinHandle<()>)>,
}

impl QueryService {
    pub(crate) fn new(
        query_catalog: Arc<QueryCatalog>,
        worker_registry: WorkerRegistryHandle,
    ) -> Self {
        QueryService {
            query_catalog,
            worker_registry,
            tasks: HashMap::default(),
        }
    }

    pub(crate) async fn run(mut self) {
        let mut active_queries = self.query_catalog.subscribe();
        info!("Starting");
        self.reconcile().await;

        loop {
            if let Ok(result) =
                tokio::time::timeout(QUERY_SERVICE_POLLING_DURATION, active_queries.changed()).await
            {
                result.expect("Query catalog notification channel closed unexpectedly")
            }
            self.reconcile().await;
        }
    }

    async fn reconcile(&mut self) {
        // 1. Fetch mismatches between the current state and desired state of queries from the catalog.
        // We only care about queries present in the active_queries table.
        let mismatches = self.query_catalog.get_mismatch().await.unwrap();

        // 2. Clean up finished tasks
        self.tasks.retain(|_, (_, handle)| !handle.is_finished());

        // 3. Reconcile
        // Make sure each mismatch has a corresponding running task
        for mismatch in mismatches {
            match self.tasks.get(&mismatch.id) {
                // Mismatch has a task
                Some((stop_channel, _)) => match mismatch.desired_state {
                    DesiredQueryState::Running => {
                        debug!("Reconciliation task for {mismatch:?} is already running");
                    }
                    DesiredQueryState::Stopped => {
                        self.send_stop_signal(&mismatch, stop_channel).await;
                    }
                },
                // Mismatch does not have a reconciliation task -> spawn one
                None => {
                    self.spawn_reconciliation_task(mismatch);
                }
            }
        }
    }

    fn spawn_reconciliation_task(&mut self, mismatch: ActiveQuery) {
        let (stop_controller, stop_listener) = flume::bounded(1);

        let handle = tokio::spawn(QueryReconciler::run(
            mismatch.clone(),
            self.query_catalog.clone(),
            self.worker_registry.clone(),
            stop_listener,
        ));
        self.tasks.insert(mismatch.id, (stop_controller, handle));
    }

    async fn send_stop_signal(
        &self,
        query_to_stop: &ActiveQuery,
        stop_channel: &flume::Sender<StopMode>,
    ) {
        stop_channel
            .send_async(
                query_to_stop
                    .stop_mode
                    .expect("If desired_state == 'Stopped', StopMode must be set"),
            )
            .await
            .expect("Reconciler should be alive");
    }
}
