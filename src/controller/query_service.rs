use crate::catalog::notification::Notifier;
use crate::catalog::query::query_catalog::QueryCatalog;
use crate::catalog::query::{QueryId, QueryState, StopMode};
use crate::controller::query_reconciler::QueryReconciler;
use crate::network::worker_registry::WorkerRegistryHandle;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info};

const QUERY_SERVICE_POLLING_DURATION: Duration = Duration::from_secs(5);

pub(crate) struct QueryService {
    query_catalog: Arc<QueryCatalog>,
    worker_registry: WorkerRegistryHandle,
    tasks: HashMap<QueryId, (mpsc::Sender<StopMode>, JoinHandle<()>)>,
}

impl QueryService {
    pub(crate) fn new(catalog: Arc<QueryCatalog>, worker_registry: WorkerRegistryHandle) -> Self {
        QueryService {
            query_catalog: catalog,
            worker_registry,
            tasks: HashMap::default(),
        }
    }

    pub(crate) async fn run(&mut self) {
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
        let mismatched_queries = match self.query_catalog.get_mismatch().await {
            Ok(queries) => queries,
            Err(e) => {
                error!("Failed to fetch active queries: {:?}", e);
                return;
            }
        };

        // 2. Clean up finished tasks
        self.tasks.retain(|_, (_, handle)| !handle.is_finished());

        // 3. Reconcile
        // Make sure each mismatch has a corresponding running task
        for mismatch in mismatched_queries {
            match self.tasks.get(&mismatch.id) {
                // Mismatch has a task
                Some((stop_controller, _)) => {
                    // When there is a task, only stopping the query is a valid transition
                    if mismatch.desired_state == QueryState::Stopped {
                        stop_controller
                            .send(
                                mismatch.stop_mode.expect(
                                    "BUG: if desired_state is Stopped, StopMode must be set",
                                ),
                            )
                            .await
                            .expect("Reconciler should be alive");
                    } else {
                        error!(
                            "If a reconciliation task is running, only stopping the query is valid"
                        );
                    }
                }
                // Mismatch does not have a reconciliation task -> spawn one
                None => {
                    let (stop_controller, stop_listener) = mpsc::channel(1);
                    let reconciler = QueryReconciler::new(
                        mismatch.id.clone(),
                        self.query_catalog.clone(),
                        self.worker_registry.clone(),
                        stop_listener,
                    );

                    let handle = tokio::spawn(reconciler.run());
                    self.tasks.insert(mismatch.id, (stop_controller, handle));
                }
            }
        }
    }
}
