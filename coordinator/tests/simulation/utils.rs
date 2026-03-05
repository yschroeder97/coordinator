#![cfg(madsim)]
use crate::cluster::{Cluster, POLL_INTERVAL};
use controller::cluster::health_monitor::{FAILURE_THRESHOLD, HEALTH_CHECK_INTERVAL};
use controller::cluster::service::CLUSTER_SERVICE_POLLING_DURATION;
use model::query::GetQuery;
use model::query::query_state::QueryState;
use model::worker::endpoint::NetworkAddr;
use model::worker::{GetWorker, WorkerState};
use std::time::Duration;

// Upper bound on how long until a killed worker is marked Unreachable:
// - HealthMonitor probes every HEALTH_CHECK_INTERVAL; after FAILURE_THRESHOLD
//   consecutive failures the worker transitions to Unreachable
// - ClusterService must poll once more to propagate the state change
pub(crate) const fn worker_unreachable_deadline() -> Duration {
    Duration::from_secs(
        FAILURE_THRESHOLD as u64 * HEALTH_CHECK_INTERVAL.as_secs()
            + CLUSTER_SERVICE_POLLING_DURATION.as_secs(),
    )
}

pub(crate) async fn poll_query_state(
    cluster: &Cluster,
    query_id: i64,
    target: QueryState,
    timeout: Duration,
) -> model::query::Model {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let query: model::query::Model = cluster
            .send(GetQuery::all().with_id(query_id))
            .await
            .unwrap()
            .first()
            .unwrap()
            .clone();
        if query.current_state == target {
            return query;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out waiting for query {} to reach {:?} (current: {:?})",
            query_id,
            target,
            query.current_state
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

pub(crate) async fn poll_worker_state(
    cluster: &Cluster,
    host: &NetworkAddr,
    target: WorkerState,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let worker: model::worker::Model = cluster
            .send(GetWorker::all().with_host_addr(host.clone()))
            .await
            .unwrap()
            .first()
            .unwrap()
            .clone();
        if worker.current_state == target {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out waiting for worker at {} to reach {:?} (current: {:?})",
            host,
            target,
            worker.current_state
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}
