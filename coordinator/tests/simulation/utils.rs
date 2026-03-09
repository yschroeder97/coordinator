#![cfg(madsim)]
use crate::cluster::{Cluster, POLL_INTERVAL};
use controller::worker::health_monitor::{HEALTH_CHECK_INTERVAL, PROBE_TIMEOUT};
use controller::worker::worker_controller::WORKER_SERVICE_POLL_INTERVAL;
use model::query::GetQuery;
use model::query::query_state::QueryState;
use model::worker::endpoint::NetworkAddr;
use model::worker::{GetWorker, WorkerState};
use std::time::Duration;

pub(crate) const fn worker_unreachable_deadline() -> Duration {
    Duration::from_secs(
        HEALTH_CHECK_INTERVAL.as_secs()
            + PROBE_TIMEOUT.as_secs()
            + WORKER_SERVICE_POLL_INTERVAL.as_secs(),
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
