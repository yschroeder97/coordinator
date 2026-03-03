#![cfg(madsim)]
use crate::cluster::{Cluster, ClusterConfig};
use controller::cluster::health_monitor::{FAILURE_THRESHOLD, HEALTH_CHECK_INTERVAL};
use controller::cluster::service::CLUSTER_SERVICE_POLLING_DURATION;
use controller::cluster::worker_client::{
    WorkerClient, CONNECT_INITIAL_BACKOFF_MS, CONNECT_MAX_RETRIES,
};
use model::query::query_state::QueryState;
use model::query::{CreateQuery, DropQuery, GetQuery, StopMode};
use model::worker::endpoint::NetworkAddr;
use model::worker::{CreateWorker, GetWorker, WorkerState};
use std::time::Duration;

/// Worst-case time for the health monitor to mark a dead worker as Unreachable.
///
/// The health monitor probes every `HEALTH_CHECK_INTERVAL`. After `FAILURE_THRESHOLD`
/// consecutive failures it updates the worker state. The cluster service then needs
/// up to one poll interval to notice and tear down the connection.
const fn worker_unreachable_deadline() -> Duration {
    Duration::from_secs(
        FAILURE_THRESHOLD as u64 * HEALTH_CHECK_INTERVAL.as_secs()
            + CLUSTER_SERVICE_POLLING_DURATION.as_secs(),
    )
}

/// Worst-case time for a worker to become Active after being restarted.
///
/// If a connect attempt was already in progress when the worker was down, it must
/// exhaust all retries before the cluster service spawns a new (successful) attempt.
/// Backoff sum (no jitter): `CONNECT_INITIAL_BACKOFF_MS * (2^CONNECT_MAX_RETRIES - 1)`.
const fn worker_recovery_deadline() -> Duration {
    let backoff_total_ms = CONNECT_INITIAL_BACKOFF_MS * ((1 << CONNECT_MAX_RETRIES) - 1);
    let connect_attempts_secs =
        (CONNECT_MAX_RETRIES as u64 + 1) * WorkerClient::CONNECT_TIMEOUT.as_secs();
    Duration::from_secs(
        connect_attempts_secs
            + backoff_total_ms / 1000
            + CLUSTER_SERVICE_POLLING_DURATION.as_secs()
            + WorkerClient::CONNECT_TIMEOUT.as_secs(),
    )
}

async fn setup_cluster(num_workers: usize, worker_capacity: i32) -> Cluster {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let cluster = Cluster::start(ClusterConfig {
        num_workers,
        ..Default::default()
    })
        .await
        .unwrap();

    for i in 1..=num_workers {
        let host = NetworkAddr::new(format!("192.168.2.{i}"), 9090);
        let grpc = NetworkAddr::new(format!("192.168.2.{i}"), 8080);
        let result: anyhow::Result<model::worker::Model> =
            cluster.send(CreateWorker::new(host, grpc, worker_capacity)).await;
        result.unwrap();
    }

    let deadline = tokio::time::Instant::now() + worker_recovery_deadline();
    loop {
        let workers: anyhow::Result<Vec<model::worker::Model>> =
            cluster.send(GetWorker::all()).await;
        let workers = workers.unwrap();

        let active_count = workers
            .iter()
            .filter(|w| w.current_state == WorkerState::Active)
            .count();

        if active_count == num_workers {
            break;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out waiting for workers to become Active ({active_count}/{num_workers})"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    cluster
}

#[madsim::test]
async fn query_lifecycle() {
    let cluster = setup_cluster(4, 100).await;

    let query: model::query::Model = cluster
        .send(
            CreateQuery::new("SELECT 1 FROM test".to_string()).block_until(QueryState::Running),
        )
        .await
        .unwrap();

    assert_eq!(query.current_state, QueryState::Running);

    // Invariant: exactly one Running query in the catalog
    let queries: Vec<model::query::Model> = cluster
        .send(GetQuery::all())
        .await
        .unwrap();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].id, query.id);

    // Stop the query
    let dropped: Vec<model::query::Model> = cluster
        .send(
            DropQuery::all()
                .with_filters(GetQuery::all().with_id(query.id))
                .stop_mode(StopMode::Forceful)
                .blocking(),
        )
        .await
        .unwrap();

    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0].current_state, QueryState::Stopped);
    assert_eq!(dropped[0].stop_mode.unwrap(), StopMode::Forceful);
}

/// A non-blocking create eventually reconciles to Running.
#[madsim::test]
async fn non_blocking_create_reconciles_to_running() {
    let cluster = setup_cluster(2, 100).await;

    let created: model::query::Model = cluster
        .send(CreateQuery::new("SELECT 1 FROM test".to_string()))
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let queries: Vec<model::query::Model> = cluster
            .send(GetQuery::all().with_id(created.id))
            .await
            .unwrap();

        if queries[0].current_state == QueryState::Running {
            break;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out waiting for query to reach Running (current: {:?})",
            queries[0].current_state
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Dropping a query with Graceful stop mode returns Stopped with the correct mode.
#[madsim::test]
async fn drop_query_gracefully() {
    let cluster = setup_cluster(2, 100).await;

    let query: model::query::Model = cluster
        .send(
            CreateQuery::new("SELECT 1 FROM test".to_string()).block_until(QueryState::Running),
        )
        .await
        .unwrap();

    let dropped: Vec<model::query::Model> = cluster
        .send(
            DropQuery::all()
                .with_filters(GetQuery::all().with_id(query.id))
                .stop_mode(StopMode::Graceful)
                .blocking(),
        )
        .await
        .unwrap();

    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0].current_state, QueryState::Stopped);
    assert_eq!(dropped[0].stop_mode.unwrap(), StopMode::Graceful);
}

/// Dropping a query that hasn't reached Running yet still results in Stopped.
#[madsim::test]
async fn drop_query_before_running() {
    let cluster = setup_cluster(2, 100).await;

    let query: model::query::Model = cluster
        .send(CreateQuery::new("SELECT 1 FROM test".to_string()))
        .await
        .unwrap();

    let dropped: Vec<model::query::Model> = cluster
        .send(
            DropQuery::all()
                .with_filters(GetQuery::all().with_id(query.id))
                .stop_mode(StopMode::Forceful)
                .blocking(),
        )
        .await
        .unwrap();

    assert_eq!(dropped.len(), 1);
    assert!(dropped[0].current_state.is_terminal());
}

/// A Running query survives a transient worker failure (kill + restart).
/// The Running poller skips unreachable fragments and resumes once the worker reconnects.
#[madsim::test]
async fn running_query_survives_transient_worker_failure() {
    let cluster = setup_cluster(2, 100).await;

    let query: model::query::Model = cluster
        .send(
            CreateQuery::new("SELECT 1 FROM test".to_string()).block_until(QueryState::Running),
        )
        .await
        .unwrap();
    assert_eq!(query.current_state, QueryState::Running);

    // Kill a worker — the Running poller will fail to reach some fragments
    cluster.simple_kill_nodes(vec!["worker-1"]).await;

    let worker_1_host = NetworkAddr::new("192.168.2.1".to_string(), 9090);
    let deadline = tokio::time::Instant::now() + worker_unreachable_deadline();
    loop {
        let workers: Vec<model::worker::Model> = cluster
            .send(GetWorker::all().with_host_addr(worker_1_host.clone()))
            .await
            .unwrap();
        if workers[0].current_state == WorkerState::Unreachable {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out waiting for worker-1 to become Unreachable"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // The query should still be Running — poll failures are transient, not fatal
    let queries: Vec<model::query::Model> = cluster
        .send(GetQuery::all().with_id(query.id))
        .await
        .unwrap();
    assert_eq!(
        queries[0].current_state,
        QueryState::Running,
        "Query should survive a transient worker failure"
    );

    // Restart the worker and wait for recovery
    cluster.simple_restart_nodes(vec!["worker-1"]).await;

    let deadline = tokio::time::Instant::now() + worker_recovery_deadline();
    loop {
        let workers: Vec<model::worker::Model> = cluster
            .send(GetWorker::all().with_host_addr(worker_1_host.clone()))
            .await
            .unwrap();
        if workers[0].current_state == WorkerState::Active {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out waiting for worker-1 to recover"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // The query should still be Running after recovery
    let queries: Vec<model::query::Model> = cluster
        .send(GetQuery::all().with_id(query.id))
        .await
        .unwrap();
    assert_eq!(
        queries[0].current_state,
        QueryState::Running,
        "Query should still be Running after worker recovery"
    );
}

#[madsim::test]
async fn worker_killed_becomes_unreachable_and_recovers() {
    let cluster = setup_cluster(2, 100).await;

    let worker_1_host = NetworkAddr::new("192.168.2.1".to_string(), 9090);

    cluster.simple_kill_nodes(vec!["worker-1"]).await;

    let deadline = tokio::time::Instant::now() + worker_unreachable_deadline();
    loop {
        let workers: Vec<model::worker::Model> = cluster
            .send(GetWorker::all().with_host_addr(worker_1_host.clone()))
            .await
            .unwrap();

        if workers[0].current_state == WorkerState::Unreachable {
            break;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out waiting for worker-1 to become Unreachable (current: {:?})",
            workers[0].current_state
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    cluster.simple_restart_nodes(vec!["worker-1"]).await;

    let deadline = tokio::time::Instant::now() + worker_recovery_deadline();
    loop {
        let workers: Vec<model::worker::Model> = cluster
            .send(GetWorker::all().with_host_addr(worker_1_host.clone()))
            .await
            .unwrap();

        if workers[0].current_state == WorkerState::Active {
            break;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out waiting for worker-1 to become Active again (current: {:?})",
            workers[0].current_state
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let worker_2_host = NetworkAddr::new("192.168.2.2".to_string(), 9090);
    let workers: Vec<model::worker::Model> = cluster
        .send(GetWorker::all().with_host_addr(worker_2_host))
        .await
        .unwrap();
    assert_eq!(
        workers[0].current_state,
        WorkerState::Active,
        "Worker-2 should have remained Active"
    );
}
