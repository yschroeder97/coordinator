#![cfg(madsim)]
use crate::cluster::{Cluster, ClusterConfig};
use model::query::query_state::QueryState;
use model::query::{CreateQuery, GetQuery};
use model::worker::endpoint::NetworkAddr;
use model::worker::{CreateWorker, GetWorker, WorkerState};
use std::time::Duration;

use controller::cluster::worker_client::{
    CONNECT_INITIAL_BACKOFF_MS, CONNECT_MAX_RETRIES, CONNECT_TIMEOUT,
};

const fn worker_recovery_deadline() -> Duration {
    let backoff_total_ms = CONNECT_INITIAL_BACKOFF_MS * ((1 << CONNECT_MAX_RETRIES) - 1);
    let connect_attempts_secs =
        (CONNECT_MAX_RETRIES as u64 + 1) * CONNECT_TIMEOUT.as_secs();
    Duration::from_secs(
        connect_attempts_secs
            + backoff_total_ms / 1000
            + controller::cluster::service::CLUSTER_SERVICE_POLLING_DURATION.as_secs()
            + CONNECT_TIMEOUT.as_secs(),
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

async fn poll_query_state(
    cluster: &Cluster,
    query_id: i64,
    target: QueryState,
    timeout: Duration,
) -> model::query::Model {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let queries: Vec<model::query::Model> = cluster
            .send(GetQuery::all().with_id(query_id))
            .await
            .unwrap();
        if queries[0].current_state == target {
            return queries[0].clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out waiting for query {} to reach {:?} (current: {:?})",
            query_id,
            target,
            queries[0].current_state
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[madsim::test]
async fn recovery_after_pre_register_crash() {
    let cluster = setup_cluster(2, 100).await;

    fail::cfg("reconciler_pre_register", "1*panic(injected pre-register crash)").unwrap();

    let created: model::query::Model = cluster
        .send(CreateQuery::new("SELECT 1 FROM test".to_string()))
        .await
        .unwrap();

    let query = poll_query_state(&cluster, created.id, QueryState::Running, Duration::from_secs(60)).await;
    assert_eq!(query.current_state, QueryState::Running);
}

#[madsim::test]
async fn recovery_after_pre_start_crash() {
    let cluster = setup_cluster(2, 100).await;

    fail::cfg("reconciler_pre_start", "1*panic(injected pre-start crash)").unwrap();

    let created: model::query::Model = cluster
        .send(CreateQuery::new("SELECT 1 FROM test".to_string()))
        .await
        .unwrap();

    let query = poll_query_state(&cluster, created.id, QueryState::Running, Duration::from_secs(60)).await;
    assert_eq!(query.current_state, QueryState::Running);
}

#[madsim::test]
async fn recovery_after_post_rpc_pre_db_crash() {
    let cluster = setup_cluster(2, 100).await;

    fail::cfg("reconciler_post_rpc_pre_db", "1*panic(injected post-rpc crash)").unwrap();

    let created: model::query::Model = cluster
        .send(CreateQuery::new("SELECT 1 FROM test".to_string()))
        .await
        .unwrap();

    let query = poll_query_state(&cluster, created.id, QueryState::Running, Duration::from_secs(60)).await;
    assert_eq!(query.current_state, QueryState::Running);
}

#[madsim::test]
async fn error_during_create_fragments_fails_query() {
    let cluster = setup_cluster(2, 100).await;

    fail::cfg("reconciler_create_fragments", "1*return").unwrap();

    let created: model::query::Model = cluster
        .send(CreateQuery::new("SELECT 1 FROM test".to_string()))
        .await
        .unwrap();

    let query = poll_query_state(&cluster, created.id, QueryState::Failed, Duration::from_secs(60)).await;
    assert_eq!(query.current_state, QueryState::Failed);
    assert!(query.error.is_some());

    fail::remove("reconciler_create_fragments");
}
