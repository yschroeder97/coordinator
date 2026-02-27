#![cfg(madsim)]
use crate::cluster::{Cluster, ClusterConfig};
use model::query::query_state::QueryState;
use model::query::{CreateQuery, DropQuery, GetQuery, StopMode};
use model::worker::endpoint::NetworkAddr;
use model::worker::{CreateWorker, GetWorker, WorkerState};
use std::time::Duration;

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

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
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
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();
    let cluster = setup_cluster(4, 100).await;

    let query: model::query::Model = cluster
        .send(
            CreateQuery::new("SELECT 1 FROM test".to_string()).block_until(QueryState::Running),
        )
        .await
        .unwrap();

    // Invariant: query reached Running
    assert_eq!(query.current_state, QueryState::Running);

    // Invariant: exactly one Running query in the catalog
    let running: Vec<model::query::Model> = cluster
        .send(GetQuery::new().with_current_state(QueryState::Running))
        .await
        .unwrap();
    assert_eq!(running.len(), 1);
    assert_eq!(running[0].id, query.id);

    // Invariant: no queries stuck in intermediate states
    for state in [QueryState::Pending, QueryState::Planned, QueryState::Registered] {
        let stuck: Vec<model::query::Model> = cluster
            .send(GetQuery::new().with_current_state(state))
            .await
            .unwrap();
        assert!(
            stuck.is_empty(),
            "Expected no queries in {state:?}, found {}",
            stuck.len()
        );
    }

    // Stop the query
    let dropped: Vec<model::query::Model> = cluster
        .send(
            DropQuery::new()
                .with_filters(GetQuery::new().with_id(query.id))
                .stop_mode(StopMode::Forceful)
                .blocking(),
        )
        .await
        .unwrap();

    assert_eq!(dropped.len(), 1);
    assert!(dropped[0].current_state.is_terminal());

    // Invariant: no queries remain in any non-terminal state
    let all: Vec<model::query::Model> = cluster.send(GetQuery::new()).await.unwrap();
    for q in &all {
        assert!(
            q.current_state.is_terminal(),
            "Query {} in non-terminal state {:?} after drop",
            q.id,
            q.current_state
        );
    }
}

#[madsim::test]
async fn concurrent_queries_all_reach_running() {
    let cluster = setup_cluster(4, 100).await;

    let num_queries = 5;
    let mut query_ids = Vec::new();

    for i in 0..num_queries {
        let query: model::query::Model = cluster
            .send(CreateQuery::new(format!("SELECT {i} FROM test")))
            .await
            .unwrap();
        assert_eq!(query.current_state, QueryState::Pending);
        query_ids.push(query.id);
    }

    // Wait for reconciliation to advance all queries to Running
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let running: Vec<model::query::Model> = cluster
            .send(GetQuery::new().with_current_state(QueryState::Running))
            .await
            .unwrap();

        if running.len() == num_queries {
            break;
        }

        // Invariant: query states only move forward — no query should
        // be in a state before Pending (there is none) or have regressed
        let all: Vec<model::query::Model> = cluster.send(GetQuery::new()).await.unwrap();
        for q in &all {
            assert!(
                !q.current_state.is_terminal() || q.current_state == QueryState::Completed,
                "Query {} unexpectedly in terminal state {:?} before explicit stop",
                q.id,
                q.current_state
            );
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "Timed out waiting for all queries to reach Running ({}/{})",
            running.len(),
            num_queries
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // Invariant: all submitted queries are Running
    let running: Vec<model::query::Model> = cluster
        .send(GetQuery::new().with_current_state(QueryState::Running))
        .await
        .unwrap();
    assert_eq!(running.len(), num_queries);
    for q in &running {
        assert!(query_ids.contains(&q.id));
    }

    // Invariant: no queries stuck in intermediate states
    for state in [QueryState::Pending, QueryState::Planned, QueryState::Registered] {
        let stuck: Vec<model::query::Model> = cluster
            .send(GetQuery::new().with_current_state(state))
            .await
            .unwrap();
        assert!(
            stuck.is_empty(),
            "Queries stuck in {state:?}: {}",
            stuck.len()
        );
    }

    // Drop all queries at once
    let dropped: Vec<model::query::Model> = cluster
        .send(DropQuery::new().stop_mode(StopMode::Forceful).blocking())
        .await
        .unwrap();

    assert_eq!(dropped.len(), num_queries);

    // Invariant: every query reached a terminal state
    for q in &dropped {
        assert!(
            q.current_state.is_terminal(),
            "Query {} should be terminal, got {:?}",
            q.id,
            q.current_state
        );
    }

    // Invariant: catalog is clean — no non-terminal queries
    for state in [
        QueryState::Pending,
        QueryState::Planned,
        QueryState::Registered,
        QueryState::Running,
    ] {
        let remaining: Vec<model::query::Model> = cluster
            .send(GetQuery::new().with_current_state(state))
            .await
            .unwrap();
        assert!(
            remaining.is_empty(),
            "Expected 0 queries in {state:?} after drop, found {}",
            remaining.len()
        );
    }
}

#[madsim::test]
async fn worker_killed_becomes_unreachable_and_recovers() {
    let cluster = setup_cluster(2, 100).await;

    let worker_1_host = NetworkAddr::new("192.168.2.1".to_string(), 9090);

    cluster.simple_kill_nodes(vec!["worker-1"]).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
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
