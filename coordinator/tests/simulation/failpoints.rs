#![cfg(madsim)]
use crate::cluster::Cluster;
use model::query::query_state::QueryState;
use model::query::{CreateQuery, GetQuery};
use std::time::Duration;

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
    let cluster = Cluster::setup(2, 100).await;

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
    let cluster = Cluster::setup(2, 100).await;

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
    let cluster = Cluster::setup(2, 100).await;

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
    let cluster = Cluster::setup(2, 100).await;

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
