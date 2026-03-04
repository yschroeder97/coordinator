#![cfg(madsim)]
use crate::cluster::{Cluster, POLL_INTERVAL, arb_query, query_reconciliation_deadline};
use model::query::GetQuery;
use model::query::query_state::QueryState;
use std::time::Duration;

async fn poll_query_state(
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

#[madsim::test]
async fn recovery_after_pre_register_crash() {
    let cluster = Cluster::setup(2, 100).await;

    fail::cfg(
        "reconciler_pre_register",
        "1*panic(injected pre-register crash)",
    )
    .unwrap();

    let created: model::query::Model = cluster.send(arb_query()).await.unwrap();

    let query = poll_query_state(
        &cluster,
        created.id,
        QueryState::Running,
        query_reconciliation_deadline(),
    )
    .await;
    assert_eq!(query.current_state, QueryState::Running);
}

#[madsim::test]
async fn recovery_after_pre_start_crash() {
    let cluster = Cluster::setup(2, 100).await;

    fail::cfg("reconciler_pre_start", "1*panic(injected pre-start crash)").unwrap();

    let created: model::query::Model = cluster.send(arb_query()).await.unwrap();

    let query = poll_query_state(
        &cluster,
        created.id,
        QueryState::Running,
        query_reconciliation_deadline(),
    )
    .await;
    assert_eq!(query.current_state, QueryState::Running);
}

#[madsim::test]
async fn recovery_after_post_rpc_pre_db_crash() {
    let cluster = Cluster::setup(2, 100).await;

    fail::cfg(
        "reconciler_post_rpc_pre_db",
        "1*panic(injected post-rpc crash)",
    )
    .unwrap();

    let created: model::query::Model = cluster.send(arb_query()).await.unwrap();

    let query = poll_query_state(
        &cluster,
        created.id,
        QueryState::Running,
        query_reconciliation_deadline(),
    )
    .await;
    assert_eq!(query.current_state, QueryState::Running);
}

#[madsim::test]
async fn error_during_create_fragments_fails_query() {
    let cluster = Cluster::setup(2, 100).await;

    fail::cfg("reconciler_create_fragments", "1*return").unwrap();

    let created: model::query::Model = cluster.send(arb_query()).await.unwrap();

    let query = poll_query_state(
        &cluster,
        created.id,
        QueryState::Failed,
        query_reconciliation_deadline(),
    )
    .await;
    assert_eq!(query.current_state, QueryState::Failed);
    assert!(query.error.is_some());

    fail::remove("reconciler_create_fragments");
}
