#![cfg(madsim)]
use crate::cluster::{Cluster, arb_query, arb_topology, arb_worker_config, query_reconciliation_deadline};
use crate::utils::poll_query_state;
use model::query::query_state::QueryState;

#[madsim::test]
async fn recovery_after_pre_register_crash() {
    let cluster = Cluster::setup(arb_topology(), arb_worker_config()).await;

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
    let cluster = Cluster::setup(arb_topology(), arb_worker_config()).await;

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
    let cluster = Cluster::setup(arb_topology(), arb_worker_config()).await;

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
    let cluster = Cluster::setup(arb_topology(), arb_worker_config()).await;

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
