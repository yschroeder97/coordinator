#![cfg(madsim)]
use crate::cluster::{Cluster, arb_query, arb_topology, arb_topology_min, query_reconciliation_deadline, worker_recovery_deadline};
use crate::utils::{poll_query_state, poll_worker_state, worker_unreachable_deadline};
use madsim::rand::{Rng, thread_rng};
use model::query::query_state::QueryState;
use model::worker::WorkerState;

#[madsim::test]
async fn recovery_after_pre_register_crash() {
    let cluster = Cluster::setup(arb_topology()).await;

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
    let cluster = Cluster::setup(arb_topology()).await;

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
    let cluster = Cluster::setup(arb_topology()).await;

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
    let cluster = Cluster::setup(arb_topology()).await;

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
}

#[madsim::test]
async fn recovery_after_worker_activate_crash() {
    let cluster = Cluster::setup(arb_topology_min(2)).await;

    let kill_idx = thread_rng().gen_range(0..cluster.num_workers());
    let kill_name = cluster.worker_name(kill_idx);
    let kill_host = cluster.worker_host(kill_idx);

    cluster.simple_kill_nodes(vec![&kill_name]).await;
    poll_worker_state(&cluster, &kill_host, WorkerState::Unreachable, worker_unreachable_deadline()).await;

    fail::cfg(
        "worker_controller_pre_activate",
        "1*panic(injected pre-activate crash)",
    )
    .unwrap();

    cluster.simple_restart_nodes(vec![&kill_name]).await;
    poll_worker_state(&cluster, &kill_host, WorkerState::Active, worker_recovery_deadline()).await;
}
