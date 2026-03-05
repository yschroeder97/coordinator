#![cfg(madsim)]
use crate::cluster::{Cluster, arb_query, arb_topology_min, worker_recovery_deadline};
use crate::utils::{poll_worker_state, worker_unreachable_deadline};
use madsim::rand::{Rng, thread_rng};
use model::query::query_state::QueryState;
use model::query::GetQuery;
use model::worker::{GetWorker, WorkerState};

#[madsim::test]
async fn running_query_survives_transient_worker_failure() {
    let cluster = Cluster::setup(arb_topology_min(2)).await;

    let query: model::query::Model = cluster
        .send(arb_query().block_until(QueryState::Running))
        .await
        .unwrap();
    assert_eq!(query.current_state, QueryState::Running);

    let kill_idx = thread_rng().gen_range(0..cluster.num_workers());
    let kill_name = cluster.worker_name(kill_idx);
    let kill_host = cluster.worker_host(kill_idx);

    cluster.simple_kill_nodes(vec![&kill_name]).await;
    poll_worker_state(&cluster, &kill_host, WorkerState::Unreachable, worker_unreachable_deadline()).await;

    let after_kill: model::query::Model = cluster
        .send(GetQuery::all().with_id(query.id))
        .await
        .unwrap()
        .first()
        .unwrap()
        .clone();
    assert_eq!(
        after_kill.current_state,
        QueryState::Running,
        "Query should survive a transient worker failure"
    );

    cluster.simple_restart_nodes(vec![&kill_name]).await;
    poll_worker_state(&cluster, &kill_host, WorkerState::Active, worker_recovery_deadline()).await;

    let after_recovery: model::query::Model = cluster
        .send(GetQuery::all().with_id(query.id))
        .await
        .unwrap()
        .first()
        .unwrap()
        .clone();
    assert_eq!(
        after_recovery.current_state,
        QueryState::Running,
        "Query should still be Running after worker recovery"
    );
}

#[madsim::test]
async fn worker_killed_becomes_unreachable_and_recovers() {
    let cluster = Cluster::setup(arb_topology_min(2)).await;

    let kill_idx = thread_rng().gen_range(0..cluster.num_workers());
    let kill_name = cluster.worker_name(kill_idx);
    let kill_host = cluster.worker_host(kill_idx);

    cluster.simple_kill_nodes(vec![&kill_name]).await;
    poll_worker_state(&cluster, &kill_host, WorkerState::Unreachable, worker_unreachable_deadline()).await;

    cluster.simple_restart_nodes(vec![&kill_name]).await;
    poll_worker_state(&cluster, &kill_host, WorkerState::Active, worker_recovery_deadline()).await;

    let survivor_idx = (0..cluster.num_workers())
        .filter(|&i| i != kill_idx)
        .nth(thread_rng().gen_range(0..cluster.num_workers() - 1))
        .unwrap();
    let survivor_host = cluster.worker_host(survivor_idx);
    let survivor: model::worker::Model = cluster
        .send(GetWorker::all().with_host_addr(survivor_host))
        .await
        .unwrap()
        .first()
        .unwrap()
        .clone();
    assert_eq!(
        survivor.current_state,
        WorkerState::Active,
        "Survivor worker should have remained Active"
    );
}
