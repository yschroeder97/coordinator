#![cfg(madsim)]
use crate::cluster::{Cluster, arb_query, arb_topology, arb_worker_config, query_reconciliation_deadline};
use crate::utils::poll_query_state;
use model::query::query_state::QueryState;
use model::query::{DropQuery, GetQuery, StopMode};

#[madsim::test]
async fn query_lifecycle() {
    let cluster = Cluster::setup(arb_topology(), arb_worker_config()).await;

    let query: model::query::Model = cluster
        .send(arb_query().block_until(QueryState::Running))
        .await
        .unwrap();

    assert_eq!(query.current_state, QueryState::Running);

    let queries: Vec<model::query::Model> = cluster
        .send(GetQuery::all())
        .await
        .unwrap();
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].id, query.id);

    let dropped: model::query::Model = cluster
        .send(
            DropQuery::all()
                .with_filters(GetQuery::all().with_id(query.id))
                .stop_mode(StopMode::Forceful)
                .blocking(),
        )
        .await
        .unwrap()
        .first()
        .unwrap()
        .clone();

    assert_eq!(dropped.current_state, QueryState::Stopped);
    assert_eq!(dropped.stop_mode.unwrap(), StopMode::Forceful);
}

#[madsim::test]
async fn drop_query_gracefully() {
    let cluster = Cluster::setup(arb_topology(), arb_worker_config()).await;

    let query: model::query::Model = cluster
        .send(arb_query().block_until(QueryState::Running))
        .await
        .unwrap();

    let dropped: model::query::Model = cluster
        .send(
            DropQuery::all()
                .with_filters(GetQuery::all().with_id(query.id))
                .stop_mode(StopMode::Graceful)
                .blocking(),
        )
        .await
        .unwrap()
        .first()
        .unwrap()
        .clone();

    assert_eq!(dropped.current_state, QueryState::Stopped);
    assert_eq!(dropped.stop_mode.unwrap(), StopMode::Graceful);
}

#[madsim::test]
async fn drop_query_before_running() {
    let cluster = Cluster::setup(arb_topology(), arb_worker_config()).await;

    let query: model::query::Model = cluster
        .send(arb_query().block_until(QueryState::Pending))
        .await
        .unwrap();

    let dropped: model::query::Model = cluster
        .send(
            DropQuery::all()
                .with_filters(GetQuery::all().with_id(query.id))
                .stop_mode(StopMode::Forceful)
                .blocking(),
        )
        .await
        .unwrap()
        .first()
        .unwrap()
        .clone();

    assert!(dropped.current_state.is_terminal());
}

#[madsim::test]
async fn non_blocking_create_reconciles_to_running() {
    let cluster = Cluster::setup(arb_topology(), arb_worker_config()).await;

    let created: model::query::Model = cluster
        .send(arb_query().block_until(QueryState::Pending))
        .await
        .unwrap();

    let query = poll_query_state(
        &cluster,
        created.id,
        QueryState::Running,
        query_reconciliation_deadline(),
    )
    .await;
    assert_eq!(query.current_state, QueryState::Running);
}
