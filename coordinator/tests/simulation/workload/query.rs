#![cfg(madsim)]
use crate::harness::{TestHarness, arb};
use crate::workload::{
    Invariant, InvariantContext, Workload, WorkloadFactory, check_invariants, derive_query_state,
    generate_weighted, parse_options, precompute_delays,
};
use async_trait::async_trait;
use model::query::fragment;
use model::query::{CreateQuery, DropQuery, GetQuery, StopMode};
use model::query::query_state::QueryState;
use model::worker::endpoint::HostAddr;
use model::worker::GetWorker;
use model::{query, worker, Generate};
use proptest::prelude::*;
use serde::Deserialize;
use model::worker::WorkerState;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::{debug, info};

const DEFAULT_NUM_OPS: usize = 15;
const DEFAULT_TEST_DURATION: f64 = 30.0;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct QueryConfig {
    num_ops: usize,
    test_duration: f64,
    create_weight: u32,
    drop_weight: u32,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            num_ops: DEFAULT_NUM_OPS,
            test_duration: DEFAULT_TEST_DURATION,
            create_weight: 3,
            drop_weight: 1,
        }
    }
}

pub struct QueryWorkload {
    num_ops: usize,
    test_duration: Duration,
    create_weight: u32,
    drop_weight: u32,
    state: RefCell<QueryWorkloadState>,
}

#[derive(Default)]
struct QueryWorkloadState {
    created_query_ids: Vec<i64>,
    dropped_query_ids: Vec<i64>,
}

#[derive(Clone, Debug)]
enum QueryOp {
    CreateQuery,
    DropQuery(usize),
}

struct GeneratorState {
    created_queries: usize,
    dropped_queries: Vec<bool>,
}

impl GeneratorState {
    fn new() -> Self {
        Self {
            created_queries: 0,
            dropped_queries: Vec::new(),
        }
    }

    fn valid_ops(
        &self,
        create_weight: u32,
        drop_weight: u32,
    ) -> Vec<(u32, BoxedStrategy<QueryOp>)> {
        let mut strategies: Vec<(u32, BoxedStrategy<QueryOp>)> = Vec::new();

        strategies.push((create_weight, Just(QueryOp::CreateQuery).boxed()));

        let droppable: Vec<usize> = self
            .dropped_queries
            .iter()
            .enumerate()
            .filter(|(_, dropped)| !**dropped)
            .map(|(i, _)| i)
            .collect();
        if !droppable.is_empty() {
            strategies.push((
                drop_weight,
                prop::sample::select(droppable)
                    .prop_map(QueryOp::DropQuery)
                    .boxed(),
            ));
        }

        strategies
    }

    fn apply(&mut self, op: &QueryOp) {
        match op {
            QueryOp::CreateQuery => {
                self.created_queries += 1;
                self.dropped_queries.push(false);
            }
            QueryOp::DropQuery(i) => self.dropped_queries[*i] = true,
        }
    }
}

impl QueryWorkload {
    pub const NAME: &str = "Query";

    pub fn from_options(options: &HashMap<String, toml::Value>) -> Self {
        let c: QueryConfig = parse_options(options);
        Self {
            num_ops: c.num_ops,
            test_duration: Duration::from_secs_f64(c.test_duration),
            create_weight: c.create_weight,
            drop_weight: c.drop_weight,
            state: RefCell::new(QueryWorkloadState::default()),
        }
    }

    fn generate_op(&self, gen_state: &mut GeneratorState) -> QueryOp {
        let strategies = gen_state.valid_ops(self.create_weight, self.drop_weight);
        let op = generate_weighted(strategies);
        gen_state.apply(&op);
        op
    }

    async fn execute_op(
        &self,
        harness: &TestHarness,
        i: usize,
        op: QueryOp,
        created_ids: &mut Vec<i64>,
        dropped_ids: &mut Vec<i64>,
    ) {
        match op {
            QueryOp::CreateQuery => {
                let mut query = arb(CreateQuery::generate());
                query.block_until = QueryState::Pending;
                let resp: anyhow::Result<query::Model> = harness.send(query).await;
                match resp {
                    Ok(q) => {
                        debug!("[{i}] create -> id={}", q.id);
                        created_ids.push(q.id);
                    }
                    Err(e) => info!("[{i}] create failed: {e}"),
                }
            }
            QueryOp::DropQuery(idx) => {
                if idx < created_ids.len() {
                    let qid = created_ids[idx];
                    let stop_mode: StopMode = arb(any::<StopMode>());
                    let resp: anyhow::Result<Vec<query::Model>> = harness
                        .send(
                            DropQuery::all()
                                .stop_mode(stop_mode)
                                .with_filters(GetQuery::all().with_id(qid)),
                        )
                        .await;
                    match resp {
                        Ok(_) => {
                            debug!("[{i}] drop id={qid}");
                            dropped_ids.push(qid);
                        }
                        Err(e) => info!("[{i}] drop id={qid} failed: {e}"),
                    }
                }
            }
        }
    }

}

#[async_trait(?Send)]
impl Workload for QueryWorkload {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn start(&self, harness: &TestHarness) {
        info!(
            "{}: running {} ops over {:?}",
            self.name(), self.num_ops, self.test_duration,
        );

        let delays = precompute_delays(self.num_ops, self.test_duration);
        let mut gen_state = GeneratorState::new();
        let mut created_ids = Vec::new();
        let mut dropped_ids = Vec::new();

        for i in 0..self.num_ops {
            let op = self.generate_op(&mut gen_state);
            self.execute_op(harness, i, op, &mut created_ids, &mut dropped_ids)
                .await;
            tokio::time::sleep(delays[i]).await;
        }
        info!("{}: completed {} ops", self.name(), self.num_ops);

        let mut state = self.state.borrow_mut();
        state.created_query_ids = created_ids;
        state.dropped_query_ids = dropped_ids;
    }

    async fn check(&self, harness: &TestHarness) {
        let (created_ids, dropped_ids) = {
            let state = self.state.borrow();
            (
                state.created_query_ids.clone(),
                state.dropped_query_ids.clone(),
            )
        };

        let ctx = InvariantContext::new(created_ids, dropped_ids);

        check_invariants(
            &[
                &QueryLiveness,
                &QueryTermination,
                &TerminalFragmentCleanup,
                &FragmentCoherence,
                &FragmentPlacementValidity,
                &RpcStateConsistency,
            ],
            harness,
            &ctx,
        )
        .await;
    }
}

pub struct QueryLiveness;

#[async_trait(?Send)]
impl Invariant for QueryLiveness {
    fn name(&self) -> &str {
        "query_liveness"
    }

    async fn check(&self, harness: &TestHarness, ctx: &InvariantContext) {
        let results: Vec<(query::Model, Vec<fragment::Model>)> = harness
            .send(GetQuery::all().with_ids(ctx.query_ids().to_vec()))
            .await
            .unwrap();
        let non_settled: Vec<_> = results
            .iter()
            .filter(|(q, _)| q.current_state != QueryState::Running && !q.current_state.is_terminal())
            .map(|(q, _)| (q.id, q.current_state))
            .collect();
        assert!(
            non_settled.is_empty(),
            "query_liveness: queries not Running or terminal: {non_settled:?}"
        );
    }
}

pub struct QueryTermination;

#[async_trait(?Send)]
impl Invariant for QueryTermination {
    fn name(&self) -> &str {
        "query_termination"
    }

    async fn check(&self, harness: &TestHarness, ctx: &InvariantContext) {
        let results: Vec<(query::Model, Vec<fragment::Model>)> = harness
            .send(GetQuery::all().with_ids(ctx.query_ids().to_vec()))
            .await
            .unwrap();
        for &qid in ctx.dropped_query_ids() {
            if let Some((q, _)) = results.iter().find(|(q, _)| q.id == qid) {
                assert!(
                    q.current_state.is_terminal(),
                    "query_termination: dropped query {} in state {:?}",
                    q.id,
                    q.current_state
                );
            }
        }
    }
}

pub struct FragmentCoherence;

#[async_trait(?Send)]
impl Invariant for FragmentCoherence {
    fn name(&self) -> &str {
        "fragment_coherence"
    }

    async fn check(&self, harness: &TestHarness, ctx: &InvariantContext) {
        let results: Vec<(query::Model, Vec<fragment::Model>)> = harness
            .send(
                GetQuery::all()
                    .with_ids(ctx.query_ids().to_vec())
                    .with_fragments(),
            )
            .await
            .unwrap();
        for (query, fragments) in &results {
            if fragments.is_empty() {
                continue;
            }

            let expected = derive_query_state(fragments, query.current_state);
            assert_eq!(
                query.current_state, expected,
                "fragment_coherence: query {} is {:?} but fragments imply {:?}. fragments: {:?}",
                query.id,
                query.current_state,
                expected,
                fragments.iter().map(|f| (f.id, f.current_state)).collect::<Vec<_>>()
            );
        }
    }
}

pub struct TerminalFragmentCleanup;

#[async_trait(?Send)]
impl Invariant for TerminalFragmentCleanup {
    fn name(&self) -> &str {
        "terminal_fragment_cleanup"
    }

    async fn check(&self, harness: &TestHarness, ctx: &InvariantContext) {
        let results: Vec<(query::Model, Vec<fragment::Model>)> = harness
            .send(
                GetQuery::all()
                    .with_ids(ctx.dropped_query_ids().to_vec())
                    .with_fragments(),
            )
            .await
            .unwrap();
        for (query, fragments) in &results {
            for f in fragments {
                assert!(
                    f.current_state.is_terminal(),
                    "terminal_fragment_cleanup: fragment {} of terminal query {} in state {:?}",
                    f.id,
                    query.id,
                    f.current_state
                );
            }
        }
    }
}

pub struct RpcStateConsistency;

#[async_trait(?Send)]
impl Invariant for RpcStateConsistency {
    fn name(&self) -> &str {
        "rpc_state_consistency"
    }

    async fn check(&self, harness: &TestHarness, ctx: &InvariantContext) {
        let results: Vec<(query::Model, Vec<fragment::Model>)> = harness
            .send(
                GetQuery::all()
                    .with_ids(ctx.query_ids().to_vec())
                    .with_fragments(),
            )
            .await
            .unwrap();

        let worker_statuses = harness.query_worker_statuses().await;
        let mut rpc_active_fragment_ids: HashSet<u64> = HashSet::new();
        for resp in worker_statuses.values() {
            for aq in &resp.active_queries {
                rpc_active_fragment_ids.insert(aq.query_id);
            }
        }

        for (q, fragments) in &results {
            let active_fragment_ids: Vec<u64> = fragments
                .iter()
                .filter(|f| !f.current_state.is_terminal())
                .map(|f| f.id as u64)
                .collect();

            if q.current_state == QueryState::Running && !active_fragment_ids.is_empty() {
                let any_on_worker = active_fragment_ids
                    .iter()
                    .any(|fid| rpc_active_fragment_ids.contains(fid));
                assert!(
                    any_on_worker,
                    "rpc_state_consistency: query {} is Running with fragments {:?} but no worker reports them active",
                    q.id, active_fragment_ids
                );
            }

            if q.current_state.is_terminal() {
                for &fid in &active_fragment_ids {
                    assert!(
                        !rpc_active_fragment_ids.contains(&fid),
                        "rpc_state_consistency: terminal query {} has fragment {} still active on a worker",
                        q.id, fid
                    );
                }
            }
        }
    }
}

pub struct FragmentPlacementValidity;

#[async_trait(?Send)]
impl Invariant for FragmentPlacementValidity {
    fn name(&self) -> &str {
        "fragment_placement_validity"
    }

    async fn check(&self, harness: &TestHarness, ctx: &InvariantContext) {
        let workers: Vec<worker::Model> = harness.send(GetWorker::all()).await.unwrap();
        let active_hosts: HashSet<HostAddr> = workers
            .iter()
            .filter(|w| w.current_state == WorkerState::Active)
            .map(|w| w.host_addr.clone())
            .collect();

        let results: Vec<(query::Model, Vec<fragment::Model>)> = harness
            .send(
                GetQuery::all()
                    .with_ids(ctx.query_ids().to_vec())
                    .with_fragments(),
            )
            .await
            .unwrap();
        for (query, fragments) in &results {
            for f in fragments.iter().filter(|f| !f.current_state.is_terminal()) {
                assert!(
                    active_hosts.contains(&f.host_addr),
                    "fragment_placement_validity: fragment {} of query {} on non-active worker {}",
                    f.id,
                    query.id,
                    f.host_addr
                );
            }
        }
    }
}

inventory::submit! {
    WorkloadFactory {
        name: QueryWorkload::NAME,
        create: |opts| Box::new(QueryWorkload::from_options(opts)),
    }
}
