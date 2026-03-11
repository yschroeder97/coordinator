#![cfg(madsim)]
use crate::harness::{TestHarness, arb};
use crate::workload::{
    Invariant, InvariantContext, Workload, WorkloadFactory, check_invariants, derive_query_state,
    generate_weighted, parse_options, run_timed_ops, sleep_strategy,
};
use async_trait::async_trait;
use model::query::fragment;
use model::query::fragment::GetFragment;
use model::query::query_state::QueryState;
use model::query::{CreateQuery, DropQuery, GetQuery, StopMode};
use model::worker::endpoint::HostAddr;
use model::worker::GetWorker;
use model::{query, worker, Generate};
use proptest::prelude::*;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use tracing::{debug, info};

#[derive(Deserialize)]
#[serde(default)]
struct QueryConfig {
    test_duration: f64,
    max_sleep_secs: u64,
    create_weight: u32,
    drop_weight: u32,
    sleep_weight: u32,
    block_until: String,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            test_duration: 30.0,
            max_sleep_secs: 5,
            create_weight: 3,
            drop_weight: 1,
            sleep_weight: 1,
            block_until: "Pending".to_string(),
        }
    }
}

pub struct QueryWorkload {
    test_duration: Duration,
    max_sleep_secs: u64,
    create_weight: u32,
    drop_weight: u32,
    sleep_weight: u32,
    block_until: QueryState,
    state: Mutex<QueryWorkloadState>,
}

#[derive(Default)]
struct QueryWorkloadState {
    initial_capacities: HashMap<HostAddr, i32>,
    max_concurrent_queries: usize,
    created_query_ids: Vec<i64>,
    dropped_query_ids: Vec<i64>,
}

#[derive(Clone, Debug)]
enum QueryOp {
    CreateQuery,
    DropQuery(usize),
    Sleep(Duration),
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

    fn active_queries(&self) -> usize {
        self.created_queries - self.dropped_queries.iter().filter(|&&d| d).count()
    }

    fn valid_ops(
        &self,
        max_sleep_secs: u64,
        create_weight: u32,
        drop_weight: u32,
        sleep_weight: u32,
        max_concurrent: usize,
    ) -> Vec<(u32, BoxedStrategy<QueryOp>)> {
        let mut strategies: Vec<(u32, BoxedStrategy<QueryOp>)> = Vec::new();

        if self.active_queries() < max_concurrent {
            strategies.push((create_weight, Just(QueryOp::CreateQuery).boxed()));
        }

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

        let (_, s) = sleep_strategy(max_sleep_secs, QueryOp::Sleep);
        strategies.push((sleep_weight, s));

        strategies
    }

    fn apply(&mut self, op: &QueryOp) {
        match op {
            QueryOp::CreateQuery => {
                self.created_queries += 1;
                self.dropped_queries.push(false);
            }
            QueryOp::DropQuery(i) => self.dropped_queries[*i] = true,
            QueryOp::Sleep(_) => {}
        }
    }
}

impl QueryWorkload {
    pub const NAME: &str = "Query";

    pub fn from_options(options: &HashMap<String, toml::Value>) -> Self {
        let c: QueryConfig = parse_options(options);
        let block_until = match c.block_until.as_str() {
            "Pending" => QueryState::Pending,
            "Planned" => QueryState::Planned,
            "Registered" => QueryState::Registered,
            "Running" => QueryState::Running,
            other => panic!("invalid block_until: {other}"),
        };
        Self {
            test_duration: Duration::from_secs_f64(c.test_duration),
            max_sleep_secs: c.max_sleep_secs,
            create_weight: c.create_weight,
            drop_weight: c.drop_weight,
            sleep_weight: c.sleep_weight,
            block_until,
            state: Mutex::new(QueryWorkloadState::default()),
        }
    }

    fn generate_op(&self, gen_state: &mut GeneratorState, max_concurrent: usize) -> QueryOp {
        let strategies = gen_state.valid_ops(
            self.max_sleep_secs,
            self.create_weight,
            self.drop_weight,
            self.sleep_weight,
            max_concurrent,
        );
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
                let query = arb(CreateQuery::generate()).block_until(self.block_until);
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
            QueryOp::Sleep(d) => {
                debug!("[{i}] sleep {d:?}");
                tokio::time::sleep(d).await;
            }
        }
    }

}

#[async_trait(?Send)]
impl Workload for QueryWorkload {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn setup(&mut self, harness: &TestHarness) {
        let workers: Vec<worker::Model> =
            harness.send(GetWorker::all()).await.unwrap();
        let max_concurrent = workers
            .iter()
            .filter(|w| w.capacity > 0)
            .map(|w| w.capacity)
            .min()
            .unwrap_or(0) as usize;
        let capacities = workers
            .into_iter()
            .map(|w| (w.host_addr, w.capacity))
            .collect();
        let mut state = self.state.lock().unwrap();
        state.initial_capacities = capacities;
        state.max_concurrent_queries = max_concurrent;
        info!("max_concurrent_queries={max_concurrent}");
    }

    async fn start(&self, harness: &TestHarness) {
        let max_concurrent = self.state.lock().unwrap().max_concurrent_queries;
        info!(
            "running for {:?} (max_concurrent={max_concurrent})",
            self.test_duration,
        );

        let mut gen_state = GeneratorState::new();
        let mut created_ids = Vec::new();
        let mut dropped_ids = Vec::new();

        run_timed_ops(self.test_duration, self.name(), |i| {
            let op = self.generate_op(&mut gen_state, max_concurrent);
            Box::pin(self.execute_op(harness, i, op, &mut created_ids, &mut dropped_ids))
        })
        .await;

        let mut state = self.state.lock().unwrap();
        state.created_query_ids = created_ids;
        state.dropped_query_ids = dropped_ids;
    }

    async fn check(&self, harness: &TestHarness) {
        let (created_ids, dropped_ids, initial_caps) = {
            let state = self.state.lock().unwrap();
            (
                state.created_query_ids.clone(),
                state.dropped_query_ids.clone(),
                state.initial_capacities.clone(),
            )
        };

        let ctx = InvariantContext::new(created_ids, dropped_ids);
        let caps = CapacityConservation {
            initial_capacities: initial_caps,
        };

        check_invariants(
            &[
                &QueryLiveness,
                &QueryTermination,
                &FragmentCoherence,
                &caps,
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
        if ctx.query_ids().is_empty() {
            return;
        }
        let queries: Vec<query::Model> = harness
            .send(GetQuery::all().with_ids(ctx.query_ids().to_vec()))
            .await
            .unwrap();
        let non_settled: Vec<_> = queries
            .iter()
            .filter(|q| q.current_state != QueryState::Running && !q.current_state.is_terminal())
            .map(|q| (q.id, q.current_state))
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
        if ctx.query_ids().is_empty() || ctx.dropped_query_ids().is_empty() {
            return;
        }
        let queries: Vec<query::Model> = harness
            .send(GetQuery::all().with_ids(ctx.query_ids().to_vec()))
            .await
            .unwrap();
        for &qid in ctx.dropped_query_ids() {
            if let Some(q) = queries.iter().find(|q| q.id == qid) {
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
        if ctx.query_ids().is_empty() {
            return;
        }
        let queries: Vec<query::Model> = harness
            .send(GetQuery::all().with_ids(ctx.query_ids().to_vec()))
            .await
            .unwrap();
        for &qid in ctx.query_ids() {
            if let Some(query) = queries.iter().find(|q| q.id == qid) {
                let fragments: Vec<fragment::Model> =
                    harness.send(GetFragment::for_query(qid)).await.unwrap();

                if fragments.is_empty() {
                    continue;
                }

                let expected = derive_query_state(&fragments, query.current_state);
                assert_eq!(
                    query.current_state, expected,
                    "fragment_coherence: query {qid} is {:?} but fragments imply {:?}. fragments: {:?}",
                    query.current_state,
                    expected,
                    fragments.iter().map(|f| (f.id, f.current_state)).collect::<Vec<_>>()
                );
            }
        }
    }
}

pub struct CapacityConservation {
    initial_capacities: HashMap<HostAddr, i32>,
}

#[async_trait(?Send)]
impl Invariant for CapacityConservation {
    fn name(&self) -> &str {
        "capacity_conservation"
    }

    async fn check(&self, harness: &TestHarness, ctx: &InvariantContext) {
        let queries: Vec<query::Model> = if ctx.query_ids().is_empty() {
            Vec::new()
        } else {
            harness
                .send(GetQuery::all().with_ids(ctx.query_ids().to_vec()))
                .await
                .unwrap()
        };
        let workers: Vec<worker::Model> = harness.send(GetWorker::all()).await.unwrap();

        let mut used_per_worker: HashMap<HostAddr, i32> = HashMap::new();
        for q in &queries {
            let fragments: Vec<fragment::Model> =
                harness.send(GetFragment::for_query(q.id)).await.unwrap();
            for f in fragments
                .iter()
                .filter(|f| !f.current_state.is_terminal())
            {
                *used_per_worker.entry(f.host_addr.clone()).or_default() += f.used_capacity;
            }
        }

        for w in &workers {
            if let Some(&initial) = self.initial_capacities.get(&w.host_addr) {
                let used = used_per_worker.get(&w.host_addr).copied().unwrap_or(0);
                assert_eq!(
                    w.capacity + used,
                    initial,
                    "capacity_conservation: worker {} capacity={} used={} expected_initial={}",
                    w.host_addr,
                    w.capacity,
                    used,
                    initial
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
