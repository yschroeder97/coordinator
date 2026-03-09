#![cfg(madsim)]
use crate::harness::{TestHarness, arb, arb_query};
use crate::workload::TestWorkload;
use futures::future::BoxFuture;
use model::query::fragment::{FragmentState, GetFragment};
use model::query::query_state::QueryState;
use model::query::{DropQuery, GetQuery};
use model::worker::endpoint::HostAddr;
use model::worker::{DropWorker, GetWorker, WorkerState};
use madsim::rand::Rng;
use proptest::prelude::*;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

const DEFAULT_MIN_OPS: usize = 10;
const DEFAULT_MAX_OPS: usize = 50;
const DEFAULT_MAX_SLEEP_SECS: u64 = 5;
const RANDOM_MIN_OPS_LO: usize = 5;
const RANDOM_MIN_OPS_HI: usize = 20;
const RANDOM_MAX_OPS_OFFSET: usize = 10;
const RANDOM_MAX_OPS_CEIL: usize = 60;
const RANDOM_MAX_SLEEP_LO: u64 = 1;
const RANDOM_MAX_SLEEP_HI: u64 = 5;

pub struct CrudWorkload {
    min_ops: usize,
    max_ops: usize,
    max_sleep_secs: u64,
    state: Mutex<CrudState>,
}

#[derive(Default)]
struct CrudState {
    initial_capacities: HashMap<HostAddr, i32>,
    result: WorkloadResult,
}

#[derive(Default, Debug)]
struct WorkloadResult {
    created_query_ids: Vec<i64>,
    dropped_query_ids: Vec<i64>,
    registered_worker_indices: Vec<usize>,
}

#[derive(Clone, Debug)]
enum WorkloadOp {
    CreateWorker(usize),
    DropWorker(usize),
    CreateQuery,
    DropQuery(usize),
    Sleep(Duration),
}

struct GeneratorState {
    registered_workers: Vec<bool>,
    created_queries: usize,
    dropped_queries: Vec<bool>,
}

impl GeneratorState {
    fn new(num_workers: usize) -> Self {
        Self {
            registered_workers: vec![true; num_workers],
            created_queries: 0,
            dropped_queries: Vec::new(),
        }
    }

    fn valid_ops(&self, max_sleep_secs: u64) -> Vec<BoxedStrategy<WorkloadOp>> {
        let mut strategies: Vec<BoxedStrategy<WorkloadOp>> = Vec::new();

        let unregistered: Vec<usize> = self
            .registered_workers
            .iter()
            .enumerate()
            .filter(|(_, r)| !**r)
            .map(|(i, _)| i)
            .collect();
        if !unregistered.is_empty() {
            let indices = unregistered.clone();
            strategies.push(
                prop::sample::select(indices)
                    .prop_map(WorkloadOp::CreateWorker)
                    .boxed(),
            );
        }

        let registered: Vec<usize> = self
            .registered_workers
            .iter()
            .enumerate()
            .filter(|(_, r)| **r)
            .map(|(i, _)| i)
            .collect();
        if !registered.is_empty() {
            let indices = registered.clone();
            strategies.push(
                prop::sample::select(indices)
                    .prop_map(WorkloadOp::DropWorker)
                    .boxed(),
            );
        }

        strategies.push(Just(WorkloadOp::CreateQuery).boxed());

        let droppable: Vec<usize> = self
            .dropped_queries
            .iter()
            .enumerate()
            .filter(|(_, dropped)| !**dropped)
            .map(|(i, _)| i)
            .collect();
        if !droppable.is_empty() {
            let indices = droppable.clone();
            strategies.push(
                prop::sample::select(indices)
                    .prop_map(WorkloadOp::DropQuery)
                    .boxed(),
            );
        }

        strategies.push(
            (0..=max_sleep_secs)
                .prop_map(|s| WorkloadOp::Sleep(Duration::from_secs(s)))
                .boxed(),
        );

        strategies
    }

    fn apply(&mut self, op: &WorkloadOp) {
        match op {
            WorkloadOp::CreateWorker(i) => self.registered_workers[*i] = true,
            WorkloadOp::DropWorker(_) => {}
            WorkloadOp::CreateQuery => {
                self.created_queries += 1;
                self.dropped_queries.push(false);
            }
            WorkloadOp::DropQuery(i) => self.dropped_queries[*i] = true,
            WorkloadOp::Sleep(_) => {}
        }
    }
}

impl CrudWorkload {
    pub fn new(
        min_ops: Option<usize>,
        max_ops: Option<usize>,
        max_sleep_secs: Option<u64>,
    ) -> Self {
        Self {
            min_ops: min_ops.unwrap_or(DEFAULT_MIN_OPS),
            max_ops: max_ops.unwrap_or(DEFAULT_MAX_OPS),
            max_sleep_secs: max_sleep_secs.unwrap_or(DEFAULT_MAX_SLEEP_SECS),
            state: Mutex::new(CrudState::default()),
        }
    }

    pub fn random() -> Self {
        let mut rng = madsim::rand::thread_rng();
        let min_ops = rng.gen_range(RANDOM_MIN_OPS_LO..=RANDOM_MIN_OPS_HI);
        let max_ops = rng.gen_range(min_ops + RANDOM_MAX_OPS_OFFSET..=RANDOM_MAX_OPS_CEIL);
        let max_sleep_secs = rng.gen_range(RANDOM_MAX_SLEEP_LO..=RANDOM_MAX_SLEEP_HI);
        Self::new(Some(min_ops), Some(max_ops), Some(max_sleep_secs))
    }

    fn generate_ops(&self, num_workers: usize) -> Vec<WorkloadOp> {
        let op_count = arb(self.min_ops..=self.max_ops);
        let mut state = GeneratorState::new(num_workers);
        let mut ops = Vec::with_capacity(op_count);

        for _ in 0..op_count {
            let strategies = state.valid_ops(self.max_sleep_secs);
            let strategy = proptest::strategy::Union::new(strategies);
            let op = arb(strategy);
            state.apply(&op);
            ops.push(op);
        }

        ops
    }

    async fn execute_ops(&self, harness: &TestHarness, ops: Vec<WorkloadOp>) {
        let mut result = WorkloadResult::default();

        for (i, op) in ops.into_iter().enumerate() {
            match op {
                WorkloadOp::CreateWorker(idx) => {
                    info!("workload[{i}]: CreateWorker({idx})");
                    harness.ensure_node_running(idx);
                    let worker = harness.worker_config(idx);

                    let workers: Vec<model::worker::Model> =
                        harness.send(GetWorker::all()).await.unwrap();
                    let exists = workers.iter().any(|w| w.host_addr == worker.host_addr);

                    if !exists {
                        let resp: anyhow::Result<model::worker::Model> =
                            harness.send(worker).await;
                        match resp {
                            Ok(_) => {
                                info!("workload[{i}]: CreateWorker({idx}) succeeded");
                                result.registered_worker_indices.push(idx);
                            }
                            Err(e) => info!("workload[{i}]: CreateWorker({idx}) failed: {e}"),
                        }
                    } else {
                        info!("workload[{i}]: CreateWorker({idx}) already exists, skipping");
                        result.registered_worker_indices.push(idx);
                    }
                }
                WorkloadOp::DropWorker(idx) => {
                    info!("workload[{i}]: DropWorker({idx})");
                    let host = harness.worker_host(idx);
                    let resp: anyhow::Result<model::worker::Model> =
                        harness.send(DropWorker::new(host)).await;
                    match resp {
                        Ok(_) => info!("workload[{i}]: DropWorker({idx}) succeeded"),
                        Err(e) => info!("workload[{i}]: DropWorker({idx}) rejected: {e}"),
                    }
                }
                WorkloadOp::CreateQuery => {
                    info!("workload[{i}]: CreateQuery");
                    let query = arb_query().block_until(QueryState::Pending);
                    let resp: anyhow::Result<model::query::Model> = harness.send(query).await;
                    match resp {
                        Ok(q) => {
                            info!("workload[{i}]: CreateQuery succeeded (id={})", q.id);
                            result.created_query_ids.push(q.id);
                        }
                        Err(e) => info!("workload[{i}]: CreateQuery failed: {e}"),
                    }
                }
                WorkloadOp::DropQuery(idx) => {
                    if idx < result.created_query_ids.len() {
                        let qid = result.created_query_ids[idx];
                        info!("workload[{i}]: DropQuery(idx={idx}, id={qid})");
                        let resp: anyhow::Result<Vec<model::query::Model>> = harness
                            .send(
                                DropQuery::all()
                                    .with_filters(GetQuery::all().with_id(qid)),
                            )
                            .await;
                        match resp {
                            Ok(_) => {
                                info!("workload[{i}]: DropQuery({qid}) succeeded");
                                result.dropped_query_ids.push(qid);
                            }
                            Err(e) => info!("workload[{i}]: DropQuery({qid}) failed: {e}"),
                        }
                    } else {
                        info!(
                            "workload[{i}]: DropQuery({idx}) skipped, query not yet created"
                        );
                    }
                }
                WorkloadOp::Sleep(d) => {
                    info!("workload[{i}]: Sleep({d:?})");
                    tokio::time::sleep(d).await;
                }
            }
        }

        *self.state.lock().unwrap() = CrudState {
            result,
            ..CrudState::default()
        };
    }

    fn check_liveness(&self, queries: &[model::query::Model]) -> bool {
        let non_settled: Vec<_> = queries
            .iter()
            .filter(|q| q.current_state != QueryState::Running && !q.current_state.is_terminal())
            .map(|q| (q.id, q.current_state))
            .collect();

        if !non_settled.is_empty() {
            info!("FAIL liveness: queries not Running or terminal: {non_settled:?}");
            return false;
        }
        true
    }

    fn check_terminal_correctness(
        &self,
        harness_queries: &[model::query::Model],
        dropped_ids: &[i64],
    ) -> bool {
        for &qid in dropped_ids {
            if let Some(q) = harness_queries.iter().find(|q| q.id == qid) {
                if !q.current_state.is_terminal() {
                    info!(
                        "FAIL terminal_correctness: dropped query {} in state {:?}",
                        q.id, q.current_state
                    );
                    return false;
                }
            }
        }
        true
    }

    fn check_reconciliation_progress(&self, queries: &[model::query::Model]) -> bool {
        let stuck: Vec<_> = queries
            .iter()
            .filter(|q| {
                matches!(
                    q.current_state,
                    QueryState::Pending | QueryState::Planned | QueryState::Registered
                )
            })
            .map(|q| (q.id, q.current_state))
            .collect();

        if !stuck.is_empty() {
            info!("FAIL reconciliation_progress: queries stuck in intermediate states: {stuck:?}");
            return false;
        }
        true
    }

    fn check_worker_convergence(&self, workers: &[model::worker::Model]) -> bool {
        if let Some(w) = workers.iter().find(|w| {
            w.desired_state == model::worker::DesiredWorkerState::Active
                && w.current_state != WorkerState::Active
        }) {
            info!(
                "FAIL worker_convergence: worker {} desired=Active current={:?}",
                w.host_addr, w.current_state
            );
            return false;
        }
        true
    }
}

fn derive_query_state(fragments: &[model::query::fragment::Model]) -> Option<QueryState> {
    if fragments.is_empty() {
        return None;
    }
    if fragments
        .iter()
        .any(|f| f.current_state == FragmentState::Failed)
    {
        Some(QueryState::Failed)
    } else if fragments
        .iter()
        .all(|f| f.current_state == FragmentState::Completed)
    {
        Some(QueryState::Completed)
    } else if fragments
        .iter()
        .all(|f| f.current_state == FragmentState::Stopped)
    {
        Some(QueryState::Stopped)
    } else if fragments
        .iter()
        .all(|f| matches!(f.current_state, FragmentState::Running | FragmentState::Started))
    {
        Some(QueryState::Running)
    } else if fragments
        .iter()
        .all(|f| f.current_state == FragmentState::Registered)
    {
        Some(QueryState::Registered)
    } else {
        None
    }
}

impl TestWorkload for CrudWorkload {
    fn description(&self) -> &str {
        "CrudWorkload"
    }

    fn is_primary(&self) -> bool { true }

    fn setup<'a>(&'a self, harness: &'a TestHarness) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let workers: Vec<model::worker::Model> =
                harness.send(GetWorker::all()).await.unwrap();
            let capacities = workers
                .into_iter()
                .map(|w| (w.host_addr, w.capacity))
                .collect();
            self.state.lock().unwrap().initial_capacities = capacities;
        })
    }

    fn start<'a>(&'a self, harness: &'a TestHarness, _cancel: CancellationToken) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let ops = self.generate_ops(harness.num_workers());
            info!(
                "{}: executing {} ops (min={}, max={}, sleep_max={}s)",
                self.description(),
                ops.len(),
                self.min_ops,
                self.max_ops,
                self.max_sleep_secs
            );
            self.execute_ops(harness, ops).await;
        })
    }

    fn check<'a>(&'a self, harness: &'a TestHarness) -> BoxFuture<'a, bool> {
        Box::pin(async move {
        let (created_ids, dropped_ids, initial_caps) = {
            let state = self.state.lock().unwrap();
            (
                state.result.created_query_ids.clone(),
                state.result.dropped_query_ids.clone(),
                state.initial_capacities.clone(),
            )
        };

        let queries: Vec<model::query::Model> = if created_ids.is_empty() {
            Vec::new()
        } else {
            harness
                .send(GetQuery::all().with_ids(created_ids.clone()))
                .await
                .unwrap()
        };
        let workers: Vec<model::worker::Model> = harness.send(GetWorker::all()).await.unwrap();

        let mut ok = true;

        ok = self.check_liveness(&queries) && ok;
        ok = self.check_terminal_correctness(&queries, &dropped_ids) && ok;
        ok = self.check_reconciliation_progress(&queries) && ok;
        ok = self.check_worker_convergence(&workers) && ok;

        for &qid in &created_ids {
            if let Some(query) = queries.iter().find(|q| q.id == qid) {
                let fragments: Vec<model::query::fragment::Model> =
                    harness.send(GetFragment::for_query(qid)).await.unwrap();

                let derived = derive_query_state(&fragments);
                if let Some(expected) = derived {
                    if query.current_state != expected {
                        info!(
                            "FAIL query_state_matches_fragments: query {qid} is {:?} but fragments imply {:?}. Fragments: {:?}",
                            query.current_state,
                            expected,
                            fragments.iter().map(|f| (f.id, f.current_state)).collect::<Vec<_>>()
                        );
                        ok = false;
                    }
                }
            }
        }

        let mut used_per_worker: HashMap<HostAddr, i32> = HashMap::new();
        for q in &queries {
            let fragments: Vec<model::query::fragment::Model> =
                harness.send(GetFragment::for_query(q.id)).await.unwrap();
            for f in fragments
                .iter()
                .filter(|f| !f.current_state.is_terminal())
            {
                *used_per_worker.entry(f.host_addr.clone()).or_default() += f.used_capacity;
            }
        }

        for w in &workers {
            if let Some(&initial) = initial_caps.get(&w.host_addr) {
                let used = used_per_worker.get(&w.host_addr).copied().unwrap_or(0);
                if w.capacity + used != initial {
                    info!(
                        "FAIL capacity_conservation: worker {} capacity={} used={} expected_initial={}",
                        w.host_addr, w.capacity, used, initial
                    );
                    ok = false;
                }
            }
        }

        ok
        })
    }
}
