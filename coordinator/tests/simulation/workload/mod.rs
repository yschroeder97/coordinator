#![cfg(madsim)]

pub mod attrition;
pub mod cluster;
pub mod partition;
pub mod query;

use crate::harness::{TestHarness, arb};
use async_trait::async_trait;
use madsim::rand::Rng;
use model::query::fragment;
use model::query::query_state::QueryState;
use proptest::prelude::*;
use proptest::strategy::Union;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;
use std::time::Duration;
use tracing::info;

pub async fn run_timed_ops<'a, F>(duration: Duration, name: &str, mut step: F) -> usize
where
    F: FnMut(usize) -> Pin<Box<dyn std::future::Future<Output = ()> + 'a>>,
{
    let mut op_index = 0;
    let _ = tokio::time::timeout(duration, async {
        loop {
            step(op_index).await;
            op_index += 1;
        }
    })
    .await;
    info!("{name}: completed {op_index} ops");
    op_index
}

pub fn generate_weighted<Op: Clone + Debug>(strategies: Vec<(u32, BoxedStrategy<Op>)>) -> Op {
    let strategy = Union::new_weighted(strategies);
    arb(strategy)
}

pub fn sleep_strategy<Op>(max_secs: u64, wrap: fn(Duration) -> Op) -> (u32, BoxedStrategy<Op>) {
    (1, (0..=max_secs).prop_map(move |s| wrap(Duration::from_secs(s))).boxed())
}

pub struct InvariantContext {
    created_query_ids: Vec<i64>,
    dropped_query_ids: Vec<i64>,
}

impl InvariantContext {
    pub fn new(created_query_ids: Vec<i64>, dropped_query_ids: Vec<i64>) -> Self {
        Self {
            created_query_ids,
            dropped_query_ids,
        }
    }

    pub fn query_ids(&self) -> &[i64] {
        &self.created_query_ids
    }

    pub fn dropped_query_ids(&self) -> &[i64] {
        &self.dropped_query_ids
    }

    pub fn empty() -> Self {
        Self::new(Vec::new(), Vec::new())
    }
}

#[async_trait(?Send)]
pub trait Invariant {
    fn name(&self) -> &str;
    async fn check(&self, harness: &TestHarness, ctx: &InvariantContext);
}

pub async fn check_invariants(
    invariants: &[&dyn Invariant],
    harness: &TestHarness,
    ctx: &InvariantContext,
) {
    for inv in invariants {
        inv.check(harness, ctx).await;
        info!("invariant {}: ok", inv.name());
    }
}

#[async_trait(?Send)]
pub trait Workload {
    fn name(&self) -> &str;

    fn min_workers(&self) -> u8 {
        1
    }

    async fn setup(&mut self, _harness: &TestHarness) {}

    async fn start(&self, harness: &TestHarness);

    async fn check(&self, _harness: &TestHarness) {}
}

pub struct WorkloadFactory {
    pub name: &'static str,
    pub create: fn(&HashMap<String, toml::Value>) -> Box<dyn Workload>,
}

pub fn parse_options<T: DeserializeOwned + Default>(
    options: &HashMap<String, toml::Value>,
) -> T {
    if options.is_empty() {
        return T::default();
    }
    let table: toml::map::Map<String, toml::Value> = options
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    toml::Value::Table(table)
        .try_into()
        .expect("failed to parse workload options")
}

inventory::collect!(WorkloadFactory);

pub struct FailureInjectorFactory {
    pub name: &'static str,
    pub should_inject: fn(already_added: usize) -> bool,
    pub create: fn() -> Box<dyn Workload>,
}

inventory::collect!(FailureInjectorFactory);

const MAX_FAILURE_INJECTIONS_PER_TYPE: usize = 3;
const BASE_INJECTION_PROBABILITY: f64 = 0.1;

pub fn create_workload(
    test_name: &str,
    options: &HashMap<String, toml::Value>,
) -> Box<dyn Workload> {
    for factory in inventory::iter::<WorkloadFactory> {
        if factory.name == test_name {
            return (factory.create)(options);
        }
    }
    panic!("unknown workload: {test_name}");
}

pub fn inject_failure_workloads(workloads: &mut Vec<Box<dyn Workload>>) {
    let mut rng = madsim::rand::thread_rng();
    for factory in inventory::iter::<FailureInjectorFactory> {
        let mut count = 0;
        while count < MAX_FAILURE_INJECTIONS_PER_TYPE
            && (factory.should_inject)(count)
            && rng.gen_bool(
                (BASE_INJECTION_PROBABILITY / (1.0 + count as f64)).clamp(0.0, 1.0),
            )
        {
            let w = (factory.create)();
            info!("auto-injecting failure workload: {}", w.name());
            workloads.push(w);
            count += 1;
        }
    }
}

pub fn derive_query_state(
    fragments: &[fragment::Model],
    current_state: QueryState,
) -> QueryState {
    if fragments
        .iter()
        .any(|f| f.current_state == fragment::FragmentState::Failed)
    {
        return QueryState::Failed;
    }
    if fragments
        .iter()
        .all(|f| f.current_state == fragment::FragmentState::Completed)
    {
        return QueryState::Completed;
    }
    if fragments
        .iter()
        .all(|f| f.current_state == fragment::FragmentState::Stopped)
    {
        return QueryState::Stopped;
    }
    if fragments.iter().all(|f| {
        matches!(
            f.current_state,
            fragment::FragmentState::Running | fragment::FragmentState::Started
        )
    }) {
        return QueryState::Running;
    }
    if fragments
        .iter()
        .all(|f| f.current_state == fragment::FragmentState::Registered)
    {
        return QueryState::Registered;
    }
    current_state
}


