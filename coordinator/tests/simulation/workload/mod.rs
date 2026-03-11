#![cfg(madsim)]

pub mod attrition;
pub mod cluster;
pub mod partition;
pub mod query;

use crate::harness::TestHarness;
use async_trait::async_trait;
use madsim::rand::Rng;
use model::query::fragment;
use model::query::query_state::QueryState;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use tracing::info;

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

pub fn derive_query_state(fragments: &[fragment::Model]) -> QueryState {
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
    QueryState::Pending
}


