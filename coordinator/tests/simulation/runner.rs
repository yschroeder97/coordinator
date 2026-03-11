#![cfg(madsim)]
use crate::harness::{TestHarness, arb};
use crate::spec::TestSpec;
use crate::workload::{Workload, create_workload, inject_failure_workloads};
use futures::future::join_all;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::Handle;
use model::query;
use model::query::GetQuery;
use model::worker::CreateWorker;
use std::time::Duration;
use tracing::info;

pub async fn run_test(spec: TestSpec) {
    let seed = Handle::current().seed();
    let title = spec.title.as_deref().unwrap_or("unnamed");
    let timeout = spec.timeout();
    let convergence_timeout = spec.convergence_timeout();

    info!("=== simulation test: {title} (seed={seed}) ===");
    info!("timeout={timeout:?}, convergence_timeout={convergence_timeout:?}");

    let result = tokio::time::timeout(timeout, run_test_inner(spec, convergence_timeout)).await;
    match result {
        Ok(()) => info!("=== simulation test PASSED (seed={seed}) ==="),
        Err(_) => panic!("simulation test TIMED OUT after {timeout:?} (seed={seed})"),
    }
}

async fn run_test_inner(spec: TestSpec, convergence_timeout: Duration) {
    let should_buggify = spec
        .buggify
        .unwrap_or_else(|| thread_rng().gen_bool(TestSpec::buggify_probability()));
    if should_buggify {
        madsim::buggify::enable();
        info!("buggify enabled");
    }

    let mut workloads: Vec<Box<dyn Workload>> = match spec.workload {
        Some(ref entries) => entries
            .iter()
            .map(|entry| create_workload(&entry.test_name, &entry.options))
            .collect(),
        None => Vec::new(),
    };

    if spec.run_failure_workloads() {
        inject_failure_workloads(&mut workloads);
    }

    let min_workers = workloads.iter().map(|w| w.min_workers()).max().unwrap_or(1);
    let topology = arb(CreateWorker::topology(min_workers));

    let network = spec.network.clone();
    let harness = TestHarness::start(&topology, network).await.unwrap();

    if workloads.is_empty() {
        info!("no workloads configured, skipping");
        return;
    }

    info!(
        "workloads: [{}]",
        workloads
            .iter()
            .map(|w| w.name())
            .collect::<Vec<_>>()
            .join(", ")
    );

    for w in &mut workloads {
        w.setup(&harness).await;
    }

    join_all(workloads.iter().map(|w| w.start(&harness))).await;

    info!("workloads finished, resetting network");
    harness.reset_network();

    let all_query_ids: Vec<i64> = {
        let results: Vec<(query::Model, Vec<_>)> = harness.send(GetQuery::all()).await.unwrap();
        results.iter().map(|(q, _)| q.id).collect()
    };

    info!("waiting for convergence ({} queries)", all_query_ids.len());
    let converged = harness
        .wait_convergence(&all_query_ids, convergence_timeout)
        .await;
    assert!(
        converged,
        "system did not converge within {convergence_timeout:?}"
    );
    info!("convergence reached");

    for w in &workloads {
        w.check(&harness).await;
        info!("check {}: ok", w.name());
    }
}
