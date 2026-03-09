#![cfg(madsim)]
use crate::harness::{TestHarness, arb_topology_min};
use crate::spec::TestSpec;
use crate::workload::{TestWorkload, build_random_workloads, build_workloads};
use futures::future::join_all;
use madsim::rand::Rng;
use madsim::runtime::Handle;
use model::query::GetQuery;
use tokio_util::sync::CancellationToken;
use tracing::info;

async fn run_workload(w: &dyn TestWorkload, harness: &TestHarness, cancel: CancellationToken) {
    let trigger = cancel.clone();
    w.start(harness, cancel).await;
    if w.is_primary() {
        info!("primary workload completed, cancelling fault workloads");
        trigger.cancel();
    }
}

pub async fn run_test(spec: TestSpec) {
    let seed = Handle::current().seed();
    let title = spec.title.as_deref().unwrap_or("random");
    let timeout = spec.timeout();
    let min_workers = spec.min_workers();

    info!("=== simulation test: {title} (seed={seed}) ===");
    info!("timeout={timeout:?} min_workers={min_workers}");

    let result = tokio::time::timeout(timeout, run_test_inner(spec)).await;
    match result {
        Ok(()) => info!("=== simulation test PASSED (seed={seed}) ==="),
        Err(_) => panic!("simulation test TIMED OUT after {timeout:?} (seed={seed})"),
    }
}

async fn run_test_inner(spec: TestSpec) {
    let min_workers = spec.min_workers();
    let should_buggify = spec
        .buggify
        .unwrap_or_else(|| madsim::rand::thread_rng().gen_bool(0.25));
    if should_buggify {
        madsim::buggify::enable();
        info!("buggify enabled");
    }

    let topology = arb_topology_min(min_workers);

    let network = spec.network.clone();
    let harness = TestHarness::start(&topology, network).await.unwrap();
    harness.register_workers().await;

    let workloads = match spec.workload {
        Some(ref entries) => build_workloads(entries),
        None => build_random_workloads(),
    };

    info!(
        "workloads: [{}]",
        workloads
            .iter()
            .map(|w| w.description())
            .collect::<Vec<_>>()
            .join(", ")
    );

    for w in &workloads {
        w.setup(&harness).await;
    }

    let cancel = CancellationToken::new();

    let futs: Vec<_> = workloads
        .iter()
        .map(|w| run_workload(w.as_ref(), &harness, cancel.clone()))
        .collect();

    join_all(futs).await;

    harness.reset_network();

    let created_ids: Vec<i64> = {
        let queries: Vec<model::query::Model> =
            harness.send(GetQuery::all()).await.unwrap();
        queries.iter().map(|q| q.id).collect()
    };

    if !created_ids.is_empty() {
        harness.wait_for_convergence(&created_ids).await;
    }

    let mut all_passed = true;
    for w in &workloads {
        let passed = w.check(&harness).await;
        if passed {
            info!("CHECK {}: PASSED", w.description());
        } else {
            info!("CHECK {}: FAILED", w.description());
            all_passed = false;
        }
    }

    assert!(all_passed, "one or more workload checks failed");
}
