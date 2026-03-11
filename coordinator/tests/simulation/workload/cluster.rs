#![cfg(madsim)]
use crate::harness::{TestHarness, arb};
use crate::workload::{Workload, WorkloadFactory, parse_options};
use async_trait::async_trait;
use model::worker;
use model::worker::{DesiredWorkerState, DropWorker, GetWorker, WorkerState};
use proptest::prelude::*;
use proptest::strategy::Union;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use tracing::info;

#[derive(Deserialize)]
#[serde(default)]
struct ClusterConfig {
    test_duration: f64,
    max_sleep_secs: u64,
    create_weight: u32,
    drop_weight: u32,
    sleep_weight: u32,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            test_duration: 30.0,
            max_sleep_secs: 5,
            create_weight: 3,
            drop_weight: 1,
            sleep_weight: 1,
        }
    }
}

pub struct ClusterWorkload {
    test_duration: Duration,
    max_sleep_secs: u64,
    create_weight: u32,
    drop_weight: u32,
    sleep_weight: u32,
    state: Mutex<ClusterState>,
}

#[derive(Default)]
struct ClusterState {
    registered_worker_indices: Vec<usize>,
}

#[derive(Clone, Debug)]
enum ClusterOp {
    CreateWorker(usize),
    DropWorker(usize),
    Sleep(Duration),
}

struct GeneratorState {
    registered: Vec<bool>,
}

impl GeneratorState {
    fn new(num_workers: usize) -> Self {
        Self {
            registered: vec![true; num_workers],
        }
    }

    fn valid_ops(
        &self,
        max_sleep_secs: u64,
        create_weight: u32,
        drop_weight: u32,
        sleep_weight: u32,
    ) -> Vec<(u32, BoxedStrategy<ClusterOp>)> {
        let mut strategies: Vec<(u32, BoxedStrategy<ClusterOp>)> = Vec::new();

        let unregistered: Vec<usize> = self
            .registered
            .iter()
            .enumerate()
            .filter(|(_, r)| !**r)
            .map(|(i, _)| i)
            .collect();
        if !unregistered.is_empty() {
            strategies.push((
                create_weight,
                prop::sample::select(unregistered)
                    .prop_map(ClusterOp::CreateWorker)
                    .boxed(),
            ));
        }

        let registered: Vec<usize> = self
            .registered
            .iter()
            .enumerate()
            .filter(|(_, r)| **r)
            .map(|(i, _)| i)
            .collect();
        if !registered.is_empty() {
            strategies.push((
                drop_weight,
                prop::sample::select(registered)
                    .prop_map(ClusterOp::DropWorker)
                    .boxed(),
            ));
        }

        strategies.push((
            sleep_weight,
            (0..=max_sleep_secs)
                .prop_map(|s| ClusterOp::Sleep(Duration::from_secs(s)))
                .boxed(),
        ));

        strategies
    }

    fn apply(&mut self, op: &ClusterOp) {
        match op {
            ClusterOp::CreateWorker(i) => self.registered[*i] = true,
            ClusterOp::DropWorker(_) | ClusterOp::Sleep(_) => {}
        }
    }
}

impl ClusterWorkload {
    pub const NAME: &str = "Cluster";

    pub fn from_options(options: &HashMap<String, toml::Value>) -> Self {
        let c: ClusterConfig = parse_options(options);
        Self {
            test_duration: Duration::from_secs_f64(c.test_duration),
            max_sleep_secs: c.max_sleep_secs,
            create_weight: c.create_weight,
            drop_weight: c.drop_weight,
            sleep_weight: c.sleep_weight,
            state: Mutex::new(ClusterState::default()),
        }
    }

    fn generate_op(&self, gen_state: &mut GeneratorState) -> ClusterOp {
        let strategies = gen_state.valid_ops(
            self.max_sleep_secs,
            self.create_weight,
            self.drop_weight,
            self.sleep_weight,
        );
        let strategy = Union::new_weighted(strategies);
        let op = arb(strategy);
        gen_state.apply(&op);
        op
    }

    async fn execute_op(
        &self,
        harness: &TestHarness,
        i: usize,
        op: ClusterOp,
        result: &mut ClusterState,
    ) {
        match op {
            ClusterOp::CreateWorker(idx) => {
                info!("cluster[{i}]: CreateWorker({idx})");
                harness.ensure_node_running(idx);
                let worker = harness.worker_config(idx);

                let workers: Vec<worker::Model> =
                    harness.send(GetWorker::all()).await.unwrap();
                let exists = workers.iter().any(|w| w.host_addr == worker.host_addr);

                if !exists {
                    let resp: anyhow::Result<worker::Model> =
                        harness.send(worker).await;
                    match resp {
                        Ok(_) => {
                            info!("cluster[{i}]: CreateWorker({idx}) succeeded");
                            result.registered_worker_indices.push(idx);
                        }
                        Err(e) => info!("cluster[{i}]: CreateWorker({idx}) failed: {e}"),
                    }
                } else {
                    info!("cluster[{i}]: CreateWorker({idx}) already exists, skipping");
                    result.registered_worker_indices.push(idx);
                }
            }
            ClusterOp::DropWorker(idx) => {
                info!("cluster[{i}]: DropWorker({idx})");
                let host = harness.worker_host(idx);
                let resp: anyhow::Result<worker::Model> =
                    harness.send(DropWorker::new(host)).await;
                match resp {
                    Ok(_) => info!("cluster[{i}]: DropWorker({idx}) succeeded"),
                    Err(e) => info!("cluster[{i}]: DropWorker({idx}) rejected: {e}"),
                }
            }
            ClusterOp::Sleep(d) => {
                info!("cluster[{i}]: Sleep({d:?})");
                tokio::time::sleep(d).await;
            }
        }
    }

    fn check_worker_convergence(&self, workers: &[worker::Model]) {
        for w in workers {
            assert!(
                w.desired_state != DesiredWorkerState::Active
                    || w.current_state == WorkerState::Active,
                "worker_convergence: worker {} desired=Active current={:?}",
                w.host_addr,
                w.current_state
            );
        }
    }
}

#[async_trait(?Send)]
impl Workload for ClusterWorkload {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn start(&self, harness: &TestHarness) {
        info!(
            "{}: running for {:?} (sleep_max={}s)",
            self.name(),
            self.test_duration,
            self.max_sleep_secs
        );

        let mut gen_state = GeneratorState::new(harness.num_workers());
        let mut result = ClusterState::default();
        let mut op_index = 0;

        let _ = tokio::time::timeout(self.test_duration, async {
            loop {
                let op = self.generate_op(&mut gen_state);
                self.execute_op(harness, op_index, op, &mut result).await;
                op_index += 1;
            }
        })
        .await;

        info!("{}: completed {} ops", self.name(), op_index);
        *self.state.lock().unwrap() = result;
    }

    async fn check(&self, harness: &TestHarness) {
        let workers: Vec<worker::Model> = harness.send(GetWorker::all()).await.unwrap();
        self.check_worker_convergence(&workers);
    }
}

inventory::submit! {
    WorkloadFactory {
        name: ClusterWorkload::NAME,
        create: |opts| Box::new(ClusterWorkload::from_options(opts)),
    }
}
