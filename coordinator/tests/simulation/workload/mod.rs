#![cfg(madsim)]

pub mod attrition;
pub mod crud;
pub mod partition;

use crate::harness::TestHarness;
use crate::spec::WorkloadEntry;
use attrition::AttritionWorkload;
use crud::CrudWorkload;
use futures::future::BoxFuture;
use madsim::rand::Rng;
use partition::PartitionWorkload;
use tokio_util::sync::CancellationToken;

const ATTRITION_AUTO_INJECT_PROB: f64 = 0.50;
const PARTITION_AUTO_INJECT_PROB: f64 = 0.25;

pub trait TestWorkload: Send + Sync {
    fn description(&self) -> &str;
    fn is_primary(&self) -> bool { false }
    fn setup<'a>(&'a self, _harness: &'a TestHarness) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
    fn start<'a>(&'a self, harness: &'a TestHarness, cancel: CancellationToken) -> BoxFuture<'a, ()>;
    fn check<'a>(&'a self, harness: &'a TestHarness) -> BoxFuture<'a, bool>;
}

pub fn build_workloads(entries: &[WorkloadEntry]) -> Vec<Box<dyn TestWorkload>> {
    entries
        .iter()
        .map(|e| -> Box<dyn TestWorkload> {
            match e {
                WorkloadEntry::CrudWorkload {
                    min_ops,
                    max_ops,
                    max_sleep_secs,
                } => Box::new(CrudWorkload::new(*min_ops, *max_ops, *max_sleep_secs)),
                WorkloadEntry::Attrition {
                    kill_rate,
                    kill_interval_secs,
                    restart_delay_lo_secs,
                    restart_delay_hi_secs,
                    restart_all_after_secs,
                } => Box::new(AttritionWorkload::new(
                    *kill_rate,
                    *kill_interval_secs,
                    *restart_delay_lo_secs,
                    *restart_delay_hi_secs,
                    *restart_all_after_secs,
                )),
                WorkloadEntry::NetworkPartition {
                    partition_rate,
                    duration_lo_secs,
                    duration_hi_secs,
                } => Box::new(PartitionWorkload::new(
                    *partition_rate,
                    *duration_lo_secs,
                    *duration_hi_secs,
                )),
            }
        })
        .collect()
}

pub fn build_random_workloads() -> Vec<Box<dyn TestWorkload>> {
    let mut rng = madsim::rand::thread_rng();
    let mut workloads: Vec<Box<dyn TestWorkload>> = vec![Box::new(CrudWorkload::random())];

    if rng.gen_bool(ATTRITION_AUTO_INJECT_PROB) {
        workloads.push(Box::new(AttritionWorkload::random()));
    }

    if rng.gen_bool(PARTITION_AUTO_INJECT_PROB) {
        workloads.push(Box::new(PartitionWorkload::random()));
    }

    workloads
}
