#![cfg(madsim)]
use crate::harness::TestHarness;
use crate::workload::{FailureInjectorFactory, Workload, WorkloadFactory, parse_options, run_ops};
use async_trait::async_trait;
use madsim::net::NetSim;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::Handle;
use serde::Deserialize;
use std::collections::HashMap;
use std::iter::once;
use std::ops::Range;
use std::time::Duration;
use tracing::info;

const DEFAULT_NUM_OPS: usize = 10;
const DEFAULT_TEST_DURATION: f64 = 30.0;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct PartitionConfig {
    num_ops: usize,
    test_duration: f64,
    partition_rate: f64,
    duration_lo_secs: u64,
    duration_hi_secs: u64,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            num_ops: DEFAULT_NUM_OPS,
            test_duration: DEFAULT_TEST_DURATION,
            partition_rate: 0.15,
            duration_lo_secs: 1,
            duration_hi_secs: 10,
        }
    }
}

pub struct PartitionWorkload {
    num_ops: usize,
    test_duration: Duration,
    partition_rate: f64,
    duration: Range<Duration>,
}

impl PartitionWorkload {
    pub const NAME: &str = "NetworkPartition";

    pub fn from_options(options: &HashMap<String, toml::Value>) -> Self {
        let c: PartitionConfig = parse_options(options);
        Self {
            num_ops: c.num_ops,
            test_duration: Duration::from_secs_f64(c.test_duration),
            partition_rate: c.partition_rate,
            duration: Duration::from_secs(c.duration_lo_secs)..Duration::from_secs(c.duration_hi_secs),
        }
    }

    pub fn with_defaults() -> Self {
        Self::from_options(&HashMap::new())
    }
}

#[async_trait(?Send)]
impl Workload for PartitionWorkload {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn min_workers(&self) -> u8 {
        2
    }

    async fn start(&self, harness: &TestHarness) {
        info!(
            "{}: {} ops over {:?} rate={:.0}% partition_duration={:?}..{:?}",
            self.name(),
            self.num_ops,
            self.test_duration,
            self.partition_rate * 100.0,
            self.duration.start,
            self.duration.end,
        );

        let all_names: Vec<String> = once("coordinator".to_string())
            .chain((0..harness.num_workers()).map(|i| harness.worker_name(i)))
            .collect();

        let min_nodes_for_partition: usize = 2;
        if all_names.len() < min_nodes_for_partition {
            return;
        }

        let net = NetSim::current();
        let rt = Handle::current();

        run_ops(self.num_ops, self.test_duration, self.name(), |_| {
            Box::pin(async {
                let mut rng = thread_rng();
                if !rng.gen_bool(self.partition_rate) {
                    return;
                }

                let src_idx = rng.gen_range(0..all_names.len());
                let mut dst_idx = rng.gen_range(0..all_names.len() - 1);
                if dst_idx >= src_idx {
                    dst_idx += 1;
                }

                let src_name = all_names[src_idx].clone();
                let dst_name = all_names[dst_idx].clone();

                let src_node = rt.get_node(&src_name);
                let dst_node = rt.get_node(&dst_name);

                let (src_id, dst_id) = match (src_node, dst_node) {
                    (Some(s), Some(d)) => (s.id(), d.id()),
                    _ => return,
                };

                let duration = rng.gen_range(self.duration.clone());
                info!("partition: clog {src_name} -> {dst_name} for {duration:?}");
                net.clog_link(src_id, dst_id);

                let net_ref = net.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(duration).await;
                    net_ref.unclog_link(src_id, dst_id);
                    info!("partition: heal {src_name} -> {dst_name}");
                });
            })
        })
        .await;
    }
}

inventory::submit! {
    WorkloadFactory {
        name: PartitionWorkload::NAME,
        create: |opts| Box::new(PartitionWorkload::from_options(opts)),
    }
}

inventory::submit! {
    FailureInjectorFactory {
        name: PartitionWorkload::NAME,
        should_inject: |already_added| already_added == 0,
        create: || Box::new(PartitionWorkload::with_defaults()),
    }
}
