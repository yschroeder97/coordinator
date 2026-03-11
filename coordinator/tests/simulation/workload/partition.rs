#![cfg(madsim)]
use crate::harness::TestHarness;
use crate::workload::{FailureInjectorFactory, Workload, WorkloadFactory, parse_options};
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

#[derive(Deserialize)]
#[serde(default)]
struct PartitionConfig {
    test_duration: f64,
    partition_rate: f64,
    partition_interval_secs: u64,
    duration_lo_secs: u64,
    duration_hi_secs: u64,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            test_duration: 30.0,
            partition_rate: 0.15,
            partition_interval_secs: 3,
            duration_lo_secs: 1,
            duration_hi_secs: 10,
        }
    }
}

pub struct PartitionWorkload {
    test_duration: Duration,
    partition_rate: f64,
    partition_interval: Duration,
    duration: Range<Duration>,
}

impl PartitionWorkload {
    pub const NAME: &str = "NetworkPartition";

    pub fn from_options(options: &HashMap<String, toml::Value>) -> Self {
        let c: PartitionConfig = parse_options(options);
        Self {
            test_duration: Duration::from_secs_f64(c.test_duration),
            partition_rate: c.partition_rate,
            partition_interval: Duration::from_secs(c.partition_interval_secs),
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
            "{}: rate={:.0}% interval={:?} duration={:?}..{:?} test_duration={:?}",
            self.name(),
            self.partition_rate * 100.0,
            self.partition_interval,
            self.duration.start,
            self.duration.end,
            self.test_duration,
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

        let _ = tokio::time::timeout(self.test_duration, async {
            loop {
                tokio::time::sleep(self.partition_interval).await;

                let mut rng = thread_rng();
                if !rng.gen_bool(self.partition_rate) {
                    continue;
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
                    _ => continue,
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
            }
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
