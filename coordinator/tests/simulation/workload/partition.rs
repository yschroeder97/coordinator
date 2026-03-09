#![cfg(madsim)]
use crate::harness::TestHarness;
use crate::workload::TestWorkload;
use futures::future::BoxFuture;
use madsim::rand::Rng;
use madsim::runtime::Handle;
use std::ops::Range;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

const DEFAULT_PARTITION_RATE: f64 = 0.15;
const DEFAULT_PARTITION_INTERVAL: Duration = Duration::from_secs(3);
const DEFAULT_DURATION_LO: Duration = Duration::from_secs(1);
const DEFAULT_DURATION_HI: Duration = Duration::from_secs(10);

const RANDOM_PARTITION_RATE_LO_PCT: u32 = 0;
const RANDOM_PARTITION_RATE_HI_PCT: u32 = 25;
const RANDOM_DURATION_LO_SECS: u64 = 1;
const RANDOM_DURATION_HI_SECS: u64 = 10;

pub struct PartitionWorkload {
    partition_rate: f64,
    duration: Range<Duration>,
}

impl PartitionWorkload {
    pub fn new(
        partition_rate: Option<f64>,
        duration_lo_secs: Option<u64>,
        duration_hi_secs: Option<u64>,
    ) -> Self {
        Self {
            partition_rate: partition_rate.unwrap_or(DEFAULT_PARTITION_RATE),
            duration: duration_lo_secs
                .map(Duration::from_secs)
                .unwrap_or(DEFAULT_DURATION_LO)
                ..duration_hi_secs
                    .map(Duration::from_secs)
                    .unwrap_or(DEFAULT_DURATION_HI),
        }
    }

    pub fn random() -> Self {
        let mut rng = madsim::rand::thread_rng();
        let rate =
            rng.gen_range(RANDOM_PARTITION_RATE_LO_PCT..=RANDOM_PARTITION_RATE_HI_PCT) as f64
                / 100.0;
        let lo = rng.gen_range(RANDOM_DURATION_LO_SECS..=RANDOM_DURATION_HI_SECS / 2);
        let hi = rng.gen_range(lo + 1..=RANDOM_DURATION_HI_SECS);
        Self::new(Some(rate), Some(lo), Some(hi))
    }
}

impl TestWorkload for PartitionWorkload {
    fn description(&self) -> &str {
        "NetworkPartition"
    }

    fn start<'a>(&'a self, harness: &'a TestHarness, cancel: CancellationToken) -> BoxFuture<'a, ()> {
        Box::pin(async move {
        info!(
            "{}: rate={:.0}% duration={:?}..{:?}",
            self.description(),
            self.partition_rate * 100.0,
            self.duration.start,
            self.duration.end,
        );

        let all_names: Vec<String> = std::iter::once("coordinator".to_string())
            .chain((0..harness.num_workers()).map(|i| harness.worker_name(i)))
            .collect();

        if all_names.len() < 2 {
            return;
        }

        let net = madsim::net::NetSim::current();
        let rt = Handle::current();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(DEFAULT_PARTITION_INTERVAL) => {}
            }

            let mut rng = madsim::rand::thread_rng();
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
    }

    fn check<'a>(&'a self, _harness: &'a TestHarness) -> BoxFuture<'a, bool> {
        Box::pin(async { true })
    }
}
