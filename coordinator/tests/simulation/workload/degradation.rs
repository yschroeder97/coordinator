#![cfg(madsim)]
use crate::harness::TestHarness;
use crate::workload::{FailureInjectorFactory, Workload, WorkloadFactory, parse_options, run_ops};
use async_trait::async_trait;
use madsim::net::NetSim;
use madsim::rand::{Rng, thread_rng};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tracing::info;

const DEFAULT_NUM_OPS: usize = 10;
const DEFAULT_TEST_DURATION: f64 = 30.0;
const RESTORE_LATENCY_LO_MS: u64 = 1;
const RESTORE_LATENCY_HI_MS: u64 = 100;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct DegradationConfig {
    num_ops: usize,
    test_duration: f64,
    episode_duration_lo_secs: u64,
    episode_duration_hi_secs: u64,
    latency_lo_ms: u64,
    latency_hi_ms: u64,
    loss_rate: f64,
    degradation_probability: f64,
}

impl Default for DegradationConfig {
    fn default() -> Self {
        Self {
            num_ops: DEFAULT_NUM_OPS,
            test_duration: DEFAULT_TEST_DURATION,
            episode_duration_lo_secs: 2,
            episode_duration_hi_secs: 10,
            latency_lo_ms: 50,
            latency_hi_ms: 500,
            loss_rate: 0.1,
            degradation_probability: 0.5,
        }
    }
}

pub struct DegradationWorkload {
    num_ops: usize,
    test_duration: Duration,
    episode_duration_lo: Duration,
    episode_duration_hi: Duration,
    latency_lo: Duration,
    latency_hi: Duration,
    loss_rate: f64,
    degradation_probability: f64,
}

impl DegradationWorkload {
    pub const NAME: &str = "Degradation";

    pub fn from_options(options: &HashMap<String, toml::Value>) -> Self {
        let c: DegradationConfig = parse_options(options);
        Self {
            num_ops: c.num_ops,
            test_duration: Duration::from_secs_f64(c.test_duration),
            episode_duration_lo: Duration::from_secs(c.episode_duration_lo_secs),
            episode_duration_hi: Duration::from_secs(c.episode_duration_hi_secs),
            latency_lo: Duration::from_millis(c.latency_lo_ms),
            latency_hi: Duration::from_millis(c.latency_hi_ms),
            loss_rate: c.loss_rate,
            degradation_probability: c.degradation_probability,
        }
    }

    pub fn with_defaults() -> Self {
        Self::from_options(&HashMap::new())
    }
}

#[async_trait(?Send)]
impl Workload for DegradationWorkload {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn start(&self, _harness: &TestHarness) {
        info!(
            "{}: {} ops over {:?} episode={:?}..{:?} latency={:?}..{:?} loss={:.0}% prob={:.0}%",
            self.name(),
            self.num_ops,
            self.test_duration,
            self.episode_duration_lo,
            self.episode_duration_hi,
            self.latency_lo,
            self.latency_hi,
            self.loss_rate * 100.0,
            self.degradation_probability * 100.0,
        );

        let net = NetSim::current();

        run_ops(self.num_ops, self.test_duration, self.name(), |_| {
            Box::pin(async {
                let mut rng = thread_rng();
                if !rng.gen_bool(self.degradation_probability) {
                    return;
                }

                let duration = rng.gen_range(self.episode_duration_lo..self.episode_duration_hi);
                info!("degradation: increasing latency and loss for {duration:?}");

                net.update_config(|cfg| {
                    cfg.send_latency = self.latency_lo..self.latency_hi;
                    cfg.packet_loss_rate = self.loss_rate;
                });

                let net_ref = net.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(duration).await;
                    net_ref.update_config(|cfg| {
                        cfg.send_latency = Duration::from_millis(RESTORE_LATENCY_LO_MS)
                            ..Duration::from_millis(RESTORE_LATENCY_HI_MS);
                        cfg.packet_loss_rate = 0.0;
                    });
                    info!("degradation: restored network defaults");
                });
            })
        })
        .await;
    }
}

inventory::submit! {
    WorkloadFactory {
        name: DegradationWorkload::NAME,
        create: |opts| Box::new(DegradationWorkload::from_options(opts)),
    }
}

inventory::submit! {
    FailureInjectorFactory {
        name: DegradationWorkload::NAME,
        should_inject: |already_added| already_added == 0,
        create: || Box::new(DegradationWorkload::with_defaults()),
    }
}
