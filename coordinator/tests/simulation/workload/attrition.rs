#![cfg(madsim)]
use crate::harness::TestHarness;
use crate::workload::{FailureInjectorFactory, Workload, WorkloadFactory, parse_options, run_ops};
use async_trait::async_trait;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::Handle;
use serde::Deserialize;
use std::collections::HashMap;
use std::ops::Range;
use std::time::Duration;
use tracing::info;

const DEFAULT_NUM_OPS: usize = 10;
const DEFAULT_TEST_DURATION: f64 = 30.0;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct AttritionConfig {
    num_ops: usize,
    test_duration: f64,
    kill_rate: f64,
    restart_delay_lo_secs: u64,
    restart_delay_hi_secs: u64,
    restart_all_after_secs: Option<u64>,
    kill_coordinator: bool,
}

impl Default for AttritionConfig {
    fn default() -> Self {
        Self {
            num_ops: DEFAULT_NUM_OPS,
            test_duration: DEFAULT_TEST_DURATION,
            kill_rate: 0.20,
            restart_delay_lo_secs: 1,
            restart_delay_hi_secs: 10,
            restart_all_after_secs: None,
            kill_coordinator: false,
        }
    }
}

pub struct AttritionWorkload {
    num_ops: usize,
    test_duration: Duration,
    kill_rate: f64,
    restart_delay: Range<Duration>,
    restart_all_after_secs: Option<u64>,
    kill_coordinator: bool,
}

impl AttritionWorkload {
    pub const NAME: &str = "Attrition";

    pub fn from_options(options: &HashMap<String, toml::Value>) -> Self {
        let c: AttritionConfig = parse_options(options);
        Self {
            num_ops: c.num_ops,
            test_duration: Duration::from_secs_f64(c.test_duration),
            kill_rate: c.kill_rate,
            restart_delay: Duration::from_secs(c.restart_delay_lo_secs)
                ..Duration::from_secs(c.restart_delay_hi_secs),
            restart_all_after_secs: c.restart_all_after_secs,
            kill_coordinator: c.kill_coordinator,
        }
    }

    pub fn with_defaults() -> Self {
        Self::from_options(&HashMap::new())
    }
}

#[async_trait(?Send)]
impl Workload for AttritionWorkload {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn start(&self, harness: &TestHarness) {
        info!(
            "{}: {} ops over {:?} kill_rate={:.0}% restart_delay={:?}..{:?}",
            self.name(),
            self.num_ops,
            self.test_duration,
            self.kill_rate * 100.0,
            self.restart_delay.start,
            self.restart_delay.end,
        );

        let mut node_names: Vec<String> = (0..harness.num_workers())
            .map(|i| harness.worker_name(i))
            .collect();
        if self.kill_coordinator {
            node_names.push(harness.coordinator_name().to_string());
        }

        run_ops(self.num_ops, self.test_duration, self.name(), |_| {
            Box::pin(async {
                let mut rng = thread_rng();
                let victims: Vec<&str> = node_names
                    .iter()
                    .filter(|_| rng.gen_bool(self.kill_rate))
                    .map(|s| s.as_str())
                    .collect();

                for name in &victims {
                    info!("attrition: kill {name}");
                    Handle::current().kill(*name);
                }

                if !victims.is_empty() {
                    let delay =
                        thread_rng().gen_range(self.restart_delay.clone());
                    tokio::time::sleep(delay).await;

                    for name in &victims {
                        info!("attrition: restart {name}");
                        Handle::current().restart(*name);
                    }
                }
            })
        })
        .await;

        if let Some(secs) = self.restart_all_after_secs {
            info!("attrition: killing all workers for total restart");
            for name in &node_names {
                Handle::current().kill(name);
            }
            tokio::time::sleep(Duration::from_secs(secs)).await;
            info!("attrition: restarting all workers");
            for name in &node_names {
                Handle::current().restart(name);
            }
        }
    }
}

inventory::submit! {
    WorkloadFactory {
        name: AttritionWorkload::NAME,
        create: |opts| Box::new(AttritionWorkload::from_options(opts)),
    }
}

inventory::submit! {
    FailureInjectorFactory {
        name: AttritionWorkload::NAME,
        should_inject: |already_added| already_added == 0,
        create: || Box::new(AttritionWorkload::with_defaults()),
    }
}
