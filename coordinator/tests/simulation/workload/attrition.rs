#![cfg(madsim)]
use crate::harness::TestHarness;
use crate::workload::{FailureInjectorFactory, Workload, WorkloadFactory, parse_options};
use async_trait::async_trait;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::Handle;
use serde::Deserialize;
use std::collections::HashMap;
use std::ops::Range;
use std::time::Duration;
use tracing::info;

#[derive(Deserialize)]
#[serde(default)]
struct AttritionConfig {
    test_duration: f64,
    kill_rate: f64,
    kill_interval_secs: u64,
    restart_delay_lo_secs: u64,
    restart_delay_hi_secs: u64,
    restart_all_after_secs: Option<u64>,
    kill_coordinator: bool,
}

impl Default for AttritionConfig {
    fn default() -> Self {
        Self {
            test_duration: 30.0,
            kill_rate: 0.20,
            kill_interval_secs: 5,
            restart_delay_lo_secs: 1,
            restart_delay_hi_secs: 10,
            restart_all_after_secs: None,
            kill_coordinator: false,
        }
    }
}

pub struct AttritionWorkload {
    test_duration: Duration,
    kill_rate: f64,
    kill_interval: Duration,
    restart_delay: Range<Duration>,
    restart_all_after_secs: Option<u64>,
    kill_coordinator: bool,
}

impl AttritionWorkload {
    pub const NAME: &str = "Attrition";

    pub fn from_options(options: &HashMap<String, toml::Value>) -> Self {
        let c: AttritionConfig = parse_options(options);
        Self {
            test_duration: Duration::from_secs_f64(c.test_duration),
            kill_rate: c.kill_rate,
            kill_interval: Duration::from_secs(c.kill_interval_secs),
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
            "{}: kill_rate={:.0}% interval={:?} restart_delay={:?}..{:?} duration={:?}",
            self.name(),
            self.kill_rate * 100.0,
            self.kill_interval,
            self.restart_delay.start,
            self.restart_delay.end,
            self.test_duration,
        );

        let mut node_names: Vec<String> = (0..harness.num_workers())
            .map(|i| harness.worker_name(i))
            .collect();
        if self.kill_coordinator {
            node_names.push(harness.coordinator_name().to_string());
        }

        let _ = tokio::time::timeout(self.test_duration, async {
            loop {
                tokio::time::sleep(self.kill_interval).await;

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
            }
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
