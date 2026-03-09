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

const DEFAULT_KILL_RATE: f64 = 0.20;
const DEFAULT_KILL_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_RESTART_DELAY_LO: Duration = Duration::from_secs(1);
const DEFAULT_RESTART_DELAY_HI: Duration = Duration::from_secs(10);

const RANDOM_KILL_RATE_LO_PCT: u32 = 0;
const RANDOM_KILL_RATE_HI_PCT: u32 = 40;
const RANDOM_KILL_INTERVAL_LO_SECS: u64 = 3;
const RANDOM_KILL_INTERVAL_HI_SECS: u64 = 8;
const RANDOM_RESTART_DELAY_LO_SECS: u64 = 1;
const RANDOM_RESTART_DELAY_HI_SECS: u64 = 10;

pub struct AttritionWorkload {
    kill_rate: f64,
    kill_interval: Duration,
    restart_delay: Range<Duration>,
    restart_all_after_secs: Option<u64>,
}

impl AttritionWorkload {
    pub fn new(
        kill_rate: Option<f64>,
        kill_interval_secs: Option<u64>,
        restart_delay_lo_secs: Option<u64>,
        restart_delay_hi_secs: Option<u64>,
        restart_all_after_secs: Option<u64>,
    ) -> Self {
        Self {
            kill_rate: kill_rate.unwrap_or(DEFAULT_KILL_RATE),
            kill_interval: kill_interval_secs
                .map(Duration::from_secs)
                .unwrap_or(DEFAULT_KILL_INTERVAL),
            restart_delay: restart_delay_lo_secs
                .map(Duration::from_secs)
                .unwrap_or(DEFAULT_RESTART_DELAY_LO)
                ..restart_delay_hi_secs
                    .map(Duration::from_secs)
                    .unwrap_or(DEFAULT_RESTART_DELAY_HI),
            restart_all_after_secs,
        }
    }

    pub fn random() -> Self {
        let mut rng = madsim::rand::thread_rng();
        let kill_rate = rng.gen_range(RANDOM_KILL_RATE_LO_PCT..=RANDOM_KILL_RATE_HI_PCT) as f64
            / 100.0;
        let kill_interval_secs =
            rng.gen_range(RANDOM_KILL_INTERVAL_LO_SECS..=RANDOM_KILL_INTERVAL_HI_SECS);
        let restart_lo =
            rng.gen_range(RANDOM_RESTART_DELAY_LO_SECS..=RANDOM_RESTART_DELAY_HI_SECS / 2);
        let restart_hi = rng.gen_range(restart_lo + 1..=RANDOM_RESTART_DELAY_HI_SECS);
        Self::new(
            Some(kill_rate),
            Some(kill_interval_secs),
            Some(restart_lo),
            Some(restart_hi),
            None,
        )
    }
}

impl TestWorkload for AttritionWorkload {
    fn description(&self) -> &str {
        "Attrition"
    }

    fn start<'a>(&'a self, harness: &'a TestHarness, cancel: CancellationToken) -> BoxFuture<'a, ()> {
        Box::pin(async move {
        info!(
            "{}: kill_rate={:.0}% interval={:?} restart_delay={:?}..{:?}",
            self.description(),
            self.kill_rate * 100.0,
            self.kill_interval,
            self.restart_delay.start,
            self.restart_delay.end,
        );

        let worker_names: Vec<String> = (0..harness.num_workers())
            .map(|i| harness.worker_name(i))
            .collect();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(self.kill_interval) => {}
            }

            let mut rng = madsim::rand::thread_rng();
            let victims: Vec<&str> = worker_names
                .iter()
                .filter(|_| rng.gen_bool(self.kill_rate))
                .map(|s| s.as_str())
                .collect();

            for name in &victims {
                info!("attrition: kill {name}");
                Handle::current().kill(*name);
            }

            if !victims.is_empty() {
                let delay = madsim::rand::thread_rng().gen_range(self.restart_delay.clone());
                tokio::time::sleep(delay).await;

                for name in &victims {
                    info!("attrition: restart {name}");
                    Handle::current().restart(*name);
                }
            }
        }

        if let Some(secs) = self.restart_all_after_secs {
            info!("attrition: killing all workers for total restart");
            for name in &worker_names {
                Handle::current().kill(name);
            }
            tokio::time::sleep(Duration::from_secs(secs)).await;
            info!("attrition: restarting all workers");
            for name in &worker_names {
                Handle::current().restart(name);
            }
        }
        })
    }

    fn check<'a>(&'a self, _harness: &'a TestHarness) -> BoxFuture<'a, bool> {
        Box::pin(async { true })
    }
}
