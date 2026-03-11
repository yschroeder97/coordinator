#![cfg(madsim)]
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

const DEFAULT_TIMEOUT_SECS: u64 = 600;
const DEFAULT_CONVERGENCE_TIMEOUT_SECS: u64 = 60;
const DEFAULT_BUGGIFY_PROBABILITY: f64 = 0.25;

#[derive(Debug, Deserialize)]
pub struct TestFile {
    pub test: Vec<TestSpec>,
}

#[derive(Debug, Deserialize)]
pub struct TestSpec {
    pub title: Option<String>,
    pub timeout: Option<u64>,
    pub convergence_timeout: Option<u64>,
    pub buggify: Option<bool>,
    pub run_failure_workloads: Option<bool>,
    pub network: Option<NetworkConfig>,
    pub workload: Option<Vec<WorkloadOptions>>,
}

#[derive(Default, Clone, Debug, Deserialize)]
pub struct NetworkConfig {
    pub send_latency_lo_ms: Option<u64>,
    pub send_latency_hi_ms: Option<u64>,
    pub packet_loss_rate: Option<f64>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct WorkloadOptions {
    pub test_name: String,
    #[serde(flatten)]
    pub options: HashMap<String, toml::Value>,
}

impl TestSpec {
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout.unwrap_or(DEFAULT_TIMEOUT_SECS))
    }

    pub fn convergence_timeout(&self) -> Duration {
        Duration::from_secs(
            self.convergence_timeout
                .unwrap_or(DEFAULT_CONVERGENCE_TIMEOUT_SECS),
        )
    }

    pub fn run_failure_workloads(&self) -> bool {
        self.run_failure_workloads.unwrap_or(true)
    }

    pub fn buggify_probability() -> f64 {
        DEFAULT_BUGGIFY_PROBABILITY
    }
}

impl NetworkConfig {
    pub fn validate(&self) {
        if let (Some(lo), Some(hi)) = (self.send_latency_lo_ms, self.send_latency_hi_ms) {
            assert!(lo <= hi, "send_latency_lo_ms ({lo}) must be <= send_latency_hi_ms ({hi})");
        }
        if let Some(rate) = self.packet_loss_rate {
            assert!(
                (0.0..=1.0).contains(&rate),
                "packet_loss_rate ({rate}) must be in [0.0, 1.0]"
            );
        }
    }
}

