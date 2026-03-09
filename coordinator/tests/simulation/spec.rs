#![cfg(madsim)]
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Duration;

const ENV_CONFIG_PATH: &str = "SIM_TEST_CONFIG";
const DEFAULT_TIMEOUT_SECS: u64 = 600;
const DEFAULT_MIN_WORKERS: u8 = 2;

#[derive(Default, Debug, Deserialize)]
pub struct TestSpec {
    pub title: Option<String>,
    pub timeout_secs: Option<u64>,
    pub min_workers: Option<u8>,
    pub buggify: Option<bool>,
    pub network: Option<NetworkConfig>,
    pub workload: Option<Vec<WorkloadEntry>>,
}

#[derive(Default, Clone, Debug, Deserialize)]
pub struct NetworkConfig {
    pub send_latency_lo_ms: Option<u64>,
    pub send_latency_hi_ms: Option<u64>,
    pub packet_loss_rate: Option<f64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "name")]
pub enum WorkloadEntry {
    CrudWorkload {
        min_ops: Option<usize>,
        max_ops: Option<usize>,
        max_sleep_secs: Option<u64>,
    },
    Attrition {
        kill_rate: Option<f64>,
        kill_interval_secs: Option<u64>,
        restart_delay_lo_secs: Option<u64>,
        restart_delay_hi_secs: Option<u64>,
        restart_all_after_secs: Option<u64>,
    },
    NetworkPartition {
        partition_rate: Option<f64>,
        duration_lo_secs: Option<u64>,
        duration_hi_secs: Option<u64>,
    },
}

impl TestSpec {
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS))
    }

    pub fn min_workers(&self) -> u8 {
        self.min_workers.unwrap_or(DEFAULT_MIN_WORKERS)
    }
}

pub fn load_spec(path: Option<&str>) -> TestSpec {
    let file_path = match path {
        Some(p) => Some(p.to_string()),
        None => std::env::var(ENV_CONFIG_PATH).ok(),
    };

    let file_path = match file_path {
        Some(p) if !p.is_empty() => p,
        _ => return TestSpec::default(),
    };

    let full_path = if PathBuf::from(&file_path).is_relative() {
        let base = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
        PathBuf::from(base).join(&file_path)
    } else {
        PathBuf::from(&file_path)
    };

    let content = std::fs::read_to_string(&full_path)
        .unwrap_or_else(|e| panic!("Failed to read spec at {}: {e}", full_path.display()));
    toml::from_str(&content)
        .unwrap_or_else(|e| panic!("Failed to parse spec at {}: {e}", full_path.display()))
}
