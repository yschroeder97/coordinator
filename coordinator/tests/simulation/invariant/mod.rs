#![cfg(madsim)]
use crate::runner;
use crate::spec;

macro_rules! sim_test {
    ($name:ident) => {
        #[madsim::test]
        async fn $name() {
            runner::run_test(spec::load_spec(None)).await;
        }
    };
    ($name:ident, $config:expr) => {
        #[madsim::test]
        async fn $name() {
            runner::run_test(
                spec::load_spec(Some(concat!("tests/simulation/configs/", $config))),
            )
            .await;
        }
    };
}

sim_test!(simulation);
sim_test!(sim_no_faults, "no_faults.toml");
sim_test!(sim_heavy_churn, "heavy_churn.toml");
sim_test!(sim_total_restart, "total_restart.toml");
