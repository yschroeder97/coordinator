use crate::test_config::TestConfig;
use coordinator::catalog::catalog::Catalog;
use coordinator::coordinator::{start_coordinator, CoordinatorHandle, CoordinatorRequest};
use coordinator::network_service::NetworkService;
use std::sync::Once;
use tracing::error;

static TRACING_INIT: Once = Once::new();

pub fn init_tracing() {
    TRACING_INIT.call_once(|| {
        use tracing_subscriber::filter::{EnvFilter, LevelFilter};

        let filter = EnvFilter::from_default_env()
            .add_directive("coordinator=debug".parse().unwrap())
            .add_directive("integration_tests=debug".parse().unwrap())
            .add_directive(LevelFilter::WARN.into());

        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_test_writer()
            .with_target(true)
            .with_thread_ids(false)
            .with_level(true)
            .try_init()
            .ok();
    });
}

pub async fn setup_coordinator(conf: TestConfig) -> CoordinatorHandle<CoordinatorRequest> {
    // init_tracing();
    //
    // let cluster = NetworkService::create(conf.workers.clone()).unwrap_or_else(|e| {
    //     error!("Failed to create cluster service: {}", e);
    //     std::process::exit(1);
    // });
    //
    // let catalog = Catalog::from_env().await.unwrap_or_else(|e| {
    //     error!("Failed to create catalog: {}", e);
    //     std::process::exit(1);
    // });
    //
    // let sender = start_coordinator(conf.message_bus_batch_size, cluster, catalog);
    //
    // sender
    todo!()
}
