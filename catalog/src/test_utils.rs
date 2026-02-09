use crate::database::State;
use crate::worker_catalog::WorkerCatalog;
use model::worker::endpoint::{GrpcAddr, HostAddr};
use std::future::Future;
use std::sync::Arc;

pub const TEST_HOST_NAME: &str = "localhost";
pub const TEST_HOST_PORT: u16 = 8080;
pub const TEST_GRPC_PORT: u16 = 9090;

pub fn test_host_addr() -> HostAddr {
    HostAddr::new(TEST_HOST_NAME, TEST_HOST_PORT)
}

pub fn test_grpc_addr() -> GrpcAddr {
    GrpcAddr::new(TEST_HOST_NAME, TEST_GRPC_PORT)
}

pub fn test_worker_catalog(db: State) -> Arc<WorkerCatalog> {
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    WorkerCatalog::new(db, tx)
}

/// Helper function to run async property tests with a fresh in-memory database.
/// Creates a new SQLite in-memory database, runs migrations, and passes the
/// resulting Database to the test function.
/// Panics are treated as test failures.
pub fn test_prop<F, Fut>(f: F)
where
    F: FnOnce(State) -> Fut,
    Fut: Future<Output = ()>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime");

    rt.block_on(async {
        let db = State::for_test().await;
        f(db).await;
    });
}
