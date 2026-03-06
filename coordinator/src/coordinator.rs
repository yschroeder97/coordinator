use crate::request_handler::RequestHandler;
use anyhow::Result;
use catalog::Catalog;
use catalog::database::StateBackend;
use controller::cluster::health_monitor::HealthMonitor;
use controller::cluster::service::ClusterService;
use common::into_request;
use common::request::Request;
use controller::query::service::QueryService;
use model::query;
use model::query::{CreateQuery, DropQuery, GetQuery};
use model::sink::{self, CreateSink, DropSink, GetSink};
use model::source::logical_source::{
    self, CreateLogicalSource, DropLogicalSource, GetLogicalSource,
};
use model::source::physical_source::{
    self, CreatePhysicalSource, DropPhysicalSource, GetPhysicalSource,
};
use model::worker::{self, CreateWorker, DropWorker, GetWorker};
use controller::cluster::worker_registry::WorkerRegistry;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

#[cfg(madsim)]
async fn supervised<F: std::future::Future<Output = ()>>(fut: F) -> bool {
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;
    AssertUnwindSafe(fut).catch_unwind().await.is_err()
}

#[cfg(not(madsim))]
async fn supervised<F: std::future::Future<Output = ()>>(fut: F) -> bool {
    fut.await;
    false
}

pub(crate) type CreateLogicalSourceRequest = Request<CreateLogicalSource, Result<logical_source::Model>>;
pub(crate) type CreatePhysicalSourceRequest =
    Request<CreatePhysicalSource, Result<physical_source::Model>>;
pub(crate) type CreateSinkRequest = Request<CreateSink, Result<sink::Model>>;
pub(crate) type CreateQueryRequest = Request<CreateQuery, Result<query::Model>>;
pub(crate) type CreateWorkerRequest = Request<CreateWorker, Result<worker::Model>>;

pub(crate) type DropLogicalSourceRequest =
    Request<DropLogicalSource, Result<Option<logical_source::Model>>>;
pub(crate) type DropPhysicalSourceRequest =
    Request<DropPhysicalSource, Result<Vec<physical_source::Model>>>;
pub(crate) type DropSinkRequest = Request<DropSink, Result<Vec<sink::Model>>>;
pub(crate) type DropQueryRequest = Request<DropQuery, Result<Vec<query::Model>>>;
pub(crate) type DropWorkerRequest = Request<DropWorker, Result<worker::Model>>;

pub(crate) type GetLogicalSourceRequest = Request<GetLogicalSource, Result<Option<logical_source::Model>>>;
pub(crate) type GetPhysicalSourceRequest = Request<GetPhysicalSource, Result<Vec<physical_source::Model>>>;
pub(crate) type GetSinkRequest = Request<GetSink, Result<Vec<sink::Model>>>;
pub(crate) type GetQueryRequest = Request<GetQuery, Result<Vec<query::Model>>>;
pub(crate) type GetWorkerRequest = Request<GetWorker, Result<Vec<worker::Model>>>;

#[derive(Debug)]
pub enum CoordinatorRequest {
    CreateLogicalSource(CreateLogicalSourceRequest),
    CreatePhysicalSource(CreatePhysicalSourceRequest),
    CreateSink(CreateSinkRequest),
    CreateQuery(CreateQueryRequest),
    CreateWorker(CreateWorkerRequest),
    DropLogicalSource(DropLogicalSourceRequest),
    DropPhysicalSource(DropPhysicalSourceRequest),
    DropSink(DropSinkRequest),
    DropQuery(DropQueryRequest),
    DropWorker(DropWorkerRequest),
    GetLogicalSource(GetLogicalSourceRequest),
    GetPhysicalSource(GetPhysicalSourceRequest),
    GetSink(GetSinkRequest),
    GetQuery(GetQueryRequest),
    GetWorker(GetWorkerRequest),
}

into_request!(
    CreateLogicalSource,
    CreateLogicalSourceRequest,
    CoordinatorRequest
);
into_request!(
    CreatePhysicalSource,
    CreatePhysicalSourceRequest,
    CoordinatorRequest
);
into_request!(CreateSink, CreateSinkRequest, CoordinatorRequest);
into_request!(CreateQuery, CreateQueryRequest, CoordinatorRequest);
into_request!(CreateWorker, CreateWorkerRequest, CoordinatorRequest);
into_request!(
    DropLogicalSource,
    DropLogicalSourceRequest,
    CoordinatorRequest
);
into_request!(
    DropPhysicalSource,
    DropPhysicalSourceRequest,
    CoordinatorRequest
);
into_request!(DropSink, DropSinkRequest, CoordinatorRequest);
into_request!(DropQuery, DropQueryRequest, CoordinatorRequest);
into_request!(DropWorker, DropWorkerRequest, CoordinatorRequest);
into_request!(
    GetLogicalSource,
    GetLogicalSourceRequest,
    CoordinatorRequest
);
into_request!(
    GetPhysicalSource,
    GetPhysicalSourceRequest,
    CoordinatorRequest
);
into_request!(GetSink, GetSinkRequest, CoordinatorRequest);
into_request!(GetQuery, GetQueryRequest, CoordinatorRequest);
into_request!(GetWorker, GetWorkerRequest, CoordinatorRequest);

const DEFAULT_CAPACITY: usize = 16;

fn spawn_service<S: controller::Supervisable>(service: S) -> JoinHandle<bool> {
    tokio::spawn(supervised(service.start()))
}

fn panicked(result: &std::result::Result<bool, tokio::task::JoinError>) -> bool {
    match result {
        Ok(true) => true,
        Err(e) if e.is_panic() => true,
        _ => false,
    }
}

async fn supervise(
    catalog: Arc<Catalog>,
    registry: WorkerRegistry,
    receiver: flume::Receiver<CoordinatorRequest>,
) {
    let spawn_cluster = || {
        spawn_service(ClusterService::new(catalog.worker.clone(), registry.clone()))
    };
    let spawn_health = || spawn_service(HealthMonitor::new(catalog.worker.clone()));
    let spawn_query = || {
        spawn_service(QueryService::new(catalog.clone(), registry.handle()))
    };
    let spawn_request = || {
        spawn_service(RequestHandler::new(receiver.clone(), catalog.clone()))
    };

    let mut cluster = spawn_cluster();
    let mut health = spawn_health();
    let mut query = spawn_query();
    let mut request = spawn_request();

    loop {
        tokio::select! {
            result = &mut cluster => {
                if panicked(&result) {
                    error!("ClusterService panicked, restarting");
                    cluster = spawn_cluster();
                } else {
                    warn!("ClusterService exited: {result:?}");
                    break;
                }
            }
            result = &mut health => {
                if panicked(&result) {
                    error!("HealthMonitor panicked, restarting");
                    health = spawn_health();
                } else {
                    warn!("HealthMonitor exited: {result:?}");
                    break;
                }
            }
            result = &mut query => {
                if panicked(&result) {
                    error!("QueryService panicked, restarting");
                    query = spawn_query();
                } else {
                    warn!("QueryService exited: {result:?}");
                    break;
                }
            }
            result = &mut request => {
                if panicked(&result) {
                    error!("RequestHandler panicked, restarting");
                    request = spawn_request();
                } else {
                    info!("RequestHandler exited, shutting down");
                    break;
                }
            }
        }
    }
}

#[cfg(not(madsim))]
pub fn start(
    state_backend: StateBackend,
    batch_size: Option<usize>,
) -> flume::Sender<CoordinatorRequest> {
    info!("Starting");
    let (handle, receiver) = flume::bounded(batch_size.unwrap_or(DEFAULT_CAPACITY));

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .enable_io()
            .build()
            .expect("Failed to create Tokio Runtime");

        rt.block_on(async move {
            let state = catalog::database::Database::with(state_backend)
                .await
                .expect("Failed to create database state");
            let catalog = Catalog::from(state);
            let registry = WorkerRegistry::default();

            supervise(catalog, registry, receiver).await
        });

        rt.shutdown_background();
    });

    handle
}

#[cfg(any(test, feature = "testing", madsim))]
pub async fn start_for_test() -> flume::Sender<CoordinatorRequest> {
    info!("Starting");
    let (handle, receiver) = flume::bounded(DEFAULT_CAPACITY);

    let catalog = Catalog::for_test().await;
    let registry = WorkerRegistry::default();

    tokio::spawn(supervise(catalog, registry, receiver));

    handle
}
