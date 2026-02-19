use crate::request_handler::RequestHandler;
use anyhow::Result;
use catalog::Catalog;
use catalog::database::StateBackend;
use controller::cluster::service::ClusterService;
use controller::into_request;
use controller::query::service::QueryService;
use controller::request::Request;
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
use tracing::{Instrument, info, info_span};

pub type CreateLogicalSourceRequest = Request<CreateLogicalSource, Result<logical_source::Model>>;
pub type CreatePhysicalSourceRequest =
    Request<CreatePhysicalSource, Result<physical_source::Model>>;
pub type CreateSinkRequest = Request<CreateSink, Result<sink::Model>>;
pub type CreateQueryRequest = Request<CreateQuery, Result<query::Model>>;
pub type CreateWorkerRequest = Request<CreateWorker, Result<worker::Model>>;

pub type DropLogicalSourceRequest =
    Request<DropLogicalSource, Result<Option<logical_source::Model>>>;
pub type DropPhysicalSourceRequest =
    Request<DropPhysicalSource, Result<Vec<physical_source::Model>>>;
pub type DropSinkRequest = Request<DropSink, Result<Vec<sink::Model>>>;
pub type DropQueryRequest = Request<DropQuery, Result<Vec<query::Model>>>;
pub type DropWorkerRequest = Request<DropWorker, Result<worker::Model>>;

pub type GetLogicalSourceRequest = Request<GetLogicalSource, Result<Option<logical_source::Model>>>;
pub type GetPhysicalSourceRequest = Request<GetPhysicalSource, Result<Vec<physical_source::Model>>>;
pub type GetSinkRequest = Request<GetSink, Result<Vec<sink::Model>>>;
pub type GetQueryRequest = Request<GetQuery, Result<Vec<query::Model>>>;
pub type GetWorkerRequest = Request<GetWorker, Result<Vec<worker::Model>>>;

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

            let mut cluster_service = ClusterService::new(catalog.worker.clone());
            let worker_registry = cluster_service.registry_handle();
            tokio::spawn(async move {
                cluster_service
                    .run()
                    .instrument(info_span!("cluster_service"))
                    .await
            });

            tokio::spawn(async move {
                QueryService::new(catalog.query.clone(), worker_registry)
                    .run()
                    .instrument(info_span!("query_service"))
                    .await
            });

            RequestHandler::new(receiver, catalog.clone())
                .run()
                .instrument(info_span!("request_listener"))
                .await
        });

        rt.shutdown_background();
    });

    handle
}

pub async fn start_test() -> flume::Sender<CoordinatorRequest> {
    info!("Starting");
    let (handle, receiver) = flume::bounded(DEFAULT_CAPACITY);

    let catalog = Catalog::for_test().await;

    let mut cluster_service = ClusterService::new(catalog.worker.clone());
    let worker_registry = cluster_service.registry_handle();
    tokio::spawn(async move {
        cluster_service
            .run()
            .instrument(info_span!("cluster_service"))
            .await
    });

    tokio::spawn(async move {
        QueryService::new(catalog.query.clone(), worker_registry)
            .run()
            .instrument(info_span!("query_service"))
            .await
    });

    tokio::spawn(async move {
        RequestHandler::new(receiver, catalog)
            .run()
            .instrument(info_span!("request_listener"))
            .await
    });

    handle
}
