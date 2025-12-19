use crate::catalog::catalog_base::Catalog;
use crate::catalog::catalog_errors::CatalogErr;
use crate::catalog::query::query::{CreateQueryRequest, DropQueryRequest};
use crate::catalog::sink::sink::{CreateSinkRequest, DropSinkRequest};
use crate::catalog::source::logical_source::{
    CreateLogicalSourceRequest, DropLogicalSourceRequest,
};
use crate::catalog::source::physical_source::{
    CreatePhysicalSourceRequest, DropPhysicalSourceRequest,
};
use crate::catalog::worker::worker::{CreateWorkerRequest, DropWorkerRequest};
use crate::errors::CoordinatorErr;
pub use crate::message_bus::{message_bus, CoordinatorHandle, CoordinatorReceiver};
use crate::network::cluster_service::ClusterService;
use tracing::{info, info_span, Instrument};

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
}

#[macro_export]
macro_rules! into_request {
    ($variant:ident, $type:ty, $enum:ident) => {
        impl From<$type> for $enum {
            fn from(value: $type) -> Self {
                $enum::$variant(value)
            }
        }
    };
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

const DEFAULT_CAPACITY: usize = 16;

#[cfg(not(madsim))]
pub fn start_coordinator(batch_size: Option<usize>) -> CoordinatorHandle<CoordinatorRequest> {
    info!("Starting");
    let (handle, receiver) =
        message_bus::<CoordinatorRequest>(batch_size.unwrap_or(DEFAULT_CAPACITY));

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .enable_io()
            .build()
            .expect("Failed to create Tokio Runtime");

        let catalog = rt
            .block_on(Catalog::from_env())
            .expect("Failed to create Catalogs");

        let worker_catalog = catalog.worker_catalog();
        rt.spawn(
            async move {
                let mut cluster_service = ClusterService::new(worker_catalog);
                cluster_service.run().await
            }
            .instrument(info_span!("network_service")),
        );

        rt.block_on(
            async move { RequestListener::new(receiver, catalog).run().await }
                .instrument(info_span!("request_listener")),
        );
        rt.shutdown_background();
    });

    handle
}

pub async fn start_test_coordinator() -> CoordinatorHandle<CoordinatorRequest> {
    info!("Starting");
    let (handle, receiver) = message_bus::<CoordinatorRequest>(DEFAULT_CAPACITY);

    let catalog = Catalog::from_env().await.expect("Failed to create Catalog");

    let worker_catalog = catalog.worker_catalog();
    tokio::spawn(async move {
        let mut cluster_service = ClusterService::new(worker_catalog);
        cluster_service
            .run()
            .await
            .instrument(info_span!("cluster_service"))
    });

    info!("Spawning RequestListener");
    tokio::spawn(
        async move {
            info!("RequestListener task started");
            RequestListener::new(receiver, catalog).run().await
        }
        .instrument(info_span!("request_listener")),
    );

    handle
}

struct RequestListener {
    receiver: CoordinatorReceiver<CoordinatorRequest>,
    catalog: Catalog,
}

impl RequestListener {
    fn new(receiver: CoordinatorReceiver<CoordinatorRequest>, catalog: Catalog) -> RequestListener {
        Self { receiver, catalog }
    }

    async fn run(mut self) -> () {
        while let Some(msg) = self.receiver.recv() {
            match msg {
                CoordinatorRequest::CreateLogicalSource(req) => {
                    let _ = req.respond(
                        self.catalog
                            .source
                            .create_logical_source(&req.payload)
                            .await
                            .map_err(|e| CoordinatorErr::from(CatalogErr::from(e))),
                    );
                }
                CoordinatorRequest::CreatePhysicalSource(req) => {
                    let _ = req.respond(
                        self.catalog
                            .source
                            .create_physical_source(&req.payload)
                            .await
                            .map_err(|e| CoordinatorErr::from(CatalogErr::from(e))),
                    );
                }
                CoordinatorRequest::CreateSink(req) => {
                    let _ = req.respond(
                        self.catalog
                            .sink
                            .create_sink(&req.payload)
                            .await
                            .map_err(|e| CoordinatorErr::from(CatalogErr::from(e))),
                    );
                }
                CoordinatorRequest::CreateQuery(req) => {
                    let _ = req.respond(
                        self.catalog
                            .query
                            .create_query(&req.payload)
                            .await
                            .map_err(|e| CoordinatorErr::from(CatalogErr::from(e))),
                    );
                }
                CoordinatorRequest::CreateWorker(req) => {
                    let _ = req.respond(
                        self.catalog
                            .worker
                            .create_worker(&req.payload)
                            .await
                            .map_err(|e| CoordinatorErr::from(CatalogErr::from(e))),
                    );
                }
                CoordinatorRequest::DropLogicalSource(req) => {
                    let _ = req.respond(
                        self.catalog
                            .source
                            .drop_logical_source(&req.payload)
                            .await
                            .map_err(|e| CoordinatorErr::from(CatalogErr::from(e))),
                    );
                }
                CoordinatorRequest::DropPhysicalSource(req) => {
                    let _ = req.respond(
                        self.catalog
                            .source
                            .drop_physical_source(&req.payload)
                            .await
                            .map_err(|e| CoordinatorErr::from(CatalogErr::from(e))),
                    );
                }
                CoordinatorRequest::DropSink(req) => {
                    let _ = req.respond(
                        self.catalog
                            .sink
                            .drop_sink(&req.payload)
                            .await
                            .map_err(|e| CoordinatorErr::from(CatalogErr::from(e))),
                    );
                }
                CoordinatorRequest::DropQuery(req) => {
                    let _ = req.respond(
                        self.catalog
                            .query
                            .drop_query(&req.payload)
                            .await
                            .map_err(|e| CoordinatorErr::from(CatalogErr::from(e))),
                    );
                }
                CoordinatorRequest::DropWorker(req) => {
                    let _ = req.respond(
                        self.catalog
                            .worker
                            .drop_worker(&req.payload)
                            .await
                            .map_err(|e| CoordinatorErr::from(CatalogErr::from(e))),
                    );
                }
            }
        }
    }
}
