use crate::catalog::Catalog;
use crate::catalog::catalog_errors::CatalogErr;
use crate::catalog::query::query::GetQueryRequest;
use crate::catalog::query::query::{CreateQueryRequest, DropQueryRequest};
use crate::catalog::sink::sink::GetSinkRequest;
use crate::catalog::sink::sink::{CreateSinkRequest, DropSinkRequest};
use crate::catalog::source::logical_source::GetLogicalSourceRequest;
use crate::catalog::source::logical_source::{
    CreateLogicalSourceRequest, DropLogicalSourceRequest,
};
use crate::catalog::source::physical_source::GetPhysicalSourceRequest;
use crate::catalog::source::physical_source::{
    CreatePhysicalSourceRequest, DropPhysicalSourceRequest,
};
use crate::catalog::worker::worker::GetWorkerRequest;
use crate::catalog::worker::worker::{CreateWorkerRequest, DropWorkerRequest};
use crate::errors::CoordinatorErr;
pub use crate::message_bus::{message_bus, CoordinatorHandle, CoordinatorReceiver};
use crate::network::cluster_service::ClusterService;
use tracing::debug;
use tracing::{info, info_span, Instrument};

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

macro_rules! dispatch {
    ($msg:expr, $catalog:expr, {
        $($variant:ident => $field:ident . $method:ident),* $(,)?
    }) => {
        match $msg {
            $(
                CoordinatorRequest::$variant(req) => {
                    debug!("Received: {req:?}");
                    let _ = req.respond(
                        $catalog.$field.$method(&req.payload).await
                            .map_err(|e| CoordinatorErr::from(CatalogErr::from(e)))
                    );
                }
            )*
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
            .instrument(info_span!("cluster_service")),
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
            dispatch!(msg, self.catalog, {
                CreateLogicalSource => source.create_logical_source,
                CreatePhysicalSource => source.create_physical_source,
                CreateSink => sink.create_sink,
                CreateQuery => query.create_query,
                CreateWorker => worker.create_worker,
                DropLogicalSource => source.drop_logical_source,
                DropPhysicalSource => source.drop_physical_source,
                DropSink => sink.drop_sink,
                DropQuery => query.drop_query,
                DropWorker => worker.drop_worker,
                GetLogicalSource => source.get_logical_source,
                GetPhysicalSource => source.get_physical_sources,
                GetSink => sink.get_sinks,
                GetQuery => query.get_queries,
                GetWorker => worker.get_workers,
            });
        }
    }
}
