use crate::catalog::catalog_errors::CatalogErr;
use crate::catalog::query::{CreateQueryRequest, DropQueryRequest, GetQueryRequest};
use crate::catalog::sink::{CreateSinkRequest, DropSinkRequest, GetSinkRequest};
use crate::catalog::source::logical_source::GetLogicalSourceRequest;
use crate::catalog::source::logical_source::{
    CreateLogicalSourceRequest, DropLogicalSourceRequest,
};
use crate::catalog::source::physical_source::GetPhysicalSourceRequest;
use crate::catalog::source::physical_source::{
    CreatePhysicalSourceRequest, DropPhysicalSourceRequest,
};
use crate::catalog::worker::{CreateWorkerRequest, DropWorkerRequest, GetWorkerRequest};
use crate::catalog::Catalog;
use crate::controller::query_service::QueryService;
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
                    let crate::request::Request { payload, reply_to } = req;
                    let _ = reply_to.send(
                        $catalog.$field.$method(&payload).await
                        .map_err(Into::into)
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
        let mut cluster_service = ClusterService::new(worker_catalog);
        let worker_registry = cluster_service.registry_handle();
        rt.spawn(async move {
            cluster_service
                .run()
                .instrument(info_span!("cluster_service"))
                .await
        });

        let query_catalog = catalog.query_catalog();
        rt.spawn(async move {
            QueryService::new(query_catalog, worker_registry)
                .run()
                .instrument(info_span!("query_service"))
                .await
        });

        rt.block_on(async move {
            RequestListener::new(receiver, catalog)
                .run()
                .instrument(info_span!("request_listener"))
                .await
        });

        rt.shutdown_background();
    });

    handle
}

pub async fn start_test_coordinator() -> CoordinatorHandle<CoordinatorRequest> {
    info!("Starting");
    let (handle, receiver) = message_bus::<CoordinatorRequest>(DEFAULT_CAPACITY);

    let catalog = Catalog::from_env().await.expect("Failed to create Catalog");

    let worker_catalog = catalog.worker_catalog();
    let mut cluster_service = ClusterService::new(worker_catalog);
    let worker_registry = cluster_service.registry_handle();
    tokio::spawn(async move {
        cluster_service
            .run()
            .instrument(info_span!("cluster_service"))
            .await
    });

    let query_catalog = catalog.query_catalog();
    tokio::spawn(async move {
        QueryService::new(query_catalog, worker_registry)
            .run()
            .instrument(info_span!("query_service"))
            .await
    });

    tokio::spawn(async move {
        RequestListener::new(receiver, catalog)
            .run()
            .instrument(info_span!("request_listener"))
            .await
    });

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
        while let Some(msg) = self.receiver.recv().await {
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
                GetQuery => query.get_active_queries,
                GetWorker => worker.get_workers,
            });
        }
    }
}
