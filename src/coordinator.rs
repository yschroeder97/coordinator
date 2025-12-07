use crate::catalog::catalog::Catalog;
pub use crate::message_bus::{message_bus, CoordinatorHandle, CoordinatorReceiver};
use crate::network_service::NetworkService;
use crate::requests::{
    CreateLogicalSourceRequest, CreatePhysicalSourceRequest, CreateQueryRequest, CreateSinkRequest,
    CreateWorkerRequest, DropLogicalSourceRequest, DropPhysicalSourceRequest, DropQueryRequest,
    DropSinkRequest, DropWorkerRequest, GetLogicalSourceRequest, GetPhysicalSourceRequest,
    GetQueryRequest, GetSinkRequest, GetWorkerRequest,
};
use tokio::runtime::Runtime;
use tracing::{info_span, Instrument};

pub enum CoordinatorRequest {
    CreateLogicalSource(CreateLogicalSourceRequest),
    CreatePhysicalSource(CreatePhysicalSourceRequest),
    CreateSink(CreateSinkRequest),
    CreateQuery(CreateQueryRequest),
    CreateWorker(CreateWorkerRequest),
    ShowLogicalSources(GetLogicalSourceRequest),
    ShowPhysicalSources(GetPhysicalSourceRequest),
    ShowSinks(GetSinkRequest),
    ShowWorkers(GetWorkerRequest),
    ShowQueries(GetQueryRequest),
    DropLogicalSource(DropLogicalSourceRequest),
    DropPhysicalSource(DropPhysicalSourceRequest),
    DropSink(DropSinkRequest),
    DropQuery(DropQueryRequest),
    DropWorker(DropWorkerRequest),
}

macro_rules! impl_from {
    ($variant:ident, $type:ty, $enum:ident) => {
        impl From<$type> for $enum {
            fn from(value: $type) -> Self {
                $enum::$variant(value)
            }
        }
    };
}

impl_from!(
    CreateLogicalSource,
    CreateLogicalSourceRequest,
    CoordinatorRequest
);
impl_from!(
    CreatePhysicalSource,
    CreatePhysicalSourceRequest,
    CoordinatorRequest
);
impl_from!(CreateSink, CreateSinkRequest, CoordinatorRequest);
impl_from!(CreateQuery, CreateQueryRequest, CoordinatorRequest);

impl_from!(
    ShowLogicalSources,
    GetLogicalSourceRequest,
    CoordinatorRequest
);
impl_from!(
    ShowPhysicalSources,
    GetPhysicalSourceRequest,
    CoordinatorRequest
);
impl_from!(ShowSinks, GetSinkRequest, CoordinatorRequest);
impl_from!(ShowWorkers, GetWorkerRequest, CoordinatorRequest);
impl_from!(ShowQueries, GetQueryRequest, CoordinatorRequest);
impl_from!(
    DropLogicalSource,
    DropLogicalSourceRequest,
    CoordinatorRequest
);
impl_from!(
    DropPhysicalSource,
    DropPhysicalSourceRequest,
    CoordinatorRequest
);
impl_from!(DropSink, DropSinkRequest, CoordinatorRequest);
impl_from!(DropQuery, DropQueryRequest, CoordinatorRequest);

const DEFAULT_CAPACITY: usize = 16;

pub fn start_coordinator(
    batch_size: Option<usize>,
    cluster: NetworkService,
    catalog: Catalog,
) -> CoordinatorHandle<CoordinatorRequest> {
    let (handle, receiver) =
        message_bus::<CoordinatorRequest>(batch_size.unwrap_or(DEFAULT_CAPACITY));

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .enable_io()
            .build()
            .expect("Failed to create Tokio Runtime");

        rt.block_on(
            async move { RequestListener::new(receiver, cluster, catalog).run().await }
                .instrument(info_span!("request_listener")),
        );
        rt.shutdown_background();
    });

    handle
}

struct RequestListener {
    receiver: CoordinatorReceiver<CoordinatorRequest>,
    cluster: NetworkService,
    catalog: Catalog,
}

impl RequestListener {
    pub(crate) fn new(
        receiver: CoordinatorReceiver<CoordinatorRequest>,
        cluster: NetworkService,
        catalog: Catalog,
    ) -> RequestListener {
        Self {
            receiver,
            cluster,
            catalog,
        }
    }

    pub(crate) async fn run(mut self) -> () {
        while let Some(msg) = self.receiver.recv() {
            match msg {
                CoordinatorRequest::CreateLogicalSource(req) => {
                    let _ = req.respond(
                        self.catalog
                            .create_logical_source(&req.payload)
                            .await
                            .map_err(Into::into),
                    );
                }
                CoordinatorRequest::CreatePhysicalSource(req) => {
                    let _ = req.respond(
                        self.catalog
                            .create_physical_source(&req.payload)
                            .await
                            .map_err(Into::into),
                    );
                }
                CoordinatorRequest::CreateSink(req) => {
                    let _ = req.respond(
                        self.catalog
                            .create_sink(&req.payload)
                            .await
                            .map_err(Into::into),
                    );
                }
                CoordinatorRequest::CreateQuery(req) => {
                    let _ = req.respond(
                        self.catalog
                            .create_query(&req.payload)
                            .await
                            .map_err(Into::into),
                    );
                }
                CoordinatorRequest::CreateWorker(req) => {
                    let _ = req.respond(
                        self.catalog
                            .create_worker(&req.payload)
                            .await
                            .map_err(Into::into),
                    );
                }
                CoordinatorRequest::ShowLogicalSources(_show_logical) => {}
                CoordinatorRequest::ShowPhysicalSources(_show_physical) => {}
                CoordinatorRequest::ShowSinks(_show_sinks) => {}
                CoordinatorRequest::ShowWorkers(_show_workers) => {}
                CoordinatorRequest::ShowQueries(_show_queries) => {}
                CoordinatorRequest::DropLogicalSource(req) => {
                    let _ = req.respond(
                        self.catalog
                            .drop_logical_source(&req.payload)
                            .await
                            .map_err(Into::into),
                    );
                }
                CoordinatorRequest::DropPhysicalSource(req) => {
                    let _ = req.respond(
                        self.catalog
                            .drop_physical_source(&req.payload)
                            .await
                            .map_err(Into::into),
                    );
                }
                CoordinatorRequest::DropSink(req) => {
                    let _ = req.respond(
                        self.catalog
                            .drop_sink(&req.payload)
                            .await
                            .map_err(Into::into),
                    );
                }
                CoordinatorRequest::DropQuery(req) => {
                    let _ = req.respond(
                        self.catalog
                            .drop_query(&req.payload)
                            .await
                            .map_err(Into::into),
                    );
                }
                CoordinatorRequest::DropWorker(req) => {
                    let _ = req.respond(
                        self.catalog
                            .drop_worker(&req.payload)
                            .await
                            .map_err(Into::into),
                    );
                }
            }
        }
    }
}
