use crate::cluster::ClusterService;
use crate::data_model::catalog::Catalog;
use crate::data_model::worker::GrpcAddr;
use crate::errors::CoordinatorError;
pub use crate::message_bus::{message_bus, MessageBusReceiver, MessageBusSender};
use crate::requests::{
    CreateLogicalSourceRequest, CreatePhysicalSourceRequest, CreateQueryRequest, CreateSinkRequest,
    CreateWorkerRequest, DropLogicalSourceRequest, DropPhysicalSourceRequest, DropQueryRequest,
    DropSinkRequest, DropWorkerRequest, GetLogicalSourceRequest, GetPhysicalSourceRequest,
    GetQueryRequest, GetSinkRequest, GetWorkerRequest,
};
use tracing::error;

pub enum CoordinatorRequest {
    CreateLogicalSource(CreateLogicalSourceRequest),
    CreatePhysicalSource(CreatePhysicalSourceRequest),
    CreateSink(CreateSinkRequest),
    CreateWorker(CreateWorkerRequest),
    CreateQuery(CreateQueryRequest),
    ShowLogicalSources(GetLogicalSourceRequest),
    ShowPhysicalSources(GetPhysicalSourceRequest),
    ShowSinks(GetSinkRequest),
    ShowWorkers(GetWorkerRequest),
    ShowQueries(GetQueryRequest),
    DropLogicalSource(DropLogicalSourceRequest),
    DropPhysicalSource(DropPhysicalSourceRequest),
    DropSink(DropSinkRequest),
    DropWorker(DropWorkerRequest),
    DropQuery(DropQueryRequest),
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
impl_from!(CreateWorker, CreateWorkerRequest, CoordinatorRequest);
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
impl_from!(DropWorker, DropWorkerRequest, CoordinatorRequest);
impl_from!(DropQuery, DropQueryRequest, CoordinatorRequest);

const DEFAULT_BATCH_SIZE: usize = 16;

pub fn start_coordinator(
    batch_size: Option<usize>,
    cluster: ClusterService,
    catalog: Catalog,
) -> MessageBusSender<CoordinatorRequest> {
    let (sender, receiver) =
        message_bus::<CoordinatorRequest>(batch_size.unwrap_or(DEFAULT_BATCH_SIZE));

    std::thread::spawn(move || {
        Coordinator::new(receiver, cluster, catalog).run();
    });

    sender
}

pub struct Coordinator {
    receiver: MessageBusReceiver<CoordinatorRequest>,
    cluster: ClusterService,
    catalog: Catalog,
}

impl Coordinator {
    pub fn new(
        receiver: MessageBusReceiver<CoordinatorRequest>,
        cluster: ClusterService,
        catalog: Catalog,
    ) -> Coordinator {
        Self {
            receiver,
            cluster,
            catalog,
        }
    }

    pub fn run(mut self) -> () {
        while let Some(req) = self.receiver.recv() {
            match req {
                CoordinatorRequest::CreateLogicalSource(_create_logical) => {}
                CoordinatorRequest::CreatePhysicalSource(_create_physical) => {}
                CoordinatorRequest::CreateSink(_create_sink) => {}
                CoordinatorRequest::CreateWorker(_create_worker) => {}
                CoordinatorRequest::CreateQuery(create_query) => {
                    let worker_addrs: Vec<GrpcAddr> =
                        self.cluster.membership.keys().cloned().collect();
                    let result: Result<(), CoordinatorError> = (|| {
                        self.cluster.submit_query(worker_addrs).map_err(|e| {
                            error!("Failed to submit query to cluster: {:?}", e);
                            CoordinatorError::ClusterService(e)
                        })?;

                        Ok(())
                    })();

                    if let Err(e) = create_query.respond(result) {
                        error!("Failed to send response: {:?}", e);
                    }
                }
                CoordinatorRequest::ShowLogicalSources(_show_logical) => {}
                CoordinatorRequest::ShowPhysicalSources(_show_physical) => {}
                CoordinatorRequest::ShowSinks(_show_sinks) => {}
                CoordinatorRequest::ShowWorkers(_show_workers) => {}
                CoordinatorRequest::ShowQueries(_show_queries) => {}
                CoordinatorRequest::DropLogicalSource(_drop_logical) => {}
                CoordinatorRequest::DropPhysicalSource(_drop_physical) => {}
                CoordinatorRequest::DropSink(_drop_sink) => {}
                CoordinatorRequest::DropWorker(_drop_worker) => {}
                CoordinatorRequest::DropQuery(_drop_query) => {}
            }
        }
    }
}
