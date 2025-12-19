use crate::catalog::query::query::{FragmentId, StopMode};
use crate::catalog::worker::worker_endpoint::GrpcAddr;
use crate::network::worker_client::worker_rpc_service::worker_rpc_service_client::WorkerRpcServiceClient;
use crate::network::worker_client::worker_rpc_service::{
    QueryStatusReply, QueryStatusRequest, RegisterQueryReply, RegisterQueryRequest,
    StartQueryRequest, StopQueryRequest, UnregisterQueryRequest, WorkerStatusRequest,
    WorkerStatusResponse,
};
use crate::request::Request;
use std::time::Duration;
use thiserror::Error;
use tokio_retry2::strategy::{jitter, ExponentialFactorBackoff};
use tokio_retry2::{Retry, RetryError};
use tonic::transport::{Channel, Endpoint};
use tracing::{info, instrument};

mod worker_rpc_service {
    tonic::include_proto!("worker_rpc");
}

#[derive(Error, Debug)]
pub(crate) enum ConnErr {
    #[error("Failed to connect to {1}: {0}")]
    Failed(tonic::transport::Error, GrpcAddr),
}

#[derive(Error, Debug)]
pub(crate) enum WorkerClientError {
    #[error(transparent)]
    Connection(#[from] ConnErr),

    #[error("gRPC error: {0}")]
    GrpcError(#[from] tonic::Status),

    #[error("Internal error: {0}")]
    InternalError(String),
}

// Wrapper types to avoid conflicting implementations
#[derive(Debug, Clone)]
pub struct StartFragmentPayload(pub FragmentId);

#[derive(Debug, Clone)]
pub struct UnregisterFragmentPayload(pub FragmentId);

// Request type aliases for worker RPCs
pub type RegisterFragmentRequest =
    Request<FragmentId, Result<RegisterQueryReply, WorkerClientError>>;
pub type StartFragmentRequest = Request<StartFragmentPayload, Result<(), WorkerClientError>>;
pub type StopFragmentRequest = Request<(FragmentId, StopMode), Result<(), WorkerClientError>>;
pub type UnregisterFragmentRequest =
    Request<UnregisterFragmentPayload, Result<(), WorkerClientError>>;
pub type GetFragmentStatusRequest =
    Request<FragmentId, Result<QueryStatusReply, WorkerClientError>>;
pub type GetWorkerStatusRequest = Request<(), Result<WorkerStatusResponse, WorkerClientError>>;

pub(crate) enum Rpc {
    RegisterFragment(RegisterFragmentRequest),
    StartFragment(StartFragmentRequest),
    StopFragment(StopFragmentRequest),
    UnregisterFragment(UnregisterFragmentRequest),
    GetFragmentStatus(GetFragmentStatusRequest),
    GetWorkerStatus(GetWorkerStatusRequest),
}

use crate::into_request;

into_request!(RegisterFragment, RegisterFragmentRequest, Rpc);
into_request!(StartFragment, StartFragmentRequest, Rpc);
into_request!(StopFragment, StopFragmentRequest, Rpc);
into_request!(UnregisterFragment, UnregisterFragmentRequest, Rpc);
into_request!(GetFragmentStatus, GetFragmentStatusRequest, Rpc);
into_request!(GetWorkerStatus, GetWorkerStatusRequest, Rpc);

#[derive(Debug)]
pub(crate) struct WorkerClient {
    grpc_addr: GrpcAddr,
    rpc_listener: flume::Receiver<Rpc>,
    client: WorkerRpcServiceClient<Channel>,
}

impl WorkerClient {
    const ENDPOINT_KEEP_ALIVE_INTERVAL_SEC: u64 = 60;
    const ENDPOINT_KEEP_ALIVE_TIMEOUT_SEC: u64 = 60;

    pub fn grpc_addr(&self) -> GrpcAddr {
        self.grpc_addr.clone()
    }

    #[instrument(fields(grpc_addr = %grpc_addr))]
    pub async fn connect(
        grpc_addr: GrpcAddr,
    ) -> Result<(flume::Sender<Rpc>, WorkerClient), ConnErr> {
        info!("Attempting to connect");
        const INITIAL_BACKOFF_MS: u64 = 1000;
        const BACKOFF_FACTOR: f64 = 2.0;
        const MAX_DELAY: Duration = Duration::from_secs(60);

        let connect_retry =
            ExponentialFactorBackoff::from_millis(INITIAL_BACKOFF_MS, BACKOFF_FACTOR)
                .max_delay(MAX_DELAY)
                .map(jitter);

        let channel = Retry::spawn(connect_retry, || async {
            let endpoint = Endpoint::from_shared(format!("http://{}", grpc_addr))
                .map_err(|e| RetryError::permanent(ConnErr::Failed(e, grpc_addr.clone())))?;

            endpoint
                .http2_keep_alive_interval(Duration::from_secs(
                    Self::ENDPOINT_KEEP_ALIVE_INTERVAL_SEC,
                ))
                .keep_alive_timeout(Duration::from_secs(Self::ENDPOINT_KEEP_ALIVE_TIMEOUT_SEC))
                .connect_timeout(Duration::from_secs(5))
                .connect()
                .await
                .map_err(|e| {
                    info!("Retrying connection establishment");
                    RetryError::transient(ConnErr::Failed(e, grpc_addr.clone()))
                })
        })
        .await?;
        info!("Established connection");

        let (rpc_sender, rpc_listener) = flume::bounded(64);
        Ok((
            rpc_sender,
            WorkerClient {
                grpc_addr,
                client: WorkerRpcServiceClient::new(channel),
                rpc_listener,
            },
        ))
    }

    #[instrument(fields(grpc_addr = %self.grpc_addr))]
    pub async fn run(self) -> () {
        while let Ok(rpc) = self.rpc_listener.recv_async().await {
            // Since recv is called sequentially, we want to avoid blocking requests by awaiting the responses
            // Instead, we clone the client (cheap) and handle the RPC in a background task
            match rpc {
                Rpc::RegisterFragment(req) => {
                    tokio::spawn(register_fragment(self.client.clone(), req));
                }
                Rpc::StartFragment(req) => {
                    tokio::spawn(start_fragment(self.client.clone(), req));
                }
                Rpc::StopFragment(req) => {
                    tokio::spawn(stop_fragment(self.client.clone(), req));
                }
                Rpc::UnregisterFragment(req) => {
                    tokio::spawn(unregister_fragment(self.client.clone(), req));
                }
                Rpc::GetFragmentStatus(req) => {
                    tokio::spawn(get_fragment_status(self.client.clone(), req));
                }
                Rpc::GetWorkerStatus(req) => {
                    tokio::spawn(get_worker_status(self.client.clone(), req));
                }
            }
        }
    }
}

async fn register_fragment(
    mut client: WorkerRpcServiceClient<Channel>,
    req: RegisterFragmentRequest,
) {
    let result = client
        .register_query(RegisterQueryRequest {
            query_id: req.payload,
        })
        .await
        .map(|response| response.into_inner())
        .map_err(WorkerClientError::from);

    let _ = req.respond(result);
}

async fn start_fragment(mut client: WorkerRpcServiceClient<Channel>, req: StartFragmentRequest) {
    let result = client
        .start_query(StartQueryRequest {
            query_id: req.payload.0,
        })
        .await
        .map(|_| ())
        .map_err(WorkerClientError::from);

    let _ = req.respond(result);
}

async fn stop_fragment(mut client: WorkerRpcServiceClient<Channel>, req: StopFragmentRequest) {
    let (id, stop_mode) = req.payload.clone();
    let result = client
        .stop_query(StopQueryRequest {
            query_id: id,
            termination_type: stop_mode.into(),
        })
        .await
        .map(|_| ())
        .map_err(WorkerClientError::from);

    let _ = req.respond(result);
}

async fn unregister_fragment(
    mut client: WorkerRpcServiceClient<Channel>,
    req: UnregisterFragmentRequest,
) {
    let result = client
        .unregister_query(UnregisterQueryRequest {
            query_id: req.payload.0,
        })
        .await
        .map(|_| ())
        .map_err(WorkerClientError::from);

    let _ = req.respond(result);
}

async fn get_fragment_status(
    mut client: WorkerRpcServiceClient<Channel>,
    req: GetFragmentStatusRequest,
) {
    let result = client
        .request_query_status(QueryStatusRequest {
            query_id: req.payload,
        })
        .await
        .map(|response| response.into_inner())
        .map_err(WorkerClientError::from);

    let _ = req.respond(result);
}

async fn get_worker_status(
    mut client: WorkerRpcServiceClient<Channel>,
    req: GetWorkerStatusRequest,
) {
    let result = client
        .request_status(WorkerStatusRequest {
            after_unix_timestamp_in_ms: 0,
        })
        .await
        .map(|response| response.into_inner())
        .map_err(WorkerClientError::from);

    let _ = req.respond(result);
}
