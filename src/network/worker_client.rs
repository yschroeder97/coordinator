use crate::catalog::query::{FragmentId, StopMode};
use crate::catalog::worker::endpoint::GrpcAddr;
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
use tracing::{info, instrument, warn};

mod worker_rpc_service {
    tonic::include_proto!("worker_rpc");
}

#[derive(Error, Debug)]
pub(crate) enum WorkerClientErr {
    #[error("Failed to connect to {1}: {0}")]
    Connection(tonic::transport::Error, GrpcAddr),

    #[error("gRPC error at '{0}'")]
    GrpcError(GrpcAddr),
}

impl WorkerClientErr {
    pub fn addr(&self) -> &GrpcAddr {
        match self {
            WorkerClientErr::Connection(_, addr) => addr,
            WorkerClientErr::GrpcError(addr) => addr,
        }
    }
}

// Wrapper types to avoid conflicting implementations
#[derive(Debug, Clone)]
pub struct StartFragmentPayload(pub FragmentId);

#[derive(Debug, Clone)]
pub struct UnregisterFragmentPayload(pub FragmentId);

// Request type aliases for worker RPCs
pub type RegisterFragmentRequest = Request<FragmentId, Result<RegisterQueryReply, WorkerClientErr>>;
pub type StartFragmentRequest = Request<StartFragmentPayload, Result<(), WorkerClientErr>>;
pub type StopFragmentRequest = Request<(FragmentId, StopMode), Result<(), WorkerClientErr>>;
pub type UnregisterFragmentRequest =
    Request<UnregisterFragmentPayload, Result<(), WorkerClientErr>>;
pub type GetFragmentStatusRequest = Request<FragmentId, Result<QueryStatusReply, WorkerClientErr>>;
pub type GetWorkerStatusRequest = Request<(), Result<WorkerStatusResponse, WorkerClientErr>>;

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
    ) -> Result<(flume::Sender<Rpc>, WorkerClient), WorkerClientErr> {
        info!("Attempting to connect");
        const INITIAL_BACKOFF_MS: u64 = 1000;
        const BACKOFF_FACTOR: f64 = 2.0;
        const MAX_RETRIES: usize = 6;

        let connect_retry =
            ExponentialFactorBackoff::from_millis(INITIAL_BACKOFF_MS, BACKOFF_FACTOR)
                .map(jitter)
                .take(MAX_RETRIES);

        let channel = Retry::spawn(connect_retry, || async {
            let endpoint = Endpoint::from_shared(format!("http://{}", grpc_addr)).map_err(|e| {
                RetryError::permanent(WorkerClientErr::Connection(e, grpc_addr.clone()))
            })?;

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
                    RetryError::transient(WorkerClientErr::Connection(e, grpc_addr.clone()))
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
        macro_rules! dispatch {
            ($client:expr, $tx:expr, $method:ident, $req:expr) => {
                let addr = self.grpc_addr.clone();
                tokio::spawn(async move {
                    let res = $client.$method($req).await;
                    reply_to(
                        $tx,
                        res.map(|resp| resp.into_inner())
                            .map_err(|_| WorkerClientErr::GrpcError(addr)),
                    );
                });
            };
            ($client:expr, $tx:expr, $method:ident, $req:expr, unit) => {
                let addr = self.grpc_addr.clone();
                tokio::spawn(async move {
                    let res = $client.$method($req).await;
                    reply_to(
                        $tx,
                        res.map(|_| ())
                            .map_err(|_| WorkerClientErr::GrpcError(addr)),
                    );
                });
            };
        }

        while let Ok(rpc) = self.rpc_listener.recv_async().await {
            let mut client = self.client.clone();
            match rpc {
                Rpc::RegisterFragment(Request {
                    payload: id,
                    reply_to: tx,
                }) => {
                    dispatch!(
                        client,
                        tx,
                        register_query,
                        RegisterQueryRequest { query_id: id }
                    );
                }
                Rpc::StartFragment(Request {
                    payload: StartFragmentPayload(id),
                    reply_to: tx,
                }) => {
                    dispatch!(
                        client,
                        tx,
                        start_query,
                        StartQueryRequest { query_id: id },
                        unit
                    );
                }
                Rpc::StopFragment(Request {
                    payload: (id, stop_mode),
                    reply_to: tx,
                }) => {
                    dispatch!(
                        client,
                        tx,
                        stop_query,
                        StopQueryRequest {
                            query_id: id,
                            termination_type: stop_mode.into(),
                        },
                        unit
                    );
                }
                Rpc::UnregisterFragment(Request {
                    payload: UnregisterFragmentPayload(id),
                    reply_to: tx,
                }) => {
                    dispatch!(
                        client,
                        tx,
                        unregister_query,
                        UnregisterQueryRequest { query_id: id },
                        unit
                    );
                }
                Rpc::GetFragmentStatus(Request {
                    payload: id,
                    reply_to: tx,
                }) => {
                    dispatch!(
                        client,
                        tx,
                        request_query_status,
                        QueryStatusRequest { query_id: id }
                    );
                }
                Rpc::GetWorkerStatus(Request {
                    payload: (),
                    reply_to: tx,
                }) => {
                    dispatch!(
                        client,
                        tx,
                        request_status,
                        WorkerStatusRequest {
                            after_unix_timestamp_in_ms: 0
                        }
                    );
                }
            }
        }
    }
}

fn reply_to<R, E>(tx: tokio::sync::oneshot::Sender<Result<R, WorkerClientErr>>, res: Result<R, E>)
where
    E: Into<WorkerClientErr>,
{
    let res = res.map_err(|e| e.into());
    if tx.send(res).is_err() {
        warn!("Requesting task dropped the receiver channel");
    }
}
