use crate::cluster::ClusterServiceError::RegisterQueryError;
use crate::data_model::query::QueryId;
use crate::data_model::worker::HostName;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;
use thiserror::Error;
use tokio::task::{JoinError, JoinSet};
use tonic::Status;
use tonic::transport::{Channel, Error};
use tracing::{info_span, Instrument};
use tracing::log::{error, info};
use worker_rpc_service::worker_rpc_service_client::WorkerRpcServiceClient;
use worker_rpc_service::{RegisterQueryRequest, StartQueryRequest};

mod worker_rpc_service {
    tonic::include_proto!("worker_rpc");
}

#[derive(Error, Debug)]
pub enum ClusterServiceError {
    #[error("Failed to register query '{query_id}' on '{addr}' with status: '{status}'")]
    RegisterQueryError {
        query_id: QueryId,
        addr: GrpcAddr,
        status: Status,
    },
    #[error("Failed to start query '{query_id}' on '{addr}' with '{status}'")]
    StartQueryError {
        query_id: QueryId,
        addr: GrpcAddr,
        status: Status,
    },

    #[error("Worker with addr '{addr}' is not part of the cluster")]
    WorkerNotRegistered { addr: GrpcAddr },

    #[error("Could not connect to worker with '{addr}'")]
    ConnectionError { addr: GrpcAddr },

    #[error("Unhandled task panic or cancellation")]
    JoinError {},
}

trait RpcCall<Rsp> {
    async fn call(
        self,
        membership: Arc<HashMap<GrpcAddr, WorkerRpcServiceClient<Channel>>>,
    ) -> Result<Rsp, ClusterServiceError>;
}

struct SubmitQueryRequest {
    query_id: QueryId,
    on_workers: Vec<GrpcAddr>,
}

struct RegisteredWorker {
    addr: GrpcAddr,
    client: WorkerRpcServiceClient<Channel>,
    worker_query_id: u64,
}

impl SubmitQueryRequest {
    async fn register_query(
        &self,
        workers: Vec<(GrpcAddr, WorkerRpcServiceClient<Channel>)>,
    ) -> Result<Vec<RegisteredWorker>, ClusterServiceError> {
        let mut register_tasks = JoinSet::new();
        for (addr, mut client) in workers {
            let query_id = self.query_id.clone();
            register_tasks.spawn(async move {
                let reply = client
                    .register_query(tonic::Request::new(RegisterQueryRequest {}))
                    .await
                    .map_err(|status| RegisterQueryError {
                        query_id: query_id.clone(),
                        addr: addr.clone(),
                        status,
                    })?
                    .into_inner();
                info!(
                    "Successfully registered query {} on worker {} (worker assigned id: {})",
                    query_id, addr, reply.query_id
                );
                Ok(RegisteredWorker {
                    addr,
                    client,
                    worker_query_id: reply.query_id,
                })
            });
        }

        let mut registered = Vec::new();
        while let Some(res) = register_tasks.join_next().await {
            registered.push(res.expect("Task panicked or cancelled")?);
        }

        Ok(registered)
    }

    async fn start_query(&self, workers: Vec<RegisteredWorker>) -> Result<(), ClusterServiceError> {
        let mut start_tasks = JoinSet::new();
        for RegisteredWorker {
            addr,
            mut client,
            worker_query_id,
        } in workers
        {
            let query_id = self.query_id.clone();
            start_tasks.spawn(async move {
                client
                    .start_query(tonic::Request::new(StartQueryRequest {
                        query_id: worker_query_id,
                    }))
                    .await
                    .map_err(|status| ClusterServiceError::StartQueryError {
                        query_id: query_id.clone(),
                        addr: addr.clone(),
                        status,
                    })?;
                info!("Successfully started query {} on worker {}", query_id, addr);
                Ok(())
            });
        }

        while let Some(res) = start_tasks.join_next().await {
            res.expect("Task panicked")?;
        }

        Ok(())
    }
}

impl RpcCall<()> for SubmitQueryRequest {
    async fn call(
        self,
        membership: Arc<HashMap<GrpcAddr, WorkerRpcServiceClient<Channel>>>,
    ) -> Result<(), ClusterServiceError> {
        let workers_to_register = self
            .on_workers
            .iter()
            .map(|addr| {
                membership
                    .get(addr)
                    .ok_or_else(|| ClusterServiceError::WorkerNotRegistered { addr: addr.clone() })
                    .map(|client| (addr.clone(), client.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let registered = self.register_query(workers_to_register).await?;
        self.start_query(registered).await
    }
}

pub enum ClusterRequest {
    SubmitQuery(SubmitQueryRequest),
}

pub enum ClusterResponse {}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GrpcAddr {
    host: HostName,
    port: u16,
}

impl fmt::Display for GrpcAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

pub struct ClusterService {
    receiver: flume::Receiver<ClusterRequest>,
    membership: Arc<HashMap<GrpcAddr, WorkerRpcServiceClient<Channel>>>,
}

impl ClusterService {
    fn create(members: Vec<GrpcAddr>) -> Result<flume::Sender<ClusterRequest>, ClusterServiceError> {
        let (tx, rx) = flume::bounded(16);
        let (init_tx, init_rx) = flume::bounded::<Result<(), ClusterServiceError>>(1);

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .expect("Should not happen with current thread runtime");

            let init_result = rt.block_on(ClusterService::init(members));

            match init_result {
                Ok(members) => {
                    let _ = init_tx.send(Ok(()));

                    let result = rt.block_on(async move {
                        Self {
                            receiver: rx,
                            membership: Arc::new(members),
                        }
                            .run()
                            .await
                    }).instrument(info_span!("cluster_service"));

                    if let Err(e) = result.inner() {
                        error!("ClusterService event loop failed: {}", e);
                    }
                }
                Err(e) => {
                    let _ = init_tx.send(Err(e));
                }
            }
        });

        match init_rx.recv() {
            Ok(Ok(())) => Ok(tx),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(ClusterServiceError::JoinError {}),
        }
    }

    async fn init(
        members: Vec<GrpcAddr>,
    ) -> Result<HashMap<GrpcAddr, WorkerRpcServiceClient<Channel>>, ClusterServiceError> {
        let mut connect_tasks = JoinSet::new();
        let num_members = members.len();

        for addr in members {
            connect_tasks.spawn(async move {
                let client =
                    WorkerRpcServiceClient::connect(format!("http://{}", addr.to_string())).await;
                (addr, client)
            });
        }

        let mut initialized = HashMap::with_capacity(num_members);
        while let Some(connect_result) = connect_tasks.join_next().await {
            match connect_result {
                Ok((addr, Ok(client))) => initialized.insert(addr, client),
                Ok((addr, Err(connect_err))) => return Err(ClusterServiceError::ConnectionError { addr }),
                Err(_) => return Err(ClusterServiceError::JoinError {}),
            };
        }
        Ok(initialized)
    }

    async fn run(self) -> Result<(), ClusterServiceError> {
        while let Ok(req) = self.receiver.recv_async().await {
            match req {
                ClusterRequest::SubmitQuery(submit_query) => {
                    let workers = Arc::clone(&self.membership);
                    let _ = tokio::spawn(async move {
                        submit_query.call(workers).await?;
                        Ok::<_, ClusterServiceError>(())
                    })
                        .instrument(tracing::info_span!("submit_query"))
                        .await;
                }
            }
        }
        info!("Cluster service terminated");
        Ok(())
    }
}

#[cfg(test)]
mod cluster_service_tests {
    use super::*;

    #[test]
    fn test_connection_fails() {
        let sender = ClusterService::create(vec![GrpcAddr { host: "localhost".to_string(), port: 8080 }]);
        assert!(sender.is_err());
    }
}
