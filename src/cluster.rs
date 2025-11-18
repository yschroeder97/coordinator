use crate::cluster::ClusterServiceError::RegisterQueryError;
use crate::data_model::query::QueryId;
use crate::data_model::worker::GrpcAddr;
use std::collections::HashMap;
use thiserror::Error;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;
use tonic::transport::Channel;
use tonic::{Request, Status};
use tracing::instrument;
use tracing::{error, info};
use worker_rpc_service::worker_rpc_service_client::WorkerRpcServiceClient;
use worker_rpc_service::{RegisterQueryRequest, StartQueryRequest};

mod worker_rpc_service {
    tonic::include_proto!("worker_rpc");
}

#[derive(Error, Debug)]
pub enum ClusterServiceError {
    #[error("Failed to register query on '{addr}' with status: '{status}'")]
    RegisterQueryError { addr: GrpcAddr, status: Status },
    #[error("Failed to start query '{query_id}' on '{addr}' with '{status}'")]
    StartQueryError {
        query_id: QueryId,
        addr: GrpcAddr,
        status: Status,
    },

    #[error("Worker with addr '{addr}' is not part of the cluster")]
    WorkerNotRegistered { addr: GrpcAddr },

    #[error("Could not connect to mock_worker with '{addr}'")]
    ConnectionError { addr: GrpcAddr, msg: String },

    #[error("Unhandled task panic or cancellation")]
    JoinError {},
}

pub struct ClusterService {
    rt: Runtime,
    pub membership: HashMap<GrpcAddr, WorkerRpcServiceClient<Channel>>,
}

impl ClusterService {
    pub fn create(members: Vec<GrpcAddr>) -> Result<ClusterService, ClusterServiceError> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_io()
            .enable_time()
            .build()
            .expect("Failed to create tokio runtime");

        let members = rt.block_on(ClusterService::init(members))?;
        info!("Created cluster service");
        Ok(ClusterService {
            rt,
            membership: members,
        })
    }

    #[instrument(skip(members))]
    async fn init(
        members: Vec<GrpcAddr>,
    ) -> Result<HashMap<GrpcAddr, WorkerRpcServiceClient<Channel>>, ClusterServiceError> {
        let mut connect_tasks = JoinSet::new();
        let num_members = members.len();

        for addr in members {
            info!("Initializing connection to worker [{}]", addr);
            connect_tasks.spawn(async move {
                let client =
                    WorkerRpcServiceClient::connect(format!("http://{}", addr.to_string())).await;
                (addr, client)
            });
        }

        let mut initialized = HashMap::with_capacity(num_members);
        while let Some(connect_result) = connect_tasks.join_next().await {
            match connect_result {
                Ok((addr, Ok(client))) => {
                    info!("Connection to remote gRPC worker [{}] established", addr);
                    initialized.insert(addr, client)
                }
                Ok((addr, Err(connect_err))) => {
                    error!(
                        "Failed to established connection to remote gRPC worker [{}]: {}",
                        addr,
                        connect_err.to_string()
                    );
                    return Err(ClusterServiceError::ConnectionError {
                        addr,
                        msg: connect_err.to_string(),
                    });
                }
                Err(_) => return Err(ClusterServiceError::JoinError {}),
            };
        }
        Ok(initialized)
    }

    pub fn submit_query(&mut self, on_workers: Vec<GrpcAddr>) -> Result<(), ClusterServiceError> {
        self.rt.block_on(async {
            for addr in &on_workers {
                if let Some(client) = self.membership.get_mut(addr) {
                    let query_id = client
                        .register_query(Request::new(RegisterQueryRequest {}))
                        .await
                        .map_err(|status| RegisterQueryError {
                            addr: addr.clone(),
                            status,
                        })?;

                    client
                        .start_query(StartQueryRequest {
                            query_id: query_id.get_ref().query_id.clone(),
                        })
                        .await
                        .map_err(|status| ClusterServiceError::StartQueryError {
                            query_id: query_id.get_ref().query_id.clone(),
                            addr: addr.clone(),
                            status,
                        })?;
                } else {
                    return Err(ClusterServiceError::WorkerNotRegistered { addr: addr.clone() });
                }
            }

            Ok(())
        })
    }
}
