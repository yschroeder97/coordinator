use crate::catalog::query::QueryId;
use crate::catalog::worker::GrpcAddr;
use crate::requests::Request;
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinSet;
use tonic::transport::Channel;
use tonic::Status;
use tracing::{error, info};
use tracing::{instrument, warn};
use worker_rpc_service::stop_query_request::QueryTerminationType;
use worker_rpc_service::worker_rpc_service_client::WorkerRpcServiceClient;
use worker_rpc_service::StopQueryRequest;
use worker_rpc_service::{RegisterQueryRequest, StartQueryRequest};
use NetworkServiceError::RegisterQueryError;

mod worker_rpc_service {
    tonic::include_proto!("worker_rpc");
}

#[derive(Error, Debug)]
pub enum NetworkServiceError {
    #[error("Failed to register query on '{addr}' with status: '{status}'")]
    RegisterQueryError { addr: GrpcAddr, status: Status },
    #[error("Failed to start query '{query_id}' on '{addr}' with '{status}'")]
    StartQueryError {
        query_id: QueryId,
        addr: GrpcAddr,
        status: Status,
    },

    #[error("Failed to stop query '{query_id}' on '{addr}' with '{status}'")]
    StopQueryError {
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

pub struct SubmitQuery {
    id: QueryId,
    to_workers: Vec<GrpcAddr>,
}

pub struct UnsubmitQuery {
    id: QueryId,
    on_workers: Vec<GrpcAddr>,
}

pub type SubmitQueryRequest = Request<SubmitQuery, Result<(), NetworkServiceError>>;
pub type UnsubmitQueryRequest = Request<UnsubmitQuery, Result<(), NetworkServiceError>>;

pub enum NetworkServiceRequest {
    SubmitQuery(SubmitQueryRequest),
    UnsubmitQuery(UnsubmitQueryRequest),
}

pub struct NetworkService {
    receiver: Receiver<NetworkServiceRequest>,
    workers: HashMap<GrpcAddr, WorkerRpcServiceClient<Channel>>,
}

impl NetworkService {
    pub fn start(
        members: Vec<GrpcAddr>,
    ) -> Result<Sender<NetworkServiceRequest>, NetworkServiceError> {
        let (sender, receiver) = tokio::sync::mpsc::channel(1024);
        let (init_tx, init_rx) =
            std::sync::mpsc::sync_channel::<Result<(), NetworkServiceError>>(1);

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .expect("Failed to create tokio runtime");

            match rt.block_on(async move {
                let workers = NetworkService::init(members).await?;
                info!("Created cluster service");
                Ok(NetworkService { receiver, workers })
            }) {
                Ok(service) => {
                    init_tx
                        .send(Ok(()))
                        .expect("Receiver blocks on this signal");
                    rt.block_on(async move {
                        service.run().await;
                    });
                }
                Err(e) => {
                    init_tx
                        .send(Err(e))
                        .expect("Receiver blocks on this signal");
                }
            }
        });

        init_rx.recv().expect("Sender sends exactly one signal")?;
        Ok(sender)
    }

    async fn run(mut self) {
        while let Some(req) = self.receiver.recv().await {
            match req {
                NetworkServiceRequest::SubmitQuery(submit) => {
                    match self.submit_query(&submit.payload).await {
                        Ok(_) => submit
                            .respond_to
                            .send(Ok(()))
                            .expect("Sender still listening"),
                        Err(e) => submit
                            .respond_to
                            .send(Err(e))
                            .expect("Sender still listening"),
                    }
                }
                NetworkServiceRequest::UnsubmitQuery(unsubmit) => {
                    match self.unsubmit_query(&unsubmit.payload).await {
                        Ok(_) => unsubmit
                            .respond_to
                            .send(Ok(()))
                            .expect("Sender still listening"),
                        Err(e) => unsubmit
                            .respond_to
                            .send(Err(e))
                            .expect("Sender still listening"),
                    }
                }
            }
        }
        warn!("Network service stopped");
    }

    #[instrument(skip(members))]
    async fn init(
        members: Vec<GrpcAddr>,
    ) -> Result<HashMap<GrpcAddr, WorkerRpcServiceClient<Channel>>, NetworkServiceError> {
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
                    return Err(NetworkServiceError::ConnectionError {
                        addr,
                        msg: connect_err.to_string(),
                    });
                }
                Err(_) => return Err(NetworkServiceError::JoinError {}),
            };
        }
        Ok(initialized)
    }

    pub async fn submit_query(&mut self, submit: &SubmitQuery) -> Result<(), NetworkServiceError> {
        for addr in &submit.to_workers {
            if let Some(client) = self.workers.get_mut(addr) {
                let query_id = client
                    .register_query(tonic::Request::new(RegisterQueryRequest {}))
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
                    .map_err(|status| NetworkServiceError::StartQueryError {
                        query_id: query_id.get_ref().query_id.clone(),
                        addr: addr.clone(),
                        status,
                    })?;
            } else {
                return Err(NetworkServiceError::WorkerNotRegistered { addr: addr.clone() });
            }
        }

        Ok(())
    }

    pub async fn unsubmit_query(
        &mut self,
        unsubmit: &UnsubmitQuery,
    ) -> Result<(), NetworkServiceError> {
        for addr in &unsubmit.on_workers {
            if let Some(client) = self.workers.get_mut(addr) {
                client
                    .stop_query(StopQueryRequest {
                        query_id: unsubmit.id.clone(),
                        termination_type: 0,
                    })
                    .await
                    .map_err(|status| NetworkServiceError::StopQueryError {
                        query_id: unsubmit.id.clone(),
                        addr: addr.clone(),
                        status,
                    })?;
            } else {
                return Err(NetworkServiceError::WorkerNotRegistered { addr: addr.clone() });
            }
        }

        Ok(())
    }
}
