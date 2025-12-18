use crate::catalog::query_builder::{SqlOperation, ToSql, WhereBuilder};
use crate::catalog::tables::{table, workers};
use crate::catalog::worker::worker_endpoint::{GrpcAddr, HostAddr, HostName};
use crate::errors::CoordinatorErr;
use crate::request::Request;
use sqlx::sqlite::SqliteArguments;
use strum::Display;

#[derive(Debug, Clone, Copy, sqlx::Type, PartialEq, Display)]
pub enum WorkerState {
    Pending,
    Active,
    Unreachable,
    Failed,
    Removed,
}

#[derive(Debug, sqlx::FromRow)]
pub struct Worker {
    pub host_name: HostName,
    #[sqlx(try_from = "i64")]
    pub grpc_port: u16,
    #[sqlx(try_from = "i64")]
    pub data_port: u16,
    #[sqlx(try_from = "i64")]
    pub capacity: u32,
    pub current_state: WorkerState,
    pub desired_state: WorkerState,
}

#[derive(Debug, Clone)]
pub struct CreateWorker {
    pub host_name: HostName,
    pub grpc_port: u16,
    pub data_port: u16,
    pub capacity: u32,
    pub peers: Vec<HostAddr>,
}
pub type CreateWorkerRequest = Request<CreateWorker, Result<(), CoordinatorErr>>;

#[derive(Debug)]
pub struct MarkWorkerDesired {
    pub id: GrpcAddr,
    pub desired_state: WorkerState,
}

impl MarkWorkerDesired {
    pub fn new(id: GrpcAddr, desired_state: WorkerState) -> Self {
        MarkWorkerDesired { id, desired_state }
    }
}

#[derive(Debug)]
pub struct MarkWorkerCurrent {
    pub id: GrpcAddr,
    pub current_state: WorkerState,
}

impl MarkWorkerCurrent {
    pub fn new(id: GrpcAddr, current_state: WorkerState) -> Self {
        MarkWorkerCurrent { id, current_state }
    }
}

#[derive(Debug, Clone)]
pub struct DropWorker {
    pub id: Option<GrpcAddr>,
    pub with_current_state: Option<WorkerState>,
    pub with_desired_state: Option<WorkerState>,
}
pub type DropWorkerRequest = Request<MarkWorkerDesired, Result<Vec<Worker>, CoordinatorErr>>;

impl ToSql for DropWorker {
    fn to_sql(&self) -> (String, SqliteArguments) {
        WhereBuilder::from(SqlOperation::Delete(table::WORKERS))
            .eq(
                workers::HOST_NAME,
                self.id.as_ref().and_then(|addr| Some(addr.host.clone())),
            )
            .eq(
                workers::GRPC_PORT,
                self.id.as_ref().and_then(|addr| Some(addr.port)),
            )
            .eq(workers::CURRENT_STATE, self.with_current_state)
            .eq(workers::DESIRED_STATE, self.with_desired_state)
            .into_parts()
    }
}
