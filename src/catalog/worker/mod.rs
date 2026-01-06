pub mod worker_catalog;
pub mod endpoint;

use crate::catalog::query_builder::UpdateBuilder;
use crate::catalog::query_builder::{SqlOperation, ToSql, WhereBuilder};
use crate::catalog::tables::{table, workers};
use crate::catalog::worker::endpoint::{GrpcAddr, HostAddr, HostName};
use crate::errors::CoordinatorErr;
use crate::request::Request;
use sqlx::sqlite::SqliteArguments;
use strum::{Display, EnumIter};

#[derive(Debug, Clone, Copy, sqlx::Type, PartialEq, Display, EnumIter)]
pub enum WorkerState {
    Pending,
    Active,
    Unreachable,
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

#[derive(Debug, Clone)]
pub struct DropWorker {
    pub id: Option<GrpcAddr>,
    pub with_current_state: Option<WorkerState>,
    pub with_desired_state: Option<WorkerState>,
}
pub type DropWorkerRequest = Request<DropWorker, Result<(), CoordinatorErr>>;

impl ToSql for DropWorker {
    fn to_sql(&self) -> (String, SqliteArguments<'_>) {
        UpdateBuilder::on_table(table::WORKERS)
            .set(workers::DESIRED_STATE, WorkerState::Removed)
            .add_where()
            .eq(
                workers::HOST_NAME,
                self.id.as_ref().map(|addr| addr.host.clone()),
            )
            .eq(workers::GRPC_PORT, self.id.as_ref().map(|addr| addr.port))
            .eq(workers::CURRENT_STATE, self.with_current_state)
            .eq(workers::DESIRED_STATE, self.with_desired_state)
            .into_parts()
    }
}

#[derive(Debug, Clone, Default)]
pub struct GetWorker {
    pub id: Option<GrpcAddr>,
    pub with_current_state: Option<WorkerState>,
    pub with_desired_state: Option<WorkerState>,
}
pub type GetWorkerRequest = Request<GetWorker, Result<Vec<Worker>, CoordinatorErr>>;

impl GetWorker {
    pub fn new() -> Self {
        GetWorker::default()
    }

    pub fn with_id(mut self, addr: GrpcAddr) -> Self {
        self.id = Some(addr);
        self
    }

    pub fn with_current_state(mut self, current: WorkerState) -> Self {
        self.with_current_state = Some(current);
        self
    }

    pub fn with_desired_state(mut self, desired: WorkerState) -> Self {
        self.with_desired_state = Some(desired);
        self
    }
}

impl ToSql for GetWorker {
    fn to_sql(&self) -> (String, SqliteArguments<'_>) {
        WhereBuilder::from(SqlOperation::Select(table::WORKERS))
            .eq(
                workers::HOST_NAME,
                self.id.as_ref().map(|addr| addr.host.clone()),
            )
            .eq(workers::GRPC_PORT, self.id.as_ref().map(|addr| addr.port))
            .eq(workers::CURRENT_STATE, self.with_current_state)
            .eq(workers::DESIRED_STATE, self.with_desired_state)
            .into_parts()
    }
}

#[derive(Debug, Clone)]
pub struct MarkWorker<'a> {
    pub addr: &'a GrpcAddr,
    pub new_current: WorkerState,
}

impl ToSql for MarkWorker<'_> {
    fn to_sql(&self) -> (String, SqliteArguments<'_>) {
        UpdateBuilder::on_table(table::WORKERS)
            .set(workers::CURRENT_STATE, self.new_current)
            .add_where()
            .eq(workers::HOST_NAME, Some(self.addr.host.clone()))
            .eq(workers::GRPC_PORT, Some(self.addr.port))
            .into_parts()
    }
}
