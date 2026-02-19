pub mod endpoint;
pub mod network_link;

use endpoint::{GrpcAddr, HostAddr};
use network_link::Relation::{SourceWorker, TargetWorker};
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use sea_orm::{Condition, LinkDef, NotSet};
use strum::{Display, EnumIter};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Display, EnumIter, DeriveActiveEnum)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "worker_state",
    rename_all = "PascalCase"
)]
pub enum WorkerState {
    #[default]
    Pending,
    Active,
    Unreachable,
    Removed,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Display, EnumIter, DeriveActiveEnum)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "desired_worker_state",
    rename_all = "PascalCase"
)]
pub enum DesiredWorkerState {
    #[default]
    Active,
    Removed,
}

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "worker")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub host_addr: HostAddr,
    #[sea_orm(unique)]
    pub grpc_addr: GrpcAddr,
    pub capacity: i32,
    pub current_state: WorkerState,
    pub desired_state: DesiredWorkerState,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "crate::source::physical_source::Entity")]
    PhysicalSource,
}

impl Related<crate::source::physical_source::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PhysicalSource.def()
    }
}

#[async_trait::async_trait]
impl ActiveModelBehavior for ActiveModel {
    async fn before_save<C>(self, _db: &C, _insert: bool) -> Result<Self, DbErr>
    where
        C: ConnectionTrait,
    {
        if self.host_addr == self.grpc_addr {
            return Err(DbErr::Custom(format!(
                "GrpcAddr must be different from HostAddr: {:?}",
                self.host_addr
            )));
        }
        Ok(self)
    }
}

/// Linked trait to find outgoing neighbors (workers this worker points to).
/// Traverses: Worker -> NetworkLink (as source) -> Worker (as target)
pub struct WorkerToOutgoingNeighbors;

impl Linked for WorkerToOutgoingNeighbors {
    type FromEntity = Entity;
    type ToEntity = Entity;

    fn link(&self) -> Vec<LinkDef> {
        vec![
            // 1. From Worker, go "backward" into the junction table via SourceWorker
            SourceWorker.def().rev(),
            // 2. From the junction table, go "forward" to the target Worker
            TargetWorker.def(),
        ]
    }
}

/// Linked trait to find incoming neighbors (workers that point to this worker).
/// Traverses: Worker -> NetworkLink (as target) -> Worker (as source)
pub struct WorkerToIncomingNeighbors;

impl Linked for WorkerToIncomingNeighbors {
    type FromEntity = Entity;
    type ToEntity = Entity;

    fn link(&self) -> Vec<LinkDef> {
        vec![
            // 1. From Worker, go "backward" into the junction table via TargetWorker
            TargetWorker.def().rev(),
            // 2. From the junction table, go "forward" to the source Worker
            SourceWorker.def(),
        ]
    }
}

#[derive(Debug, Clone)]
pub struct CreateWorker {
    pub host_addr: HostAddr,
    pub grpc_addr: GrpcAddr,
    pub capacity: i32,
    pub peers: Vec<HostAddr>,
    /// Block the request until the worker reaches this state.
    /// Defaults to `WorkerState::default()` (Pending), meaning no blocking.
    pub block_until: WorkerState,
}

impl CreateWorker {
    pub fn new(host_addr: HostAddr, grpc_addr: GrpcAddr, capacity: i32) -> Self {
        Self {
            host_addr,
            grpc_addr,
            capacity,
            peers: Vec::new(),
            block_until: WorkerState::default(),
        }
    }

    pub fn with_peers(mut self, peers: Vec<HostAddr>) -> Self {
        self.peers = peers;
        self
    }

    pub fn block_until(mut self, state: WorkerState) -> Self {
        assert!(
            state != WorkerState::Removed && state != WorkerState::Unreachable,
            "Invalid target state: {:?}",
            state
        );
        self.block_until = state;
        self
    }

    pub fn should_block(&self) -> bool {
        self.block_until != WorkerState::Pending
    }
}

impl From<CreateWorker> for ActiveModel {
    fn from(req: CreateWorker) -> Self {
        Self {
            host_addr: Set(req.host_addr),
            grpc_addr: Set(req.grpc_addr),
            capacity: Set(req.capacity),
            current_state: NotSet,
            desired_state: NotSet,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct GetWorker {
    pub host_addr: Option<HostAddr>,
    pub current_state: Option<WorkerState>,
    pub desired_state: Option<DesiredWorkerState>,
}

impl GetWorker {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn with_host_addr(mut self, host_addr: HostAddr) -> Self {
        self.host_addr = Some(host_addr);
        self
    }

    pub fn with_current_state(mut self, state: WorkerState) -> Self {
        self.current_state = Some(state);
        self
    }

    pub fn with_desired_state(mut self, state: DesiredWorkerState) -> Self {
        self.desired_state = Some(state);
        self
    }
}

impl crate::IntoCondition for GetWorker {
    fn into_condition(self) -> Condition {
        Condition::all()
            .add_option(self.host_addr.map(|v| Column::HostAddr.eq(v)))
            .add_option(self.current_state.map(|v| Column::CurrentState.eq(v)))
            .add_option(self.desired_state.map(|v| Column::DesiredState.eq(v)))
    }
}

#[derive(Debug, Clone)]
pub struct DropWorker {
    pub host_addr: HostAddr,
    pub should_block: bool,
}

impl DropWorker {
    pub fn new(host_addr: HostAddr) -> Self {
        Self {
            host_addr,
            should_block: false,
        }
    }

    pub fn blocking(mut self) -> Self {
        self.should_block = true;
        self
    }

    pub fn should_block(&self) -> bool {
        self.should_block
    }
}
