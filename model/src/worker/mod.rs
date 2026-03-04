pub mod endpoint;
pub mod network_link;
pub mod topology;

use endpoint::{GrpcAddr, HostAddr};
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use sea_orm::{Condition, NotSet};
use strum::{Display, EnumIter};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Display, EnumIter, DeriveActiveEnum)]
#[sea_orm(
    rs_type = "String",
    db_type = "Text",
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
    db_type = "Text",
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

#[derive(Debug, Clone)]
pub struct CreateWorker {
    pub host_addr: HostAddr,
    pub grpc_addr: GrpcAddr,
    pub capacity: i32,
    pub peers: Vec<HostAddr>,
}

impl CreateWorker {
    pub fn new(host_addr: HostAddr, grpc_addr: GrpcAddr, capacity: i32) -> Self {
        Self {
            host_addr,
            grpc_addr,
            capacity,
            peers: Vec::new(),
        }
    }

    pub fn with_peers(mut self, peers: Vec<HostAddr>) -> Self {
        self.peers = peers;
        self
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
}

impl DropWorker {
    pub fn new(host_addr: HostAddr) -> Self {
        Self { host_addr }
    }
}

#[cfg(feature = "testing")]
fn arb_capacity() -> impl proptest::prelude::Strategy<Value = i32> {
    use proptest::prelude::*;
    prop_oneof![
        1 => Just(0i32),
        5 => 1..=16i32,
        3 => 17..=128i32,
        1 => 129..=1024i32,
    ]
}

#[cfg(feature = "testing")]
proptest::prop_compose! {
    pub fn arb_create_worker()(
        host_addr in endpoint::arb_host_addr(),
        grpc_port in 1024..65535u16,
        capacity in arb_capacity(),
    ) -> CreateWorker {
        let grpc_port = if grpc_port == host_addr.port {
            if grpc_port < 65534 { grpc_port + 1 } else { grpc_port - 1 }
        } else {
            grpc_port
        };
        let grpc_addr = endpoint::NetworkAddr::new(host_addr.host.clone(), grpc_port);
        CreateWorker::new(host_addr, grpc_addr, capacity)
    }
}
