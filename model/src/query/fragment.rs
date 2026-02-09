use crate::query::query_state::QueryState;
use crate::query::{QueryId, active_query};
use crate::worker::endpoint::{GrpcAddr, HostAddr};
use sea_orm::entity::prelude::*;
use sea_orm::{NotSet, Set};
use serde::{Deserialize, Serialize};
use strum::Display;

pub type FragmentId = i64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct FragmentError {
    code: u64,
    msg: String,
    trace: String,
}

#[derive(Debug, Clone, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "fragment")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub id: FragmentId,
    pub query_id: QueryId,
    pub host_addr: HostAddr,
    pub grpc_addr: GrpcAddr,
    pub plan: serde_json::Value,
    pub used_capacity: i32,
    pub has_source: bool,
    pub current_state: FragmentState,
    pub started_timestamp: Option<chrono::DateTime<chrono::Local>>,
    pub running_timestamp: Option<chrono::DateTime<chrono::Local>>,
    pub stopped_timestamp: Option<chrono::DateTime<chrono::Local>>,
    #[sea_orm(column_type = "JsonBinary")]
    pub error: Option<FragmentError>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "crate::query::active_query::Entity",
        from = "Column::QueryId",
        to = "crate::query::active_query::Column::Id",
        on_update = "Restrict",
        on_delete = "Cascade"
    )]
    Query,
    #[sea_orm(
        belongs_to = "crate::worker::Entity",
        from = "Column::HostAddr",
        to = "crate::worker::Column::HostAddr",
        on_update = "Restrict",
        on_delete = "Restrict"
    )]
    Worker,
}

impl Related<active_query::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Query.def()
    }
}

impl Related<crate::worker::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Worker.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Clone, Debug)]
pub struct CreateFragment {
    pub query_id: QueryId,
    pub host_addr: HostAddr,
    pub grpc_addr: GrpcAddr,
    pub plan: serde_json::Value,
    pub used_capacity: i32,
    pub has_source: bool,
}

impl From<CreateFragment> for ActiveModel {
    fn from(req: CreateFragment) -> Self {
        Self {
            id: NotSet,
            query_id: Set(req.query_id),
            host_addr: Set(req.host_addr),
            grpc_addr: Set(req.grpc_addr),
            plan: Set(req.plan),
            used_capacity: Set(req.used_capacity),
            has_source: Set(req.has_source),
            current_state: NotSet,
            started_timestamp: NotSet,
            running_timestamp: NotSet,
            stopped_timestamp: NotSet,
            error: NotSet,
        }
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Display,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "fragment_state",
    rename_all = "PascalCase"
)]
#[strum(serialize_all = "PascalCase")]
pub enum FragmentState {
    #[default]
    Pending,
    Registered,
    Started,
    Running,
    Completed,
    Stopped,
    Failed,
}

/// Conversion from gRPC worker response integers.
/// Workers only return Registered(0), Started(1), Running(2), Stopped(3), Failed(4).
impl From<i32> for FragmentState {
    fn from(value: i32) -> Self {
        match value {
            0 => FragmentState::Registered,
            1 => FragmentState::Started,
            2 => FragmentState::Running,
            3 => FragmentState::Stopped,
            4 => FragmentState::Failed,
            _ => panic!("Tag {value} cannot be converted"),
        }
    }
}

/// Aggregated timestamps derived from a set of fragments.
pub struct QueryTimestamps {
    /// Earliest fragment start (query started when first fragment started).
    pub start: Option<chrono::DateTime<chrono::Local>>,
    /// Latest fragment stop (query stopped when last fragment finished).
    pub stop: Option<chrono::DateTime<chrono::Local>>,
}

impl From<Vec<FragmentState>> for QueryState {
    fn from(states: Vec<FragmentState>) -> Self {
        if states.contains(&FragmentState::Failed) {
            return QueryState::Failed;
        }
        // All completed means query completed
        if states.iter().all(|s| *s == FragmentState::Completed) {
            return QueryState::Completed;
        }
        // All stopped means query stopped
        if states.iter().all(|s| *s == FragmentState::Stopped) {
            return QueryState::Stopped;
        }
        // All running or started means query is running
        if states
            .iter()
            .all(|s| *s == FragmentState::Running || *s == FragmentState::Started)
        {
            return QueryState::Running;
        }
        // All registered means query is registered
        if states.iter().all(|s| *s == FragmentState::Registered) {
            return QueryState::Registered;
        }
        // All pending means query is still pending (planning)
        if states.iter().all(|s| *s == FragmentState::Pending) {
            return QueryState::Pending;
        }
        // Mixed QueryState - default to the "lowest" active state
        QueryState::Pending
    }
}
