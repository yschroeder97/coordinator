use crate::query::QueryName;
use crate::query::query_state::QueryState;
use crate::worker::endpoint::{GrpcAddr, HostAddr};
use sea_orm::entity::prelude::*;
use sea_orm::{FromJsonQueryResult, NotSet, Set};
use serde::{Deserialize, Serialize};
use strum::Display;
use thiserror::Error;

pub type FragmentId = i64;

#[derive(Debug, Clone, Error, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub enum FragmentError {
    #[error("Internal worker error; code: {code}, msg: {msg}, stacktrace: {trace}")]
    WorkerInternal {
        code: u64,
        msg: String,
        trace: String,
    },
    #[error("Worker communication error: {msg}")]
    WorkerCommunication { msg: String },
}

#[derive(Debug, Clone, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "fragment")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub id: FragmentId,
    pub query_id: i64,
    pub host_addr: HostAddr,
    pub grpc_addr: GrpcAddr,
    pub plan: serde_json::Value,
    pub used_capacity: i32,
    pub has_source: bool,
    pub current_state: FragmentState,
    pub start_timestamp: Option<chrono::DateTime<chrono::Local>>,
    pub stop_timestamp: Option<chrono::DateTime<chrono::Local>>,
    #[sea_orm(column_type = "JsonBinary")]
    pub error: Option<FragmentError>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "crate::query::Entity",
        from = "Column::QueryId",
        to = "crate::query::Column::Id",
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

impl Related<crate::query::Entity> for Entity {
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
    pub query_id: i64,
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
            start_timestamp: NotSet,
            stop_timestamp: NotSet,
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
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
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

impl FragmentState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Stopped | Self::Failed)
    }

    pub fn next(self) -> Option<Self> {
        match self {
            Self::Pending => Some(Self::Registered),
            Self::Registered => Some(Self::Started),
            Self::Started => Some(Self::Running),
            Self::Running => Some(Self::Completed),
            Self::Completed => None,
            Self::Stopped => None,
            Self::Failed => None,
        }
    }
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
