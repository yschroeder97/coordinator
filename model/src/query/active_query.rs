use super::fragment::{FragmentError, FragmentId};
use super::query_state::{DesiredQueryState, QueryState};
use super::{QueryId, StopMode};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, DeriveEntityModel)]
#[sea_orm(table_name = "active_query")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: QueryId,
    pub statement: String,
    pub current_state: QueryState,
    pub desired_state: DesiredQueryState,
    pub start_timestamp: Option<chrono::DateTime<chrono::Local>>,
    pub stop_timestamp: Option<chrono::DateTime<chrono::Local>>,
    pub stop_mode: Option<StopMode>,
    #[sea_orm(column_type = "JsonBinary")]
    pub error: Option<QueryError>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::fragment::Entity")]
    Fragment,
}

impl Related<super::fragment::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Fragment.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct QueryError {
    fragment_errors: HashMap<FragmentId, FragmentError>,
}
