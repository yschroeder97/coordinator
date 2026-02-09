use crate::query::active_query::QueryError;
use crate::query::query_state::TerminationState;
use crate::query::{QueryId, StopMode};
use sea_orm::entity::prelude::*;

#[derive(Debug, Clone, DeriveEntityModel)]
#[sea_orm(table_name = "terminated_query")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub query_id: QueryId,
    pub statement: String,
    pub termination_state: TerminationState,
    pub start_timestamp: chrono::DateTime<chrono::Local>,
    pub stop_timestamp: chrono::DateTime<chrono::Local>,
    pub stop_mode: Option<StopMode>,
    #[sea_orm(column_type = "JsonBinary")]
    pub error: Option<QueryError>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
