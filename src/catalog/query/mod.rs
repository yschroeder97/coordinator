pub(crate) mod fragment;
pub mod query_catalog;
pub(crate) mod query_state;

use crate::catalog::catalog_errors::CatalogErr;
pub(crate) use crate::catalog::query::query_state::{DesiredQueryState, QueryState};
use crate::catalog::query_builder::{SqlOperation, ToSql, UpdateBuilder, WhereBuilder};
use crate::catalog::tables::active_queries;
use crate::catalog::tables::table;
use crate::catalog::worker::endpoint::NetworkAddr;
use crate::error::BoxedErr;
use crate::request::Request;
use sqlx::sqlite::SqliteArguments;
use strum::Display;
use uuid::Uuid;

pub type QueryId = String;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ActiveQuery {
    pub id: QueryId,
    pub statement: String,
    pub current_state: QueryState,
    pub desired_state: DesiredQueryState,
    pub stop_mode: Option<StopMode>,
    pub error: Option<String>,
    pub stack_trace: Option<String>,
}

#[derive(Debug, Clone, sqlx::FromRow, PartialEq)]
pub struct QueryLogEntry {
    pub query_id: QueryId,
    pub statement: String,
    pub current_state: QueryState,
    pub desired_state: DesiredQueryState,
    pub timestamp: chrono::NaiveDateTime,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TerminatedQuery {
    pub id: Option<QueryId>,
    pub statement: String,
    pub termination_state: QueryState,
    pub error: Option<String>,
    pub stack_trace: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CreateQuery {
    pub name: QueryId,
    pub stmt: String,
}
pub type CreateQueryRequest = Request<CreateQuery, Result<(), BoxedErr>>;

impl CreateQuery {
    pub fn new(stmt: String) -> Self {
        Self::new_with_name(Uuid::new_v4().to_string(), stmt)
    }

    pub fn new_with_name(name: QueryId, stmt: String) -> Self {
        CreateQuery {
            name,
            stmt: stmt.to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DropQuery {
    pub with_id: Option<QueryId>,
    pub with_current_state: Option<QueryState>,
    pub with_desired_state: Option<QueryState>,
    pub on_worker: Option<NetworkAddr>,
    pub stop_mode: Option<StopMode>,
}
pub type DropQueryRequest = Request<DropQuery, Result<(), CatalogErr>>;

impl ToSql for DropQuery {
    fn to_sql(&self) -> (String, SqliteArguments<'_>) {
        UpdateBuilder::on_table(table::ACTIVE_QUERIES)
            .set(active_queries::DESIRED_STATE, QueryState::Stopped)
            .add_where()
            .eq(active_queries::ID, self.with_id.clone())
            .eq(active_queries::CURRENT_STATE, self.with_current_state)
            .eq(active_queries::DESIRED_STATE, self.with_desired_state)
            .eq(active_queries::STOP_MODE, self.stop_mode)
            .into_parts()
    }
}

#[derive(Clone, Debug, Default)]
pub struct GetQuery {
    pub with_id: Option<QueryId>,
    pub with_current_state: Option<QueryState>,
    pub with_desired_state: Option<DesiredQueryState>,
}
pub type GetQueryRequest = Request<GetQuery, Result<Vec<ActiveQuery>, CatalogErr>>;

impl GetQuery {
    pub fn new() -> Self {
        GetQuery::default()
    }

    pub fn with_id(mut self, id: QueryId) -> Self {
        self.with_id = Some(id);
        self
    }

    pub fn with_current_state(mut self, current: QueryState) -> Self {
        self.with_current_state = Some(current);
        self
    }

    pub fn with_desired_state(mut self, desired: DesiredQueryState) -> Self {
        self.with_desired_state = Some(desired);
        self
    }
}

impl ToSql for GetQuery {
    fn to_sql(&self) -> (String, SqliteArguments<'_>) {
        WhereBuilder::from(SqlOperation::Select(table::ACTIVE_QUERIES))
            .eq(active_queries::ID, self.with_id.clone())
            .eq(active_queries::CURRENT_STATE, self.with_current_state)
            .eq(active_queries::DESIRED_STATE, self.with_desired_state)
            .into_parts()
    }
}

#[derive(Debug, Clone)]
pub struct MarkQuery {
    pub id: QueryId,
    pub new_state: QueryState,
}

impl MarkQuery {
    pub(crate) fn new(id: QueryId, new_state: QueryState) -> Self {
        MarkQuery { id, new_state }
    }
}

impl ToSql for MarkQuery {
    fn to_sql(&self) -> (String, SqliteArguments<'_>) {
        UpdateBuilder::on_table(table::ACTIVE_QUERIES)
            .set(active_queries::CURRENT_STATE, self.new_state)
            .add_where()
            .eq(active_queries::ID, Some(self.id.clone()))
            .into_parts()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, sqlx::Type, Display)]
pub enum StopMode {
    Graceful,
    Forceful,
}

impl From<StopMode> for i32 {
    fn from(value: StopMode) -> Self {
        match value {
            StopMode::Graceful => 0,
            StopMode::Forceful => 1,
        }
    }
}
