pub mod query_catalog;

use crate::catalog::catalog_errors::CatalogErr;
use crate::catalog::query_builder::{SqlOperation, ToSql, UpdateBuilder, WhereBuilder};
use crate::catalog::tables::active_queries;
use crate::catalog::tables::table;
use crate::catalog::worker::endpoint::{HostName, NetworkAddr};
use crate::request::Request;
#[cfg(test)]
use proptest_derive::Arbitrary;
use sqlx::sqlite::SqliteArguments;
use std::str::FromStr;
use strum::{Display, EnumIter, EnumString};
use uuid::Uuid;

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

#[cfg_attr(test, derive(Arbitrary))]
#[derive(Clone, Copy, Debug, PartialEq, sqlx::Type, Display, EnumIter, EnumString)]
pub enum QueryState {
    Pending,     // Query was (partially) submitted/started
    Deploying,   // Query is in the deployment process
    Running,     // All query fragments are running
    Terminating, // Query is in the process of termination
    Completed,   // Query completed by itself
    Stopped,     // Query was stopped from the outside
    Failed,      // Query failed
}

impl From<String> for QueryState {
    fn from(s: String) -> Self {
        QueryState::from_str(&s).unwrap_or(QueryState::Failed)
    }
}

impl QueryState {
    pub fn transitions(&self) -> Vec<QueryState> {
        match self {
            QueryState::Pending => vec![QueryState::Deploying],
            QueryState::Deploying => vec![QueryState::Running, QueryState::Failed],
            QueryState::Running => vec![
                QueryState::Terminating,
                QueryState::Failed,
                QueryState::Completed,
                QueryState::Stopped,
            ],
            QueryState::Terminating => vec![QueryState::Failed, QueryState::Stopped],
            // Terminal states have no valid next states
            QueryState::Completed | QueryState::Stopped | QueryState::Failed => vec![],
        }
    }
}

#[derive(Clone, Debug, PartialEq, sqlx::Type, Display, EnumIter, EnumString)]
pub enum FragmentState {
    Pending,
    Registered,
    Started,
    Running,
    Completed,
    Stopped,
    Failed,
}

impl From<Vec<FragmentState>> for QueryState {
    fn from(states: Vec<FragmentState>) -> Self {
        states
            .contains(&FragmentState::Failed)
            .then_some(QueryState::Failed)
            .or_else(|| {
                states
                    .iter()
                    .all(|s| *s == FragmentState::Stopped)
                    .then_some(QueryState::Stopped)
            })
            .or_else(|| {
                states
                    .iter()
                    .all(|s| *s == FragmentState::Completed)
                    .then_some(QueryState::Completed)
            })
            .or_else(|| {
                states
                    .iter()
                    .all(|s| *s == FragmentState::Running)
                    .then_some(QueryState::Running)
            })
            .or_else(|| {
                states
                    .iter()
                    .all(|s| *s == FragmentState::Pending)
                    .then_some(QueryState::Pending)
            })
            .unwrap_or(QueryState::Deploying)
    }
}

pub type QueryId = String;
pub type FragmentId = u64;

#[derive(Debug, Clone)]
pub struct Fragment {
    pub query_id: QueryId,
    pub host_name: HostName,
    pub grpc_port: u16,
    pub current_state: FragmentState,
    pub desired_state: FragmentState,
    pub plan: serde_json::Value,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ActiveQuery {
    pub id: QueryId,
    pub statement: String,
    pub current_state: QueryState,
    pub desired_state: QueryState,
    pub stop_mode: Option<StopMode>,
}

#[derive(Debug, Clone, sqlx::FromRow, PartialEq)]
pub struct QueryLogEntry {
    pub query_id: QueryId,
    pub statement: String,
    pub current_state: QueryState,
    pub desired_state: QueryState,
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
    pub on_workers: Vec<NetworkAddr>,
}
pub type CreateQueryRequest = Request<CreateQuery, Result<(), CatalogErr>>;

impl CreateQuery {
    pub fn new(stmt: String, on_workers: Vec<NetworkAddr>) -> Self {
        Self::new_with_name(Uuid::new_v4().to_string(), stmt, on_workers)
    }

    pub fn new_with_name(name: QueryId, stmt: String, on_workers: Vec<NetworkAddr>) -> Self {
        CreateQuery {
            name,
            stmt: stmt.to_string(),
            on_workers,
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
    pub with_desired_state: Option<QueryState>,
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

    pub fn with_desired_state(mut self, desired: QueryState) -> Self {
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
    pub new_current: QueryState,
}

impl ToSql for MarkQuery {
    fn to_sql(&self) -> (String, SqliteArguments<'_>) {
        UpdateBuilder::on_table(table::ACTIVE_QUERIES)
            .set(active_queries::CURRENT_STATE, self.new_current)
            .add_where()
            .eq(active_queries::ID, Some(self.id.clone()))
            .into_parts()
    }
}
