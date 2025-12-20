use crate::catalog::query_builder::{SqlOperation, ToSql, UpdateBuilder, WhereBuilder};
use crate::catalog::tables::table;
use crate::catalog::tables::{queries, workers};
use crate::catalog::worker::worker_endpoint::{HostName, NetworkAddr};
use crate::errors::CoordinatorErr;
use crate::request::Request;
use sqlx::sqlite::SqliteArguments;
use strum::Display;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, sqlx::Type, Display)]
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

#[derive(Clone, Copy, Debug, sqlx::Type, Display)]
pub enum QueryState {
    Pending,     // Query was (partially) submitted/started
    Deploying,   // Query is in the deployment process
    Running,     // All query fragments are running
    Terminating, // Query is in the process of termination
    Completed,   // Query completed by itself
    Stopped,     // Query was stopped from the outside
    Failed,      // Query failed
}

#[derive(Clone, Debug, sqlx::Type)]
pub enum QueryFragmentState {
    Pending,
    Registering,
    Registered,
    Starting,
    Started,
    Running,
    Completed,
    Stopped,
    Failed,
}

pub type QueryId = String;
pub type FragmentId = u64;

#[derive(Debug, Clone)]
pub struct QueryFragment {
    pub query_id: QueryId,
    pub host_name: HostName,
    pub grpc_port: u16,
    pub current_state: QueryFragmentState,
    pub desired_state: QueryFragmentState,
    pub plan: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct Query {
    id: QueryId,
    stmt: String,
    current_state: QueryState,
    desired_state: QueryState,
    submission_timestamp: String,
}

#[derive(Clone, Debug)]
pub struct CreateQuery {
    pub name: QueryId,
    pub stmt: String,
    pub on_workers: Vec<NetworkAddr>,
}
pub type CreateQueryRequest = Request<CreateQuery, Result<(), CoordinatorErr>>;

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
pub type DropQueryRequest = Request<DropQuery, Result<(), CoordinatorErr>>;

impl ToSql for DropQuery {
    fn to_sql(&self) -> (String, SqliteArguments<'_>) {
        UpdateBuilder::on_table(table::QUERIES)
            .set(queries::DESIRED_STATE, QueryState::Stopped)
            .add_where()
            .eq(queries::ID, self.with_id.clone())
            .eq(queries::CURRENT_STATE, self.with_current_state)
            .eq(queries::DESIRED_STATE, self.with_desired_state)
            .eq(queries::STOP_MODE, self.stop_mode)
            .into_parts()
    }
}

#[derive(Clone, Debug)]
pub struct GetQuery {
    pub with_id: Option<QueryId>,
    pub with_current_state: Option<QueryState>,
    pub with_desired_state: Option<QueryState>,
}
pub type GetQueryRequest = Request<GetQuery, Result<(), CoordinatorErr>>;

impl ToSql for GetQuery {
    fn to_sql(&self) -> (String, SqliteArguments<'_>) {
        WhereBuilder::from(SqlOperation::Select(table::QUERIES))
            .eq(workers::HOST_NAME, self.with_id.clone())
            .eq(workers::CURRENT_STATE, self.with_current_state)
            .eq(workers::DESIRED_STATE, self.with_desired_state)
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
        UpdateBuilder::on_table(table::QUERIES)
            .set(queries::CURRENT_STATE, self.new_current)
            .add_where()
            .eq(queries::ID, Some(self.id.clone()))
            .into_parts()
    }
}
