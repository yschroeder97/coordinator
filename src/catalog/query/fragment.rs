use crate::catalog::catalog_errors::CatalogErr;
use crate::catalog::query::{ActiveQuery, DesiredQueryState, GetQuery, QueryId, QueryState};
use crate::catalog::query_builder::{SqlOperation, ToSql, WhereBuilder};
use crate::catalog::tables::{active_queries, query_fragments, table};
use crate::catalog::worker::endpoint::{GrpcAddr, HostName, NetworkAddr};
use crate::request::Request;
use sqlx::sqlite::SqliteArguments;
use strum::{Display, EnumIter, EnumString};

pub type FragmentId = u64;

#[derive(Debug, Clone)]
pub struct CreateQueryFragment {
    pub query_id: QueryId,
    pub host_name: HostName,
    pub grpc_port: u16,
    pub current_state: FragmentState,
    pub desired_state: FragmentState,
    pub plan: serde_json::Value,
    pub used_capacity: u32,
}

#[derive(Clone, Debug, Default)]
pub struct GetFragment {
    pub with_current_state: Option<QueryState>,
    pub with_desired_state: Option<DesiredQueryState>,
    pub on_worker: Option<NetworkAddr>,
    pub of_query: Option<QueryId>,
}
pub type GetQueryRequest = Request<GetFragment, Result<Vec<QueryFragment>, CatalogErr>>;

impl GetFragment {
    pub fn new() -> Self {
        GetFragment::default()
    }

    pub fn on_worker(mut self, addr: GrpcAddr) -> Self {
        self.on_worker = Some(addr);
        self
    }

    pub fn of_query(mut self, query_id: QueryId) -> Self {
        self.of_query = Some(query_id);
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

impl ToSql for GetFragment {
    fn to_sql(&self) -> (String, SqliteArguments<'_>) {
        WhereBuilder::from(SqlOperation::Select(table::QUERY_FRAGMENTS))
            .eq(query_fragments::QUERY_ID, self.of_query.clone())
            .eq(
                query_fragments::HOST_NAME,
                self.on_worker.clone().map(|w| w.host),
            )
            .eq(
                query_fragments::GRPC_PORT,
                self.on_worker.clone().map(|w| w.port),
            )
            .eq(active_queries::CURRENT_STATE, self.with_current_state)
            .eq(active_queries::DESIRED_STATE, self.with_desired_state)
            .into_parts()
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct QueryFragment {
    #[sqlx(try_from = "i64")]
    pub id: FragmentId,
    pub query_id: QueryId,
    pub host_name: HostName,
    pub grpc_port: u16,
    pub current_state: FragmentState,
    pub desired_state: FragmentState,
    pub plan: serde_json::Value,
    #[sqlx(try_from = "i64")]
    pub used_capacity: u32,
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

/// Request to update a fragment's current state
#[derive(Debug, Clone)]
pub struct MarkFragment {
    pub id: FragmentId,
    pub new_state: FragmentState,
}

impl From<Vec<FragmentState>> for QueryState {
    fn from(states: Vec<FragmentState>) -> Self {
        // Any failure means the query failed
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
        // Mixed states - default to the "lowest" active state
        QueryState::Pending
    }
}
