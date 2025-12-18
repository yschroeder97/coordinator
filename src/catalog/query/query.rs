use crate::catalog::worker::worker_endpoint::{HostName, NetworkAddr};
use crate::errors::CoordinatorErr;
use crate::request::Request;
use strum::Display;
use uuid::Uuid;

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug, sqlx::Type, Display)]
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
    pub with_state: Option<QueryState>,
    pub on_worker: Option<NetworkAddr>,
    pub stop_mode: Option<StopMode>,
}
pub type DropQueryRequest = Request<DropQuery, Result<Vec<Query>, CoordinatorErr>>;

#[derive(Clone, Debug)]
pub struct GetQuery {
    pub with_id: Option<QueryId>,
    pub with_state: Option<QueryState>,
    pub on_worker: Option<HostName>,
}
