use crate::catalog::catalog_errors::CatalogError;
use crate::catalog::catalog_errors::CatalogError::EmptyPredicate;
use crate::catalog::worker::{GrpcAddr, HostName};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StopMode {
    Graceful,
    Forceful,
}

#[derive(Clone, Debug, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "TEXT", rename_all = "PascalCase")]
pub enum QueryState {
    Pending,     // Query was (partially) submitted/started
    Deploying,   // Query is in the deployment process
    Running,     // All query fragments are running
    Terminating, // Query is in the process of termination
    Completed,   // Query completed by itself
    Stopped,     // Query was stopped from the outside
    Failed,      // Query failed
}

#[derive(Clone, Debug, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "TEXT", rename_all = "PascalCase")]
pub enum DesiredQueryState {
    Running,
    Stopped,
}

#[derive(Clone, Debug, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "TEXT")]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    pub id: QueryId,
    pub stmt: String,
    pub current_state: QueryState,
    pub desired_state: DesiredQueryState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryFragment {
    pub query_id: QueryId,
    pub host_name: HostName,
    pub grpc_port: u16,
    pub current_state: QueryFragmentState,
    pub desired_state: QueryFragmentState,
    pub plan: serde_json::Value,
}

#[derive(Clone, Debug)]
pub struct CreateQuery {
    pub name: QueryId,
    pub stmt: String,
    pub on_workers: Vec<GrpcAddr>,
}

impl CreateQuery {
    pub fn new(stmt: String, on_workers: Vec<GrpcAddr>) -> Self {
        Self::new_with_name(Uuid::new_v4().to_string(), stmt, on_workers)
    }

    pub fn new_with_name(name: QueryId, stmt: String, on_workers: Vec<GrpcAddr>) -> Self {
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
    pub on_worker: Option<GrpcAddr>,
    pub stop_mode: Option<StopMode>,
}

impl DropQuery {
    pub fn new(
        with_id: Option<QueryId>,
        with_state: Option<QueryState>,
        on_worker: Option<GrpcAddr>,
        stop_mode: Option<StopMode>,
    ) -> Result<Self, CatalogError> {
        if with_id.is_none() && with_state.is_none() && on_worker.is_none() {
            return Err(EmptyPredicate {});
        }
        Ok(Self {
            with_id,
            with_state,
            on_worker,
            stop_mode,
        })
    }

    pub fn stop_mode(&self) -> StopMode {
        self.stop_mode.clone().unwrap_or(StopMode::Graceful)
    }
}

#[derive(Clone, Debug)]
pub struct GetQuery {
    pub with_id: Option<QueryId>,
    pub with_state: Option<QueryState>,
    pub on_worker: Option<HostName>,
}
