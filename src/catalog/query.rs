use crate::catalog::sink::SinkName;
use crate::catalog::worker::{GrpcAddr, HostName};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum GlobalQueryState {
    Pending,   // Query was (partially) submitted/started
    Running,   // All query fragments are running
    Completed, // Query completed by itself
    Stopped,   // Query was stopped from the outside
    Failed,    // Query failed
}

#[derive(Clone, Debug)]
pub enum LocalQueryState {
    Registered,
    Started,
    Running,
    Stopped,
    Failed,
}

pub type QueryId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    pub id: QueryId,
    pub stmt: String,
    pub state: GlobalQueryState,
    pub sink: SinkName,
}

impl PartialEq for Query {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Query {}

#[derive(Debug, Clone)]
pub struct QueryFragment {
    pub query_id: QueryId,
    pub worker_id: HostName,
    pub state: LocalQueryState,
}

#[derive(Clone, Debug)]
pub struct CreateQuery {
    pub name: QueryId,
    pub stmt: String,
    pub on_workers: Vec<GrpcAddr>,
}

impl CreateQuery {
    pub fn new(stmt: &str, on_workers: Vec<GrpcAddr>) -> Self {
        Self::with_name(Uuid::new_v4().into(), stmt, on_workers)
    }

    pub fn with_name(name: QueryId, stmt: &str, on_workers: Vec<GrpcAddr>) -> Self {
        CreateQuery {
            name,
            stmt: stmt.into(),
            on_workers,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DropQuery {
    pub with_id: Option<QueryId>,
    pub with_state: Option<GlobalQueryState>,
    pub on_worker: Option<HostName>,
}

#[derive(Clone, Debug)]
pub struct GetQuery {
    pub with_id: Option<QueryId>,
    pub with_state: Option<GlobalQueryState>,
    pub on_worker: Option<HostName>,
}
