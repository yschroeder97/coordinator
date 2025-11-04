use crate::data_model::sink::SinkName;
use crate::data_model::worker::HostName;
use crate::db_errors::{DatabaseError, ErrorTranslation};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
pub enum QueryState {
    Pending,
    Running,
    Completed,
    Failed,
}

pub type QueryId = String;

/// Query definition.
///
/// # Equality and Hashing
/// Implements key-based equality: two queries are equal if they have the same `id`,
/// regardless of statement, state, or sink.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Query {
    pub id: QueryId,
    pub statement: String,
    pub state: QueryState,
    pub sink: SinkName,
}

impl PartialEq for Query {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Query {}

impl Hash for Query {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct QueryFragment {
    pub query_id: QueryId,
    pub worker_id: HostName,
    pub state: QueryState,
}

pub struct CreateQuery {
    pub id: QueryId,
    pub statement: String,
    pub sink: SinkName,
}

impl PartialEq for CreateQuery {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for CreateQuery {}

impl Hash for CreateQuery {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl ErrorTranslation for CreateQuery {
    fn unique_violation(&self, _err: sqlx::Error) -> DatabaseError {
        DatabaseError::QueryAlreadyExists {
            id: self.id.clone(),
        }
    }

    fn fk_violation(&self, _err: sqlx::Error) -> DatabaseError {
        DatabaseError::SinkNotFoundForQuery {
            sink_name: self.sink.clone(),
            query_id: self.id.clone(),
        }
    }
}

pub struct ShowQueries {
    pub query_id: Option<QueryId>,
    pub by_sink: Option<SinkName>,
}

pub struct DropQuery {
    pub query_id: String,
}

impl PartialEq for DropQuery {
    fn eq(&self, other: &Self) -> bool {
        self.query_id == other.query_id
    }
}

impl Eq for DropQuery {}

impl Hash for DropQuery {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.query_id.hash(state);
    }
}
