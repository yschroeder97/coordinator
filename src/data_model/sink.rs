use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use crate::data_model::worker::HostName;
use crate::db_errors::{DatabaseError, ErrorTranslation};

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
pub enum SinkType {
    File,
    Print,
}

pub type SinkName = String;

/// Sink definition for query output.
///
/// # Equality and Hashing
/// Implements key-based equality: two sinks are equal if they have the same `name`,
/// regardless of placement, type, or configuration.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Sink {
    pub name: SinkName,
    pub placement: String,
    pub sink_type: SinkType,
    #[sqlx(json)]
    pub config: HashMap<String, String>,
}

impl PartialEq for Sink {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Sink {}

impl Hash for Sink {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

pub struct CreateSink {
    pub name: SinkName,
    pub placement: HostName,
    pub sink_type: SinkType,
    pub config: HashMap<String, String>,
}

impl PartialEq for CreateSink {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for CreateSink {}

impl Hash for CreateSink {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ErrorTranslation for CreateSink {
    fn unique_violation(&self, _err: sqlx::Error) -> DatabaseError {
        DatabaseError::SinkAlreadyExists {
            name: self.name.clone(),
        }
    }
}

pub struct ShowSinks {
    pub name: Option<SinkName>,
    pub on_node: Option<HostName>,
    pub by_type: Option<SinkType>,
}

pub struct DropSink {
    pub name: SinkName,
}

impl PartialEq for DropSink {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for DropSink {}

impl Hash for DropSink {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}


