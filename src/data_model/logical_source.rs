use std::hash::{Hash, Hasher};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use crate::data_model::schema::Schema;
use crate::db_errors::{DatabaseError, ErrorTranslation};
use crate::requests::{BaseEntityTag, DeltaTag, RequestHeader};

pub type LogicalSourceName = String;

/// Logical source definition.
///
/// # Equality and Hashing
/// Implements key-based equality: two sources are equal if they have the same `name`,
/// regardless of schema. This enables differential dataflow cancellation where CREATE
/// and DROP operations on the same source cancel out.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct LogicalSource {
    pub name: LogicalSourceName,
    #[sqlx(json)]
    pub schema: Schema,
}

impl PartialEq for LogicalSource {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for LogicalSource {}

impl PartialOrd for LogicalSource {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LogicalSource {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}

impl Hash for LogicalSource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct CreateLogicalSource {
    pub source_name: LogicalSourceName,
    pub schema: Schema,
}

impl PartialEq for CreateLogicalSource {
    fn eq(&self, other: &Self) -> bool {
        self.source_name == other.source_name
    }
}

impl Eq for CreateLogicalSource {}

impl PartialOrd for CreateLogicalSource {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CreateLogicalSource {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.source_name.cmp(&other.source_name)
    }
}

impl Hash for CreateLogicalSource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.source_name.hash(state);
    }
}

impl ErrorTranslation for CreateLogicalSource {
    fn unique_violation(&self, _err: sqlx::Error) -> DatabaseError {
        DatabaseError::LogicalSourceAlreadyExists {
            name: self.source_name.clone(),
        }
    }
}

impl RequestHeader<LogicalSourceName> for CreateLogicalSource {
    const DELTA: DeltaTag = DeltaTag::Create;
    const ENTITY: BaseEntityTag = BaseEntityTag::LogicalSource;

    type EntityData = LogicalSource;

    fn to_entity_data(&self) -> Self::EntityData {
        LogicalSource {
            name: self.source_name.clone(),
            schema: self.schema.clone(),
        }
    }

    fn key(&self) -> LogicalSourceName {
        self.source_name.clone()
    }
}

pub struct ShowLogicalSources {
    pub source_name: Option<LogicalSourceName>,
}

#[derive(Clone, Debug)]
pub struct DropLogicalSource {
    pub source_name: LogicalSourceName,
}

impl PartialEq for DropLogicalSource {
    fn eq(&self, other: &Self) -> bool {
        self.source_name == other.source_name
    }
}

impl Eq for DropLogicalSource {}

impl PartialOrd for DropLogicalSource {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DropLogicalSource {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.source_name.cmp(&other.source_name)
    }
}

impl Hash for DropLogicalSource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.source_name.hash(state);
    }
}

impl RequestHeader<LogicalSourceName> for DropLogicalSource {
    const DELTA: DeltaTag = DeltaTag::Drop;
    const ENTITY: BaseEntityTag = BaseEntityTag::LogicalSource;

    type EntityData = LogicalSource;

    fn to_entity_data(&self) -> Self::EntityData {
        // For Drop, we don't have the full entity data, just the key
        // Create a minimal entity with just the name (schema doesn't matter due to key-based equality)
        LogicalSource {
            name: self.source_name.clone(),
            schema: Schema::with(vec![("_placeholder".to_string(), crate::data_model::schema::DataType::BOOL)]),
        }
    }

    fn key(&self) -> LogicalSourceName {
        self.source_name.clone()
    }
}



