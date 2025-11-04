use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use crate::data_model::logical_source::LogicalSourceName;
use crate::data_model::worker::HostName;
use crate::db_errors::ErrorTranslation;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
pub enum SourceType {
    File,
    Tcp,
}

pub type PhysicalSourceId = i64;

/// Physical source instantiation on a worker.
///
/// # Equality and Hashing
/// Implements key-based equality: two physical sources are equal if they have the same `id`,
/// regardless of configuration or placement.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct PhysicalSource {
    pub id: PhysicalSourceId,
    pub logical_source: LogicalSourceName,
    pub placement: HostName,
    pub source_type: SourceType,
    #[sqlx(json)]
    pub source_config: HashMap<String, String>,
    #[sqlx(json)]
    pub parser_config: HashMap<String, String>,
}

impl PartialEq for PhysicalSource {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for PhysicalSource {}

impl Hash for PhysicalSource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

pub struct CreatePhysicalSource {
    pub logical_source: LogicalSourceName,
    pub placement: HostName,
    pub source_type: SourceType,
    pub source_config: HashMap<String, String>,
    pub parser_config: HashMap<String, String>,
}

impl ErrorTranslation for CreatePhysicalSource {}

pub struct ShowPhysicalSources {
    pub for_logical_source: Option<LogicalSourceName>,
    pub on_node: Option<HostName>,
    pub by_type: Option<SourceType>,
}

pub struct DropPhysicalSource {
    pub physical_source_id: i64,
}

impl PartialEq for DropPhysicalSource {
    fn eq(&self, other: &Self) -> bool {
        self.physical_source_id == other.physical_source_id
    }
}

impl Eq for DropPhysicalSource {}

impl Hash for DropPhysicalSource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.physical_source_id.hash(state);
    }
}

