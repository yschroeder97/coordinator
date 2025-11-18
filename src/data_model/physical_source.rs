use crate::data_model::catalog_errors::CatalogError;
use crate::data_model::catalog_errors::CatalogError::EmptyPredicate;
use crate::data_model::logical_source::LogicalSourceName;
use crate::data_model::worker::HostName;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use strum::EnumIter;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, sqlx::Type, EnumIter, Eq, PartialEq, Hash)]
#[sqlx(type_name = "TEXT")]
pub enum SourceType {
    File,
    Tcp,
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceType::File => write!(f, "File"),
            SourceType::Tcp => write!(f, "Tcp"),
        }
    }
}

impl std::str::FromStr for SourceType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "File" => Ok(SourceType::File),
            "Tcp" => Ok(SourceType::Tcp),
            _ => Err(format!("Unknown source type: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhysicalSource {
    pub logical_source: LogicalSourceName,
    pub placement: HostName,
    pub source_type: SourceType,
    pub source_config: HashMap<String, String>,
    pub parser_config: HashMap<String, String>,
}

// Composite key for identifying physical sources
#[derive(Debug, Clone)]
pub struct PhysicalSourceKey {
    pub logical_source: LogicalSourceName,
    pub placement: HostName,
    pub source_type: SourceType,
}

#[derive(Clone, Debug)]
pub struct CreatePhysicalSource {
    pub logical_source: LogicalSourceName,
    pub placement: HostName,
    pub source_type: SourceType,
    pub source_config: HashMap<String, String>,
    pub parser_config: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct GetPhysicalSource {
    pub for_logical_source: Option<LogicalSourceName>,
    pub on_node: Option<HostName>,
    pub by_type: Option<SourceType>,
}

#[derive(Clone, Debug)]
pub struct DropPhysicalSource {
    pub with_logical_source: Option<LogicalSourceName>,
    pub on_node: Option<HostName>,
    pub by_type: Option<SourceType>,
}

impl DropPhysicalSource {
    pub fn new(
        with_logical_source: Option<LogicalSourceName>,
        on_node: Option<HostName>,
        by_type: Option<SourceType>,
    ) -> Result<Self, CatalogError> {
        if with_logical_source.is_none() && on_node.is_none() && by_type.is_none() {
            return Err(EmptyPredicate {});
        }
        Ok(Self {
            with_logical_source,
            on_node,
            by_type,
        })
    }
}
