use crate::catalog::catalog_errors::CatalogError;
use crate::catalog::catalog_errors::CatalogError::EmptyPredicate;
use crate::catalog::logical_source::LogicalSourceName;
use crate::catalog::worker::{GrpcAddr, HostName};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use strum::EnumIter;

pub type PhysicalSourceId = i64;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, sqlx::Type, EnumIter)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PhysicalSource {
    pub id: PhysicalSourceId,
    pub logical_source: LogicalSourceName,
    pub placement_host_name: HostName,
    pub placement_grpc_port: u16,
    pub source_type: SourceType,
    pub source_config: HashMap<String, String>,
    pub parser_config: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct CreatePhysicalSource {
    pub logical_source: LogicalSourceName,
    pub placement_host_name: HostName,
    pub placement_grpc_port: u16,
    pub source_type: SourceType,
    pub source_config: HashMap<String, String>,
    pub parser_config: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct GetPhysicalSource {
    pub with_logical_source: Option<LogicalSourceName>,
    pub on_worker: Option<GrpcAddr>,
    pub with_type: Option<SourceType>,
}

#[derive(Clone, Debug)]
pub struct DropPhysicalSource {
    pub with_logical_source: Option<LogicalSourceName>,
    pub on_worker: Option<GrpcAddr>,
    pub with_type: Option<SourceType>,
}

impl DropPhysicalSource {
    pub fn new(
        with_logical_source: Option<LogicalSourceName>,
        on_worker: Option<GrpcAddr>,
        with_type: Option<SourceType>,
    ) -> Result<Self, CatalogError> {
        if with_logical_source.is_none() && on_worker.is_none() && with_type.is_none() {
            return Err(EmptyPredicate {});
        }
        Ok(Self {
            with_logical_source,
            on_worker,
            with_type,
        })
    }
}
