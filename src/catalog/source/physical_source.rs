use super::logical_source::LogicalSourceName;
use crate::catalog::query_builder::{SqlOperation, ToSql, WhereBuilder};
use crate::catalog::tables::{physical_sources, table};
use crate::catalog::worker::worker_endpoint::{HostName, NetworkAddr};
use crate::errors::CoordinatorErr;
use crate::request::Request;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteArguments;
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

#[derive(Clone, Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct PhysicalSource {
    pub id: PhysicalSourceId,
    pub logical_source: LogicalSourceName,
    pub placement_host_name: HostName,
    pub placement_grpc_port: u16,
    pub source_type: SourceType,
    #[sqlx(json)]
    pub source_config: HashMap<String, String>,
    #[sqlx(json)]
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
pub type CreatePhysicalSourceRequest = Request<CreatePhysicalSource, Result<(), CoordinatorErr>>;

#[derive(Clone, Debug)]
pub struct DropPhysicalSource {
    pub with_logical_source: Option<LogicalSourceName>,
    pub on_worker: Option<NetworkAddr>,
    pub with_type: Option<SourceType>,
}
pub type DropPhysicalSourceRequest =
    Request<DropPhysicalSource, Result<Vec<PhysicalSource>, CoordinatorErr>>;

impl ToSql for DropPhysicalSource {
    fn to_sql(&self) -> (String, SqliteArguments) {
        WhereBuilder::from(SqlOperation::Delete(table::PHYSICAL_SOURCES))
            .eq(
                physical_sources::LOGICAL_SOURCE,
                self.with_logical_source.clone(),
            )
            .eq(
                physical_sources::PLACEMENT_HOST_NAME,
                self.on_worker.clone().and_then(|worker| Some(worker.host)),
            )
            .eq(
                physical_sources::PLACEMENT_GRPC_PORT,
                self.on_worker.clone().and_then(|worker| Some(worker.port)),
            )
            .eq(physical_sources::SOURCE_TYPE, self.with_type)
            .into_parts()
    }
}
