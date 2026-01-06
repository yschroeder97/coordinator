pub mod sink_catalog;

use crate::catalog::query_builder::{SqlOperation, ToSql, WhereBuilder};
use crate::catalog::tables::{sinks, table};
use crate::catalog::worker::endpoint::{HostName, NetworkAddr};
use crate::errors::CoordinatorErr;
use crate::request::Request;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteArguments;
use std::collections::HashMap;
use strum::EnumIter;

#[derive(Debug, Copy, Clone, Serialize, Deserialize, sqlx::Type, EnumIter)]
#[sqlx(type_name = "TEXT")]
pub enum SinkType {
    File,
    Print,
}

impl std::fmt::Display for SinkType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SinkType::File => write!(f, "File"),
            SinkType::Print => write!(f, "Print"),
        }
    }
}

pub type SinkName = String;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Sink {
    pub name: SinkName,
    pub placement_host_name: HostName,
    pub placement_grpc_port: u16,
    pub sink_type: SinkType,
    #[sqlx(json)]
    pub config: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct CreateSink {
    pub name: SinkName,
    pub placement_host_name: HostName,
    pub placement_grpc_port: u16,
    pub sink_type: SinkType,
    pub config: HashMap<String, String>,
}
pub type CreateSinkRequest = Request<CreateSink, Result<(), CoordinatorErr>>;

#[derive(Debug, Clone)]
pub struct DropSink {
    pub with_name: Option<SinkName>,
    pub on_worker: Option<NetworkAddr>,
    pub with_type: Option<SinkType>,
}
pub type DropSinkRequest = Request<DropSink, Result<Vec<Sink>, CoordinatorErr>>;

impl ToSql for DropSink {
    fn to_sql(&self) -> (String, SqliteArguments<'_>) {
        WhereBuilder::from(SqlOperation::Delete(table::PHYSICAL_SOURCES))
            .eq(sinks::NAME, self.with_name.clone())
            .eq(
                sinks::PLACEMENT_HOST_NAME,
                self.on_worker.clone().map(|worker| worker.host),
            )
            .eq(
                sinks::PLACEMENT_GRPC_PORT,
                self.on_worker.clone().map(|worker| worker.port),
            )
            .eq(sinks::SINK_TYPE, self.with_type)
            .into_parts()
    }
}

#[derive(Debug, Clone)]
pub struct GetSink {
    pub with_name: Option<SinkName>,
    pub on_worker: Option<NetworkAddr>,
    pub with_type: Option<SinkType>,
}
pub type GetSinkRequest = Request<GetSink, Result<Vec<Sink>, CoordinatorErr>>;

impl ToSql for GetSink {
    fn to_sql(&self) -> (String, SqliteArguments<'_>) {
        WhereBuilder::from(SqlOperation::Select(table::SINKS))
            .eq(sinks::NAME, self.with_name.clone())
            .eq(
                sinks::PLACEMENT_HOST_NAME,
                self.on_worker.clone().map(|worker| worker.host),
            )
            .eq(
                sinks::PLACEMENT_GRPC_PORT,
                self.on_worker.clone().map(|worker| worker.port),
            )
            .eq(sinks::SINK_TYPE, self.with_type)
            .into_parts()
    }
}
