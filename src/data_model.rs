use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Type};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryState {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::Type)]
pub enum SourceType {
    File,
    Tcp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::Type)]
pub enum SinkType {
    File,
    Print,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
pub enum DataType {
    UINT8,
    INT8,
    UINT16,
    INT16,
    UINT32,
    INT32,
    UINT64,
    INT64,
    FLOAT32,
    FLOAT64,
    BOOL,
    CHAR,
    VARSIZED,
}

pub type FieldName = String;
pub type AttributeField = (FieldName, DataType);
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
pub struct Schema {
    fields: Vec<AttributeField>,
}

impl Schema {
    pub fn with(fields: Vec<AttributeField>) -> Schema {
        assert!(
            !fields.is_empty(),
            "Cannot construct Schema with empty fields"
        );
        Self { fields }
    }
}

pub type HostName = String;
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Worker {
    pub host_name: HostName,
    pub grpc_port: u16,
    pub data_port: u16,
    pub num_slots: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct NetworkLink {
    pub source_host: HostName,
    pub target_host: HostName,
}

pub type LogicalSourceName = String;
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct LogicalSource {
    pub name: LogicalSourceName,
    #[sqlx(json)]
    pub schema: Schema,
}

pub type PhysicalSourceId = i64;
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

pub type SinkName = String;
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Sink {
    pub name: SinkName,
    pub placement: String,
    pub sink_type: SinkType,
    #[sqlx(json)]
    pub config: HashMap<String, String>,
}

pub type QueryId = String;
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Query {
    pub id: QueryId,
    pub statement: String,
    pub state: QueryState,
    pub sink: SinkName,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct QueryFragment {
    pub query_id: QueryId,
    pub worker_id: HostName,
    pub state: QueryState,
}
