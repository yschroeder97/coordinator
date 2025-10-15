use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Formatter;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryState {
    Pending,
    Running,
    Completed,
    Failed,
}

impl std::fmt::Display for QueryState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryState::Pending => write!(f, "Pending"),
            QueryState::Running => write!(f, "Running"),
            QueryState::Completed => write!(f, "Completed"),
            QueryState::Failed => write!(f, "Failed"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SourceType {
    File,
    Tcp,
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceType::File => write!(f, "File"),
            SourceType::Tcp => write!(f, "Tcp"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SinkType {
    File,
    Print,
}

impl std::fmt::Display for SinkType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SinkType::File => write!(f, "File"),
            SinkType::Print => write!(f, "Print"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::UINT8 => write!(f, "UINT8"),
            DataType::INT8 => write!(f, "INT8"),
            DataType::UINT16 => write!(f, "UINT16"),
            DataType::INT16 => write!(f, "INT16"),
            DataType::UINT32 => write!(f, "UINT32"),
            DataType::INT32 => write!(f, "INT32"),
            DataType::UINT64 => write!(f, "UINT64"),
            DataType::INT64 => write!(f, "INT64"),
            DataType::FLOAT32 => write!(f, "FLOAT32"),
            DataType::FLOAT64 => write!(f, "FLOAT64"),
            DataType::BOOL => write!(f, "BOOL"),
            DataType::CHAR => write!(f, "CHAR"),
            DataType::VARSIZED => write!(f, "VARSIZED"),
        }
    }
}

pub type FieldName = String;
pub type AttributeField = (FieldName, DataType);
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    fields: Vec<AttributeField>,
}

impl Schema {
    pub fn new(fields: Vec<AttributeField>) -> Schema {
        assert!(
            !fields.is_empty(),
            "Cannot construct Schema with empty fields"
        );
        Self { fields }
    }
}

pub type HostName = String;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Worker {
    pub host_name: HostName,
    pub grpc_port: u16,
    pub data_port: u16,
    pub num_slots: Option<u32>,
}

pub type LogicalSourceName = String;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalSource {
    pub name: LogicalSourceName,
    pub schema: Schema,
}

pub type PhysicalSourceId = i64;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhysicalSource {
    pub id: PhysicalSourceId,
    pub logical_source: LogicalSourceName,
    pub placement: HostName,
    pub source_type: SourceType,
    pub source_config: HashMap<String, String>,
    pub parser_config: HashMap<String, String>,
}

pub type SinkName = String;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sink {
    pub name: SinkName,
    pub placement: String,
    pub sink_type: SinkType,
    pub config: HashMap<String, String>,
}

pub type QueryId = String;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    pub id: QueryId,
    pub statement: String,
    pub state: QueryState,
    pub sink: SinkName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryFragment {
    pub query_id: QueryId,
    pub worker_id: HostName,
    pub state: QueryState,
}
