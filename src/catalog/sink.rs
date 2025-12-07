use crate::catalog::worker::{GrpcAddr, HostName};
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sink {
    pub name: SinkName,
    pub placement_host_name: HostName,
    pub placement_grpc_port: u16,
    pub sink_type: SinkType,
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

pub struct GetSink {
    pub name: Option<SinkName>,
    pub on_worker: Option<GrpcAddr>,
    pub by_type: Option<SinkType>,
}

pub struct DropSink {
    pub with_name: Option<SinkName>,
    pub on_worker: Option<GrpcAddr>,
    pub with_type: Option<SinkType>,
}
