use crate::catalog::worker::HostName;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use strum::EnumIter;

#[derive(
    Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, sqlx::Type, EnumIter,
)]
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

impl std::str::FromStr for SinkType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "File" => Ok(SinkType::File),
            "Print" => Ok(SinkType::Print),
            _ => Err(format!("Unknown sink type: {}", s)),
        }
    }
}

pub type SinkName = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sink {
    pub name: SinkName,
    pub placement: String,
    pub sink_type: SinkType,
    pub config: HashMap<String, String>,
}

impl PartialEq for Sink {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Sink {}

#[derive(Debug, Clone)]
pub struct CreateSink {
    pub name: SinkName,
    pub placement: HostName,
    pub sink_type: SinkType,
    pub config: HashMap<String, String>,
}

pub struct GetSink {
    pub name: Option<SinkName>,
    pub on_node: Option<HostName>,
    pub by_type: Option<SinkType>,
}

pub struct DropSink {
    pub with_name: Option<SinkName>,
    pub on_node: Option<HostName>,
    pub with_type: Option<SinkType>,
}
