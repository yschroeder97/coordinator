use crate::catalog::schema::Schema;
use serde::{Deserialize, Serialize};

pub type LogicalSourceName = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalSource {
    pub name: LogicalSourceName,
    pub schema: Schema,
}

#[derive(Clone, Debug)]
pub struct CreateLogicalSource {
    pub source_name: LogicalSourceName,
    pub schema: Schema,
}

#[derive(Clone)]
pub struct GetLogicalSource {
    pub source_name: Option<LogicalSourceName>,
}

#[derive(Clone, Debug)]
pub struct DropLogicalSource {
    pub source_name: LogicalSourceName,
}
