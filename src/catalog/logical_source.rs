use std::hash::{Hash, Hasher};
use crate::catalog::schema::Schema;
use serde::{Deserialize, Serialize};

pub type LogicalSourceName = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalSource {
    pub name: LogicalSourceName,
    pub schema: Schema,
}

impl Hash for LogicalSource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state)
    }
}

impl PartialEq for LogicalSource {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for LogicalSource {}

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
