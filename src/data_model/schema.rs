use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
#[derive(PartialEq, PartialOrd, Eq, Ord)]
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
#[derive(PartialEq, PartialOrd, Eq, Ord)]
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

