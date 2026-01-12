#[cfg(test)]
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use strum::EnumIter;

#[cfg_attr(test, derive(Arbitrary))]
#[derive(Copy, Debug, Clone, Serialize, Deserialize, EnumIter)]
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
#[cfg_attr(test, derive(Arbitrary))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    fields: Vec<AttributeField>,
}

impl Schema {
    pub fn from(fields: Vec<AttributeField>) -> Schema {
        assert!(
            !fields.is_empty(),
            "Cannot construct Schema with empty fields"
        );
        Self { fields }
    }
}
