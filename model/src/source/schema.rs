#[cfg(feature = "testing")]
use proptest_derive::Arbitrary;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};
use strum::EnumIter;

#[cfg_attr(feature = "testing", derive(Arbitrary))]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, EnumIter)]
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

#[cfg_attr(feature = "testing", derive(Arbitrary))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct Schema {
    #[cfg_attr(
        feature = "testing",
        proptest(strategy = "proptest::collection::vec(\
            (proptest::string::string_regex(\"[a-z][a-z0-9_]{0,19}\").unwrap(), proptest::prelude::any::<DataType>()),\
            1..10)")
    )]
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
