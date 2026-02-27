pub mod query;
pub mod sink;
pub mod source;
pub mod worker;

#[cfg(feature = "testing")]
pub mod testing;

pub use sea_orm::Set;
use sea_orm::Condition;

/// Trait for types that can be converted into a SeaORM Condition for filtering queries.
pub trait IntoCondition {
    fn into_condition(self) -> Condition;
}

