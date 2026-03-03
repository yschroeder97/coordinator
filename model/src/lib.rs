pub mod query;
pub mod sink;
pub mod source;
pub mod worker;

#[cfg(feature = "testing")]
pub mod testing;

pub use sea_orm::Set;
use sea_orm::Condition;

pub trait IntoCondition {
    fn into_condition(self) -> Condition;
}

