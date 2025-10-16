use thiserror::Error;
use crate::db_errors::DatabaseError;

pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Error, Debug)]
pub enum CoordinatorError {
    // Database-related errors
    #[error("Error in internal database")]
    Database(#[from] DatabaseError),
}
