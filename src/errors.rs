use thiserror::Error;
use crate::cluster::ClusterServiceError;
use crate::data_model::catalog_errors::CatalogError;

pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Error, Debug)]
pub enum CoordinatorError {
    // Database-related errors
    #[error("Error in internal database")]
    Database(#[from] CatalogError),
    // Cluster-related errors
    #[error("Error in cluster service")]
    ClusterService(#[from] ClusterServiceError),
}
