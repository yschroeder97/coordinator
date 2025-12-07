use crate::catalog::catalog_errors::CatalogError;
use crate::network_service::NetworkServiceError;
use thiserror::Error;

pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Error, Debug)]
pub enum CoordinatorError {
    // Database-related errors
    #[error("Error in internal database")]
    Database(#[from] CatalogError),
    // Cluster-related errors
    #[error("Error in cluster service")]
    ClusterService(#[from] NetworkServiceError),
}
