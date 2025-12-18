use crate::catalog::catalog_errors::CatalogErr;
use thiserror::Error;

pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Error, Debug)]
pub enum CoordinatorErr {
    // Catalog-related errors
    #[error("Error in catalog")]
    Catalog(#[from] CatalogErr),
    // Cluster-related errors
    // #[error("Error in cluster service")]
    // ClusterService(#[from] ClusterServiceErr),
}
