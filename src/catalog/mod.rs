pub mod catalog_errors;
pub mod catalog_base;
pub mod source;
pub mod query;
pub mod sink;
pub mod worker;
pub mod notification;
pub mod tables;
mod database;
mod query_builder;

// Re-export specialized catalogs for easy access
pub use source::source_catalog::SourceCatalog;
pub use sink::sink_catalog::SinkCatalog;
pub use worker::worker_catalog::WorkerCatalog;
pub use query::query_catalog::QueryCatalog;
