pub mod catalog;
pub mod coordinator;
mod message_bus;
mod cluster_controller;
pub mod request;
pub mod error;
mod query_controller;

/// Re-exports of internal types for testing purposes.
/// Only available when the `arbitrary` feature is enabled.
#[cfg(feature = "arbitrary")]
pub mod test_exports {
    pub use crate::catalog::query::fragment::{CreateQueryFragment, FragmentState};
    pub use crate::catalog::query::query_state::{QueryState, QueryStateTransitionType};
}
