mod context;
mod lifecycle;
mod reconciler;
pub(crate) mod retry;
pub mod service;

pub use retry::{MAX_RPC_ATTEMPTS, RPC_RETRY_BASE_MS};

pub(crate) type QueryId = i64;

pub(crate) struct Completed;
