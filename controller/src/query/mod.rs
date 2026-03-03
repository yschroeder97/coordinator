mod context;
mod lifecycle;
mod reconciler;
pub(crate) mod retry;
pub mod service;

pub(crate) type QueryId = i64;

pub(crate) struct Completed;
