mod context;
mod lifecycle;
mod reconciler;
pub(crate) mod retry;
pub mod service;

pub type QueryId = i64;

pub struct Completed;
