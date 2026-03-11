#![cfg(madsim)]

use crate::harness::TestHarness;
use async_trait::async_trait;
use tracing::info;

pub struct InvariantContext {
    created_query_ids: Vec<i64>,
    dropped_query_ids: Vec<i64>,
}

impl InvariantContext {
    pub fn new(created_query_ids: Vec<i64>, dropped_query_ids: Vec<i64>) -> Self {
        Self {
            created_query_ids,
            dropped_query_ids,
        }
    }

    pub fn query_ids(&self) -> &[i64] {
        &self.created_query_ids
    }

    pub fn dropped_query_ids(&self) -> &[i64] {
        &self.dropped_query_ids
    }

    pub fn empty() -> Self {
        Self::new(Vec::new(), Vec::new())
    }
}

#[async_trait(?Send)]
pub trait Invariant {
    fn name(&self) -> &str;
    async fn check(&self, harness: &TestHarness, ctx: &InvariantContext);
}

pub async fn check_invariants(
    invariants: &[&dyn Invariant],
    harness: &TestHarness,
    ctx: &InvariantContext,
) {
    for inv in invariants {
        inv.check(harness, ctx).await;
        info!("invariant {}: ok", inv.name());
    }
}
