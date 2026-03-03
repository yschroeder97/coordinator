use crate::worker::arb_create_worker;
use crate::worker::CreateWorker;
use proptest::prelude::*;

pub fn arb_unique_workers(max_workers: usize) -> impl Strategy<Value = Vec<CreateWorker>> {
    prop::collection::vec(arb_create_worker(), 1..=max_workers).prop_map(|workers| {
        let mut seen = std::collections::HashSet::new();
        workers
            .into_iter()
            .filter(|w| seen.insert(w.host_addr.clone()))
            .collect()
    })
}
