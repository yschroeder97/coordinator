use crate::query::fragment::CreateFragment;
use crate::worker::CreateWorker;
use proptest::prelude::*;
use serde_json::json;

use super::worker::arb_dag_topology;

#[derive(Debug, Clone)]
pub struct ValidFragments {
    pub workers: Vec<CreateWorker>,
    fragment_specs: Vec<FragmentConfig>,
}

#[derive(Debug, Clone)]
struct FragmentConfig {
    worker_idx: usize,
    used_capacity: i32,
    has_source: bool,
}

impl ValidFragments {
    pub fn create_fragments(&self, query_id: i64) -> Vec<CreateFragment> {
        self.fragment_specs
            .iter()
            .map(|spec| {
                let worker = &self.workers[spec.worker_idx];
                CreateFragment {
                    query_id,
                    host_addr: worker.host_addr.clone(),
                    grpc_addr: worker.grpc_addr.clone(),
                    plan: json!({}),
                    used_capacity: spec.used_capacity,
                    has_source: spec.has_source,
                }
            })
            .collect()
    }
}

pub fn arb_fragment_setup(max_workers: usize) -> impl Strategy<Value = ValidFragments> {
    (
        arb_dag_topology(max_workers),
        prop::collection::vec((0..=16i32, any::<bool>()), 1..=20usize),
    )
        .prop_map(|(workers, fragment_params)| {
            let mut remaining_capacity: Vec<i32> = workers.iter().map(|w| w.capacity).collect();
            let num_workers = workers.len();

            let mut fragment_specs = Vec::new();
            for (used_capacity, has_source) in fragment_params {
                let placed = (0..num_workers).find(|offset| {
                    let idx = (fragment_specs.len() + offset) % num_workers;
                    if used_capacity <= remaining_capacity[idx] {
                        remaining_capacity[idx] -= used_capacity;
                        true
                    } else {
                        false
                    }
                });

                if let Some(offset) = placed {
                    let idx = (fragment_specs.len() + offset) % num_workers;
                    fragment_specs.push(FragmentConfig {
                        worker_idx: idx,
                        used_capacity,
                        has_source,
                    });
                }
            }

            if fragment_specs.is_empty() {
                fragment_specs.push(FragmentConfig {
                    worker_idx: 0,
                    used_capacity: 0,
                    has_source: false,
                });
            }

            ValidFragments {
                workers,
                fragment_specs,
            }
        })
}

pub fn arb_create_fragments(
    query: &crate::query::Model,
    workers: &[crate::worker::Model],
) -> Vec<CreateFragment> {
    use crate::worker::WorkerState;

    let fragments: Vec<_> = workers
        .iter()
        .filter(|w| w.current_state == WorkerState::Active && w.capacity > 0)
        .map(|w| CreateFragment {
            query_id: query.id,
            host_addr: w.host_addr.clone(),
            grpc_addr: w.grpc_addr.clone(),
            plan: serde_json::json!({}),
            used_capacity: 1,
            has_source: false,
        })
        .collect();

    if fragments.is_empty() {
        workers
            .first()
            .map(|w| {
                vec![CreateFragment {
                    query_id: query.id,
                    host_addr: w.host_addr.clone(),
                    grpc_addr: w.grpc_addr.clone(),
                    plan: serde_json::json!({}),
                    used_capacity: 0,
                    has_source: false,
                }]
            })
            .unwrap_or_default()
    } else {
        fragments
    }
}
