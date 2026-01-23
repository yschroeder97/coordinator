//! Test utilities for coordinator crate.
//! Provides proptest strategies for generating valid test inputs.

use coordinator::catalog::query::CreateQuery;
use coordinator::test_exports::{CreateQueryFragment, FragmentState, QueryState, QueryStateTransitionType};
use coordinator::catalog::sink::{CreateSink, SinkType};
use coordinator::catalog::source::logical_source::CreateLogicalSource;
use coordinator::catalog::source::physical_source::{CreatePhysicalSource, SourceType};
use coordinator::catalog::worker::endpoint::{NetworkAddr, DEFAULT_DATA_PORT, DEFAULT_GRPC_PORT};
use coordinator::catalog::worker::{CreateWorker, Worker};
use proptest::arbitrary::any;
use proptest::prelude::{prop, Rng};
use proptest::prop_compose;
use proptest::strategy::{Just, Strategy};
use serde_json::json;

// Re-export proptest for convenience
pub use proptest;

prop_compose! {
    pub fn arb_create_worker()(
        host_name in r"[a-z0-9]{3,15}",
        grpc_port in 1..65534u16,
        capacity in any::<u32>(),
        peers in prop::collection::vec(any::<NetworkAddr>(), 0..10)
    ) -> CreateWorker {
        CreateWorker {
            host_name,
            grpc_port,
            data_port: grpc_port + 1,
            capacity,
            peers,
        }
    }
}

pub fn arb_worker_custom(
    host: String,
    grpc: u16,
    data: u16,
) -> impl Strategy<Value = CreateWorker> {
    (1..100u32).prop_map(move |capacity| CreateWorker {
        host_name: host.clone(),
        grpc_port: grpc,
        data_port: data,
        capacity,
        peers: vec![],
    })
}

pub fn arb_worker_cluster_on_subnet(count: u8) -> impl Strategy<Value = Vec<CreateWorker>> {
    prop::collection::hash_set(1..255u8, count as usize).prop_flat_map(move |octets| {
        octets
            .into_iter()
            .map(|octet| {
                let ip = format!("192.168.1.{}", octet);
                arb_worker_custom(ip, DEFAULT_GRPC_PORT, DEFAULT_DATA_PORT)
            })
            .collect::<Vec<_>>()
    })
}

#[derive(Debug, Clone)]
pub struct PhysicalSourceWithRefs {
    pub logical: CreateLogicalSource,
    pub worker: CreateWorker,
    pub physical: CreatePhysicalSource,
}

prop_compose! {
    pub fn arb_physical_with_refs()(
        logical in any::<CreateLogicalSource>(),
        worker in arb_create_worker(),
        source_type in any::<SourceType>(),
    ) -> PhysicalSourceWithRefs {
        let physical = CreatePhysicalSource {
            logical_source: logical.source_name.clone(),
            placement_host_name: worker.host_name.clone(),
            placement_grpc_port: worker.grpc_port,
            source_type,
            source_config: Default::default(),
            parser_config: Default::default(),
        };
        PhysicalSourceWithRefs { logical, worker, physical }
    }
}

#[derive(Debug, Clone)]
pub struct SinkWithRefs {
    pub worker: CreateWorker,
    pub sink: CreateSink,
}

prop_compose! {
    pub fn arb_create_sink()(
        name in "[a-z]",
        worker in arb_create_worker(),
        sink_type in any::<SinkType>(),
    ) -> SinkWithRefs {
        let sink = CreateSink {
            name,
            placement_host_name: worker.host_name.clone(),
            placement_grpc_port: worker.grpc_port,
            sink_type,
            config: Default::default(),
        };
        SinkWithRefs { worker, sink }
    }
}

prop_compose! {
    pub fn arb_create_query()(
        name in "[a-z]{1,20}",
        stmt in "[a-z0-9]{0,50}",
    ) -> CreateQuery {
        CreateQuery {
            name,
            stmt,
        }
    }
}

prop_compose! {
    pub fn arb_fragments(workers: Vec<Worker>)(query in arb_create_query()) -> Vec<CreateQueryFragment> {
        workers.iter().map(|worker| {
            CreateQueryFragment {
                query_id: query.name.clone(),
                host_name: worker.host_name.clone(),
                grpc_port: worker.grpc_port,
                current_state: FragmentState::Pending,
                desired_state: FragmentState::Running,
                plan: json!("{}"),
            }
        }).collect()
    }
}

/// A valid query request with all its dependencies
#[derive(Debug, Clone)]
pub struct QueryWithRefs {
    pub workers: Vec<CreateWorker>,
    pub fragments: Vec<CreateQueryFragment>,
    pub query: CreateQuery,
}

pub fn arb_query_with_deps(
    max_num_workers: u8,
    max_num_fragments: usize,
) -> impl Strategy<Value = QueryWithRefs> {
    arb_worker_cluster_on_subnet(max_num_workers)
        .prop_flat_map(move |workers| {
            let num_workers = workers.len();
            (
                Just(workers),
                "[a-z]{1,20}",
                "[a-z0-9]{0,50}",
                prop::collection::vec(0..num_workers, 1..max_num_fragments + 1),
            )
        })
        .prop_map(|(workers, name, stmt, worker_indices)| {
            let query = CreateQuery {
                name: name.clone(),
                stmt,
            };

            let fragments = worker_indices
                .into_iter()
                .map(|idx| {
                    let worker = &workers[idx];
                    CreateQueryFragment {
                        query_id: name.clone(),
                        host_name: worker.host_name.clone(),
                        grpc_port: worker.grpc_port,
                        current_state: FragmentState::Pending,
                        desired_state: FragmentState::Running,
                        plan: json!("{}"),
                    }
                })
                .collect();

            QueryWithRefs {
                workers,
                query,
                fragments,
            }
        })
}

/// Strategy to produce a sequence of query states
pub fn arb_state_sequence(
    transition: QueryStateTransitionType,
) -> impl Strategy<Value = Vec<QueryState>> {
    Just(vec![QueryState::Pending]).prop_perturb(move |mut states, mut rng| {
        loop {
            let current = *states.last().unwrap();
            let valid_nexts = current.transitions();

            match transition {
                QueryStateTransitionType::Invalid => {
                    if valid_nexts.is_empty() || rng.random_bool(0.2) {
                        let invalid_nexts = current.invalid_transitions();
                        let idx = rng.random_range(0..invalid_nexts.len());
                        states.push(invalid_nexts[idx]);
                        return states;
                    }
                }
                QueryStateTransitionType::Valid => {
                    if valid_nexts.is_empty() {
                        return states;
                    }
                }
            }

            let idx = rng.random_range(0..valid_nexts.len());
            states.push(valid_nexts[idx]);
        }
    })
}
