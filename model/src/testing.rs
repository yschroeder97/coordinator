//! Test utilities and proptest strategies for model types.
//!
//! This module is only available when the `testing` feature is enabled.

use crate::query::fragment::CreateFragment;
use crate::query::query_state::QueryState;
use crate::query::{CreateQuery, DropQuery, GetQuery, QueryId, StopMode};
use crate::sink::{CreateSink, SinkType};
use crate::source::logical_source::CreateLogicalSource;
use crate::source::physical_source::{CreatePhysicalSource, SourceType};
use crate::source::schema::{DataType, Schema};
use crate::worker::CreateWorker;
use crate::worker::endpoint::{HostAddr, NetworkAddr};
use proptest::prelude::*;
use serde_json::json;

/// A physical source with its required dependencies (logical source and worker).
#[derive(Debug, Clone)]
pub struct PhysicalSourceWithRefs {
    pub logical: CreateLogicalSource,
    pub worker: CreateWorker,
    pub physical: CreatePhysicalSource,
}

/// A sink with its required dependency (worker).
#[derive(Debug, Clone)]
pub struct SinkWithRefs {
    pub worker: CreateWorker,
    pub sink: CreateSink,
}

// Strategy for generating valid host addresses
fn arb_host_addr() -> impl Strategy<Value = HostAddr> {
    (
        proptest::string::string_regex("[a-z][a-z0-9]{2,14}").unwrap(),
        1024..65535u16,
    )
        .prop_map(|(host, port)| NetworkAddr::new(host, port))
}

// Strategy for generating schemas
fn arb_schema() -> impl Strategy<Value = Schema> {
    prop::collection::vec(
        (
            proptest::string::string_regex("[a-z][a-z0-9_]{0,19}").unwrap(),
            prop_oneof![
                Just(DataType::INT32),
                Just(DataType::INT64),
                Just(DataType::FLOAT32),
                Just(DataType::FLOAT64),
                Just(DataType::BOOL),
                Just(DataType::CHAR),
            ],
        ),
        1..10,
    )
    .prop_map(|fields| Schema::from(fields))
}

prop_compose! {
    /// Strategy for generating CreateWorker requests.
    pub fn arb_create_worker()(
        host_addr in arb_host_addr(),
        grpc_port in 1024..65535u16,
        capacity in 0..1024i32,
    ) -> CreateWorker {
        // Ensure grpc_addr differs from host_addr (required by worker model)
        let grpc_port = if grpc_port == host_addr.port {
            if grpc_port < 65534 { grpc_port + 1 } else { grpc_port - 1 }
        } else {
            grpc_port
        };
        let grpc_addr = NetworkAddr::new(host_addr.host.clone(), grpc_port);
        CreateWorker::new(host_addr, grpc_addr, capacity)
    }
}

prop_compose! {
    /// Strategy for generating CreateLogicalSource requests.
    pub fn arb_create_logical_source()(
        name in proptest::string::string_regex("[a-z][a-z0-9_]{2,29}").unwrap(),
        schema in arb_schema(),
    ) -> CreateLogicalSource {
        CreateLogicalSource { name, schema }
    }
}

prop_compose! {
    /// Strategy for generating a PhysicalSourceWithRefs (physical source with valid dependencies).
    pub fn arb_physical_with_refs()(
        logical in arb_create_logical_source(),
        worker in arb_create_worker(),
        source_type in any::<SourceType>(),
    ) -> PhysicalSourceWithRefs {
        let physical = CreatePhysicalSource {
            logical_source: logical.name.clone(),
            host_addr: worker.host_addr.clone(),
            source_type,
            source_config: json!({}),
            parser_config: json!({}),
        };
        PhysicalSourceWithRefs { logical, worker, physical }
    }
}

prop_compose! {
    /// Strategy for generating a SinkWithRefs (sink with valid worker dependency).
    pub fn arb_sink_with_refs()(
        name in proptest::string::string_regex("[a-z][a-z0-9_]{2,29}").unwrap(),
        worker in arb_create_worker(),
        sink_type in any::<SinkType>(),
    ) -> SinkWithRefs {
        let sink = CreateSink {
            name,
            host_addr: worker.host_addr.clone(),
            sink_type,
            config: json!({}),
        };
        SinkWithRefs { worker, sink }
    }
}

/// Setup for fragment catalog tests.
///
/// Generates workers and fragment specifications that respect capacity constraints.
/// Does not include a query — tests create the query first, then call
/// [`FragmentSetup::create_fragments`] with the real auto-generated `query_id`,
/// mirroring the real workflow where the planner creates fragments only for an
/// already-persisted query.
///
/// Invariants:
/// - Each fragment spec references a valid worker (by index).
/// - Per-worker total `used_capacity` does not exceed that worker's capacity.
/// - At least one fragment spec is present.
#[derive(Debug, Clone)]
pub struct FragmentSetup {
    pub workers: Vec<CreateWorker>,
    fragment_specs: Vec<FragmentSpec>,
}

#[derive(Debug, Clone)]
struct FragmentSpec {
    worker_idx: usize,
    used_capacity: i32,
    has_source: bool,
}

impl FragmentSetup {
    /// Build [`CreateFragment`] values using the real `query_id` from the DB.
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

/// Strategy for generating a valid [`FragmentSetup`].
///
/// Generates 1..=`max_workers` unique workers and 1..=20 fragment specs,
/// distributing fragments round-robin across workers while respecting capacity
/// constraints. Each fragment gets `used_capacity` in `0..=1` and `has_source`
/// randomly.
pub fn arb_fragment_setup(max_workers: usize) -> impl Strategy<Value = FragmentSetup> {
    (
        arb_unique_workers(max_workers),
        prop::collection::vec((0..=1i32, any::<bool>()), 1..=20usize),
    )
        .prop_map(|(workers, fragment_params)| {
            let mut remaining_capacity: Vec<i32> = workers.iter().map(|w| w.capacity).collect();
            let num_workers = workers.len();

            let mut fragment_specs = Vec::new();
            for (used_capacity, has_source) in fragment_params {
                // Round-robin: try each worker starting from the next one
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
                    fragment_specs.push(FragmentSpec {
                        worker_idx: idx,
                        used_capacity,
                        has_source,
                    });
                }
                // Skip fragments that don't fit on any worker
            }

            // Guarantee at least one fragment: if all were skipped (all workers
            // have 0 capacity and all drawn used_capacity were 1), add one with
            // used_capacity = 0 on the first worker.
            if fragment_specs.is_empty() {
                fragment_specs.push(FragmentSpec {
                    worker_idx: 0,
                    used_capacity: 0,
                    has_source: false,
                });
            }

            FragmentSetup {
                workers,
                fragment_specs,
            }
        })
}

/// Strategy for generating a vector of workers with unique host_addrs.
pub fn arb_unique_workers(max_workers: usize) -> impl Strategy<Value = Vec<CreateWorker>> {
    prop::collection::vec(arb_create_worker(), 1..=max_workers).prop_map(|workers| {
        let mut seen = std::collections::HashSet::new();
        workers
            .into_iter()
            .filter(|w| seen.insert(w.host_addr.clone()))
            .collect()
    })
}

prop_compose! {
    /// Strategy for generating CreateQuery requests.
    pub fn arb_create_query()(
        name in proptest::string::string_regex("[a-z][a-z0-9_-]{2,29}").unwrap(),
        statement in proptest::string::string_regex("SELECT [a-z]+ FROM [a-z]+").unwrap(),
        block_until in any::<QueryState>(),
    ) -> CreateQuery {
        CreateQuery::new(statement).name(name).block_until(block_until)
    }
}

prop_compose! {
    pub fn arb_drop_query(query_id: QueryId)(
        stop_mode in any::<StopMode>(),
        should_block in any::<bool>(),
    ) -> DropQuery {
        DropQuery::new()
            .stop_mode(stop_mode)
            .should_block(should_block)
            .with_filters(GetQuery::new().with_id(query_id))
    }
}

/// Strategy that generates one of the 9 valid state paths from Pending to a terminal state.
///
/// Each path is a `Vec<QueryState>` representing the full sequence of states,
/// starting with `Pending` and ending with a terminal state (`Completed`, `Stopped`, or `Failed`).
pub fn arb_valid_state_path() -> impl Strategy<Value = Vec<QueryState>> {
    use QueryState::*;
    prop_oneof![
        // Full happy path
        Just(vec![Pending, Planned, Registered, Running, Completed]),
        Just(vec![Pending, Planned, Registered, Running, Stopped]),
        Just(vec![Pending, Planned, Registered, Running, Failed]),
        // Terminate at Registered
        Just(vec![Pending, Planned, Registered, Stopped]),
        Just(vec![Pending, Planned, Registered, Failed]),
        // Terminate at Planned
        Just(vec![Pending, Planned, Stopped]),
        Just(vec![Pending, Planned, Failed]),
        // Terminate at Pending
        Just(vec![Pending, Stopped]),
        Just(vec![Pending, Failed]),
    ]
}
