//! Test utilities and proptest strategies for model types.
//!
//! This module is only available when the `testing` feature is enabled.

use crate::query::fragment::CreateFragment;
use crate::query::query_state::QueryState;
use crate::query::CreateQuery;
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
    /// Strategy for generating CreateWorker with peers.
    pub fn arb_create_worker_with_peers(max_peers: usize)(
        host_addr in arb_host_addr(),
        grpc_port in 1024..65535u16,
        capacity in 1..100i32,
        peers in prop::collection::vec(arb_host_addr(), 0..max_peers),
    ) -> CreateWorker {
        let grpc_addr = NetworkAddr::new(host_addr.host.clone(), grpc_port);
        CreateWorker::new(host_addr, grpc_addr, capacity).with_peers(peers)
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
    /// Strategy for generating CreateSink requests.
    pub fn arb_create_sink_standalone()(
        name in proptest::string::string_regex("[a-z][a-z0-9_]{2,29}").unwrap(),
        host_addr in arb_host_addr(),
        sink_type in any::<SinkType>(),
    ) -> CreateSink {
        CreateSink {
            name,
            host_addr,
            sink_type,
            config: json!({}),
        }
    }
}

/// A query with its planned fragments and the workers they're placed on.
///
/// Invariants:
/// - Each fragment references a valid worker (by host_addr and grpc_addr).
/// - Per-worker total `used_capacity` does not exceed that worker's capacity.
/// - The query id on every fragment matches `query.id`.
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub query: CreateQuery,
    pub workers: Vec<CreateWorker>,
    pub fragments: Vec<CreateFragment>,
}

/// Strategy for generating a valid `QueryPlan`.
///
/// Generates 1..=`max_workers` unique workers and 1..=20 fragments, distributing
/// fragments round-robin across workers while respecting capacity constraints.
/// Each fragment gets `used_capacity` in `0..=1` and `has_source` randomly.
pub fn arb_query_plan(max_workers: usize) -> impl Strategy<Value = QueryPlan> {
    (
        arb_create_query(),
        arb_unique_workers(max_workers),
        prop::collection::vec((0..=1i32, any::<bool>()), 1..=20usize),
    )
        .prop_map(|(query, workers, fragment_params)| {
            let mut remaining_capacity: Vec<i32> =
                workers.iter().map(|w| w.capacity).collect();
            let num_workers = workers.len();

            let mut fragments = Vec::new();
            for (used_capacity, has_source) in fragment_params {
                // Round-robin: try each worker starting from the next one
                let placed = (0..num_workers).find(|offset| {
                    let idx = (fragments.len() + offset) % num_workers;
                    if used_capacity <= remaining_capacity[idx] {
                        remaining_capacity[idx] -= used_capacity;
                        true
                    } else {
                        false
                    }
                });

                if let Some(offset) = placed {
                    let idx = (fragments.len() + offset) % num_workers;
                    let worker = &workers[idx];
                    fragments.push(CreateFragment {
                        query_id: query.id.clone(),
                        host_addr: worker.host_addr.clone(),
                        grpc_addr: worker.grpc_addr.clone(),
                        plan: json!({}),
                        used_capacity,
                        has_source,
                    });
                }
                // Skip fragments that don't fit on any worker
            }

            // Guarantee at least one fragment: if all were skipped (all workers
            // have 0 capacity and all drawn used_capacity were 1), add one with
            // used_capacity = 0 on the first worker.
            if fragments.is_empty() {
                let worker = &workers[0];
                fragments.push(CreateFragment {
                    query_id: query.id.clone(),
                    host_addr: worker.host_addr.clone(),
                    grpc_addr: worker.grpc_addr.clone(),
                    plan: json!({}),
                    used_capacity: 0,
                    has_source: false,
                });
            }

            QueryPlan {
                query,
                workers,
                fragments,
            }
        })
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
        id in proptest::string::string_regex("[a-z][a-z0-9_-]{2,29}").unwrap(),
        statement in proptest::string::string_regex("SELECT [a-z]+ FROM [a-z]+").unwrap(),
    ) -> CreateQuery {
        CreateQuery::new_with_name(id, statement)
    }
}

/// Strategy for generating a vector of queries with unique ids.
pub fn arb_unique_queries(max_queries: usize) -> impl Strategy<Value = Vec<CreateQuery>> {
    prop::collection::vec(arb_create_query(), 1..=max_queries).prop_map(|queries| {
        let mut seen = std::collections::HashSet::new();
        queries
            .into_iter()
            .filter(|q| seen.insert(q.id.clone()))
            .collect()
    })
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
