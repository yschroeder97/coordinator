// #![cfg(feature = "strategy")]
// use crate::catalog::Catalog;
// use crate::catalog::query::CreateQuery;
// use crate::catalog::query::fragment::CreateFragment;
// use crate::catalog::query::query_state::{QueryState, QueryStateTransitionType};
// use crate::catalog::sink::{CreateSink, SinkType};
// use crate::catalog::source::logical_source::CreateLogicalSource;
// use crate::catalog::source::physical_source::{CreatePhysicalSource, SourceType};
// use crate::catalog::worker::endpoint::{DEFAULT_DATA_PORT, DEFAULT_GRPC_PORT, NetworkAddr};
// use crate::catalog::worker::{CreateWorker, Worker};
// use proptest::arbitrary::any;
// use proptest::prelude::{Rng, prop};
// use proptest::prop_compose;
// use proptest::strategy::{Just, Strategy};
// use serde_json::json;
// use sqlx::sqlite::SqlitePoolOptions;
// use std::future::Future;
//
// /// Helper function to run async property tests with a fresh in-memory Catalog.
// /// Creates a new SQLite in-memory database, runs migrations, and passes the
// /// resulting Catalog to the test function.
// /// Panics are treated as test failures.
// pub fn test_prop<F, Fut>(f: F)
// where
//     F: FnOnce(Catalog) -> Fut,
//     Fut: Future<Output = ()>,
// {
//     let rt = tokio::runtime::Builder::new_current_thread()
//         .enable_all()
//         .build()
//         .expect("Failed to create tokio runtime");
//
//     rt.block_on(async {
//         let pool = SqlitePoolOptions::new()
//             .max_connections(1)
//             .connect(":memory:")
//             .await
//             .expect("Failed to create in-memory SQLite pool");
//
//         let catalog = Catalog::from_pool(pool)
//             .await
//             .expect("Failed to create catalog from pool");
//
//         f(catalog).await;
//     });
// }
//
// // TODO: create valid topologies that are DAGs
// prop_compose! {
//     pub fn arb_create_worker()(
//         host_name in r"[a-z0-9]{3,15}",
//         grpc_port in 1..65534u16,
//         capacity in any::<u8>(),
//         peers in prop::collection::vec(any::<NetworkAddr>(), 0..10)
//     ) -> CreateWorker {
//         let cap = capacity as u32;
//         CreateWorker {
//             host_name,
//             grpc_port,
//             data_port: grpc_port + 1,
//             capacity: cap,
//             peers,
//         }
//     }
// }
//
// // TODO: give the workers valid peers
// fn arb_worker_custom(
//     host: String,
//     grpc: u16,
//     data: u16,
// ) -> impl Strategy<Value = CreateWorker> {
//     (1..100u32).prop_map(move |capacity| CreateWorker {
//         host_name: host.clone(),
//         grpc_port: grpc,
//         data_port: data,
//         capacity,
//         peers: vec![],
//     })
// }
//
// pub fn arb_worker_cluster_on_subnet(count: u8) -> impl Strategy<Value = Vec<CreateWorker>> {
//     prop::collection::hash_set(1..255u8, count as usize).prop_flat_map(move |octets| {
//         octets
//             .into_iter()
//             .map(|octet| {
//                 let ip = format!("192.168.1.{}", octet);
//                 arb_worker_custom(ip, DEFAULT_GRPC_PORT, DEFAULT_DATA_PORT)
//             })
//             .collect::<Vec<_>>()
//     })
// }
//
// prop_compose! {
//     pub fn arb_create_query()(
//         name in "[a-z]{1,20}",
//         stmt in "[a-z0-9]{0,50}",
//     ) -> CreateQuery {
//         CreateQuery {
//             name,
//             stmt,
//             fragments: None,
//         }
//     }
// }
//
// /// Create fragments for a query placed on the given workers
// pub fn arb_fragments(workers: Vec<Worker>, query_name: String) -> Vec<CreateFragment> {
//     workers
//         .iter()
//         .map(|worker| CreateFragment {
//             query_id: query_name.clone(),
//             host_name: worker.host_name.clone(),
//             grpc_port: worker.grpc_port,
//             plan: json!("{}"),
//             used_capacity: 0,
//             has_source: false,
//         })
//         .collect()
// }
//
// /// A valid query request with all its dependencies
// #[derive(Debug, Clone)]
// pub struct QueryWithRefs {
//     pub workers: Vec<CreateWorker>,
//     pub fragments: Vec<CreateFragment>,
//     pub query: CreateQuery,
// }
//
// pub fn arb_query_with_deps(
//     max_num_workers: u8,
//     max_num_fragments: usize,
// ) -> impl Strategy<Value = QueryWithRefs> {
//     arb_worker_cluster_on_subnet(max_num_workers)
//         .prop_flat_map(move |workers| {
//             let num_workers = workers.len();
//             (
//                 Just(workers),
//                 "[a-z]{1,20}",
//                 "[a-z0-9]{0,50}",
//                 prop::collection::vec(0..num_workers, 1..max_num_fragments + 1),
//             )
//         })
//         .prop_map(|(workers, name, stmt, worker_indices)| {
//             let query = CreateQuery {
//                 name: name.clone(),
//                 stmt,
//                 fragments: None,
//             };
//
//             let fragments = worker_indices
//                 .into_iter()
//                 .map(|idx| {
//                     let worker = &workers[idx];
//                     CreateFragment {
//                         query_id: name.clone(),
//                         host_name: worker.host_name.clone(),
//                         grpc_port: worker.grpc_port,
//                         plan: json!("{}"),
//                         used_capacity: 0,
//                         has_source: false,
//                     }
//                 })
//                 .collect();
//
//             QueryWithRefs {
//                 workers,
//                 query,
//                 fragments,
//             }
//         })
// }
//
// /// Strategy to produce a sequence of query states
// pub fn arb_state_sequence(
//     transition: QueryStateTransitionType,
// ) -> impl Strategy<Value = Vec<QueryState>> {
//     Just(vec![QueryState::Pending]).prop_perturb(move |mut states, mut rng| {
//         loop {
//             let current = *states.last().unwrap();
//             let valid_nexts = current.transitions();
//
//             match transition {
//                 QueryStateTransitionType::Invalid => {
//                     if valid_nexts.is_empty() || rng.random_bool(0.2) {
//                         let invalid_nexts = current.invalid_transitions();
//                         let idx = rng.random_range(0..invalid_nexts.len());
//                         states.push(invalid_nexts[idx]);
//                         return states;
//                     }
//                 }
//                 QueryStateTransitionType::Valid => {
//                     if valid_nexts.is_empty() {
//                         return states;
//                     }
//                 }
//             }
//
//             let idx = rng.random_range(0..valid_nexts.len());
//             states.push(valid_nexts[idx]);
//         }
//     })
// }
