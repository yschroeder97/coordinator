// use crate::catalog::catalog_base::Catalog;
// use crate::catalog::notification::NotificationScope;
// use crate::catalog::worker::worker::{MarkWorker, WorkerState};
// use crate::catalog::worker::worker_endpoint::{GrpcAddr, NetworkAddr};
// use crate::catalog::WorkerCatalog;
// use crate::network::cluster_service::WorkerStateInternal::Active;
// use crate::network::poly_join_set::{AbortHandle, JoinSet};
// use crate::network::worker_client::{ConnErr, Rpc, WorkerClient};
// use crate::network::worker_registry::{WorkerRegistry, WorkerRegistryHandle};
// use std::collections::hash_map::Entry;
// use std::collections::HashMap;
// use std::sync::Arc;
// use thiserror::Error;
// use tracing::{error, info, warn};
// 
// #[derive(Debug)]
// enum WorkerStateInternal {
//     Pending { handle: AbortHandle },
//     Active { client: WorkerClient },
//     Unreachable { last_seen: std::time::Instant },
//     Failed { error: ClusterServiceErr },
// }
// 
// #[derive(Error, Debug)]
// pub enum ClusterServiceErr {
//     #[error("Could not connect to worker with '{addr}'")]
//     ConnectionErr { addr: NetworkAddr, msg: String },
// }
// 
// pub(crate) struct ClusterService {
//     catalog: Arc<WorkerCatalog>,
//     registry: WorkerRegistry,
//     workers: HashMap<GrpcAddr, WorkerStateInternal>,
//     pending: JoinSet<Result<(flume::Sender<Rpc>, WorkerClient), ConnErr>>,
// }
// 
// /// Invariants:
// /// - An addr should be only in a single state set at a time
// /// - The number of workers in all sets combined should be the number of workers in the catalog's table AFTER a call to on_client_request
// /// - The current state in the catalog should match the set membership here
// impl ClusterService {
//     pub fn new(catalog: Arc<WorkerCatalog>) -> Self {
//         ClusterService {
//             catalog,
//             registry: WorkerRegistry::default(),
//             workers: HashMap::default(),
//             pending: JoinSet::new(),
//         }
//     }
// 
//     pub fn registry_handle(&self) -> WorkerRegistryHandle {
//         self.registry.handle()
//     }
// 
//     pub async fn run(&mut self) -> Result<(), ClusterServiceErr> {
//         let mut workers = self.catalog.subscribe(NotificationScope::Worker);
//         info!("Starting");
// 
//         loop {
//             tokio::select! {
//                 _ = workers.changed() => self.on_client_request().await,
//                 Some(result) = self.pending.join_next() => match result {
//                     Ok(progress) => self.on_progress_pending(progress).await,
//                     Err(join_err) => {
//                         error!("Worker connection task failed: {:?}", join_err);
//                     }
//                 }
//             }
//         }
//     }
// 
//     async fn on_client_request(&mut self) {
//         self.add_workers().await;
//         self.drop_workers().await;
//     }
// 
//     async fn add_workers(&mut self) {
//         let to_add = self
//             .catalog
//             .get_workers(
//                 &MarkWorker::new()
//                     .current_state(WorkerState::Pending)
//                     .desired_state(WorkerState::Active),
//             )
//             .await
//             .unwrap();
// 
//         for worker in to_add.into_iter() {
//             let addr = GrpcAddr::new(worker.host_name, worker.grpc_port);
//             if self.workers.contains_key(&addr) {
//                 continue;
//             }
//             info!(
//                 "Worker {} is '{}', but should be '{}'",
//                 addr.clone(),
//                 worker.current_state,
//                 worker.desired_state
//             );
//             let handle = self.pending.spawn(WorkerClient::connect(addr.clone()));
//             self.workers
//                 .insert(addr, WorkerStateInternal::Pending { handle });
//         }
//     }
// 
//     async fn drop_workers(&mut self) {
//         let to_drop = self
//             .catalog
//             .get_workers(&MarkWorker::new().desired_state(WorkerState::Removed))
//             .await
//             .unwrap();
// 
//         for worker in to_drop.into_iter() {
//             let addr = GrpcAddr::new(worker.host_name, worker.grpc_port);
//             info!(
//                 "Worker {} is '{}', but should be '{}'",
//                 addr, worker.current_state, worker.desired_state
//             );
// 
//             // Cancel and remove worker state
//             if let Some(worker_state) = self.workers.remove(&addr) {
//                 match worker_state {
//                     WorkerStateInternal::Pending { handle } => {
//                         handle.abort();
//                         info!("Aborted pending connection for worker {}", addr);
//                     }
//                     WorkerStateInternal::Active { .. } => {
//                         // Remove from registry when removing active worker
//                         self.registry.unregister(&addr);
//                         info!("Removed active worker {}", addr);
//                     }
//                     other => {
//                         info!("Removed worker {} in state {:?}", addr, other);
//                     }
//                 }
//             } else {
//                 warn!("Worker {} not found in internal state", addr);
//             }
//         }
//     }
// 
//     async fn on_progress_pending(
//         &mut self,
//         client_or_err: Result<(flume::Sender<Rpc>, WorkerClient), ConnErr>,
//     ) -> () {
//         match client_or_err {
//             Ok((sender, client)) => self.on_connect_success(sender, client).await,
//             Err(ConnErr::Failed(err, addr)) => self.on_connect_err(err, addr).await,
//         }
//     }
// 
//     async fn on_connect_success(
//         &mut self,
//         rpc_sender: flume::Sender<Rpc>,
//         client: WorkerClient,
//     ) -> () {
//         let addr = client.grpc_addr();
//         self.catalog
//             .mark_worker(&addr, WorkerState::Active)
//             .await
//             .unwrap();
// 
//         // Update internal state
//         match self.workers.entry(addr.clone()) {
//             Entry::Occupied(mut entry) => match entry.get() {
//                 WorkerStateInternal::Pending { handle: _ } => {
//                     entry.insert(Active { client });
//                     // Register in registry only after successfully updating internal state
//                     self.registry.register(addr, rpc_sender);
//                 }
//                 other => {
//                     error!("Worker should be in state Pending, but was {:?}", other);
//                     entry.insert(Active { client });
//                     self.registry.register(addr, rpc_sender);
//                 }
//             },
//             Entry::Vacant(_) => {
//                 error!("Worker {} should be present, but was missing", addr);
//                 return;
//             }
//         };
//     }
// 
//     async fn on_connect_err(&mut self, err: tonic::transport::Error, addr: GrpcAddr) -> () {
//         self.catalog
//             .mark_worker(&addr, WorkerState::Failed)
//             .await
//             .unwrap();
// 
//         // Update internal state to Failed
//         match self.workers.entry(addr.clone()) {
//             Entry::Occupied(mut entry) => {
//                 entry.insert(WorkerStateInternal::Failed {
//                     error: ClusterServiceErr::ConnectionErr {
//                         addr,
//                         msg: err.to_string(),
//                     },
//                 });
//             }
//             Entry::Vacant(_) => {
//                 error!("Worker {} should be present in pending state", addr);
//             }
//         }
//         error!("Failed to connect to worker: {:?}", err);
//     }
// }
