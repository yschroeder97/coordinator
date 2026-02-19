use crate::database::Database;
use crate::notification::{NotifiableCatalog, NotificationChannel};
use anyhow::Result;
use model::IntoCondition;
use model::worker::endpoint::HostAddr;
use model::worker::network_link;
use model::worker::{
    self, CreateWorker, DesiredWorkerState, DropWorker, Entity as WorkerEntity, GetWorker,
    WorkerState,
};
use sea_orm::sea_query::Expr;
use sea_orm::{ActiveModelTrait, ActiveValue::Set, EntityTrait, QueryFilter, TransactionTrait};
use std::sync::Arc;

pub struct WorkerCatalog {
    db: Database,
    notifications: NotificationChannel,
}

impl WorkerCatalog {
    pub fn new(db: Database) -> Arc<Self> {
        Arc::new(Self {
            db,
            notifications: NotificationChannel::new(),
        })
    }

    pub async fn create_worker(&self, req: CreateWorker) -> Result<worker::Model> {
        let txn = self.db.conn.begin().await?;

        let host_addr = req.host_addr.clone();
        let peers = req.peers.clone();
        let worker_model = worker::ActiveModel::from(req).insert(&txn).await?;

        network_link::Entity::insert_many(peers.into_iter().map(|peer| {
            network_link::ActiveModel {
                source_host_addr: Set(host_addr.clone()),
                target_host_addr: Set(peer),
            }
        }))
        .exec(&txn)
        .await?;

        txn.commit().await?;
        self.notifications.notify_intent();
        Ok(worker_model)
    }

    pub async fn get_worker(&self, req: GetWorker) -> Result<Vec<worker::Model>> {
        Ok(WorkerEntity::find()
            .filter(req.into_condition())
            .all(&self.db.conn)
            .await?)
    }

    pub async fn drop_worker(&self, req: DropWorker) -> Result<worker::Model> {
        let model = worker::ActiveModel {
            host_addr: sea_orm::ActiveValue::Unchanged(req.host_addr),
            desired_state: Set(DesiredWorkerState::Removed),
            ..Default::default()
        };
        let updated = model.update(&self.db.conn).await?;
        self.notifications.notify_intent();
        Ok(updated)
    }

    pub async fn delete_worker(&self, host_addr: &HostAddr) -> Result<Option<worker::Model>> {
        Ok(WorkerEntity::delete_by_id(host_addr.clone())
            .exec_with_returning(&self.db.conn)
            .await?)
    }

    pub async fn get_mismatch(&self) -> Result<Vec<worker::Model>> {
        Ok(WorkerEntity::find()
            .filter(Expr::cust("current_state <> desired_state"))
            .all(&self.db.conn)
            .await?)
    }

    pub async fn update_worker_state(
        &self,
        mut worker: worker::ActiveModel,
        new_state: WorkerState,
    ) -> Result<worker::Model> {
        worker.current_state = Set(new_state);
        let updated = worker.update(&self.db.conn).await?;
        self.notifications.notify_state();
        Ok(updated)
    }
}

impl NotifiableCatalog for WorkerCatalog {
    fn subscribe_intent(&self) -> tokio::sync::watch::Receiver<()> {
        self.notifications.subscribe_intent()
    }

    fn subscribe_state(&self) -> tokio::sync::watch::Receiver<()> {
        self.notifications.subscribe_state()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Catalog;
    use crate::sink_catalog::SinkCatalog;
    use crate::source_catalog::SourceCatalog;
    use crate::test_utils::{test_grpc_addr, test_host_addr, test_prop};
    use model::testing::{
        PhysicalSourceWithRefs, SinkWithRefs, arb_create_worker, arb_physical_with_refs,
        arb_sink_with_refs, arb_unique_workers,
    };
    use proptest::prelude::*;

    fn test_worker_catalog(db: Database) -> Arc<WorkerCatalog> {
        WorkerCatalog::new(db)
    }

    #[tokio::test]
    async fn test_create_and_get_worker() {
        let db = Database::for_test().await;
        let catalog = test_worker_catalog(db);

        let req = CreateWorker::new(test_host_addr(), test_grpc_addr(), 10);

        let created = catalog
            .create_worker(req)
            .await
            .expect("Worker creation should succeed");

        assert_eq!(created.host_addr, test_host_addr());
        assert_eq!(created.grpc_addr, test_grpc_addr());
        assert_eq!(created.capacity, 10);
        assert_eq!(created.current_state, WorkerState::Pending);
        assert_eq!(created.desired_state, DesiredWorkerState::Active);

        let get_req = GetWorker::all().with_host_addr(test_host_addr());
        let workers = catalog
            .get_worker(get_req)
            .await
            .expect("Get worker should succeed");

        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].host_addr, test_host_addr());
    }

    #[tokio::test]
    async fn test_drop_and_delete_worker() {
        let db = Database::for_test().await;
        let catalog = test_worker_catalog(db);

        let req = CreateWorker::new(test_host_addr(), test_grpc_addr(), 10);
        catalog.create_worker(req).await.unwrap();

        let drop_req = DropWorker::new(test_host_addr());
        let updated = catalog
            .drop_worker(drop_req)
            .await
            .expect("Drop should succeed");
        assert_eq!(updated.desired_state, DesiredWorkerState::Removed);

        // Delete actually removes
        let deleted = catalog
            .delete_worker(&test_host_addr())
            .await
            .expect("Delete should succeed");
        assert!(deleted.is_some());

        // Verify it's gone
        let get_req = GetWorker::all().with_host_addr(test_host_addr());
        let workers = catalog.get_worker(get_req).await.unwrap();
        assert!(workers.is_empty(), "Worker should be deleted");
    }

    #[tokio::test]
    async fn test_mark_worker_state() {
        let db = Database::for_test().await;
        let catalog = test_worker_catalog(db);

        let req = CreateWorker::new(test_host_addr(), test_grpc_addr(), 10);
        let created = catalog.create_worker(req).await.unwrap();

        // Mark as Active
        catalog
            .update_worker_state(created.into(), WorkerState::Active)
            .await
            .expect("Mark should succeed");

        let get_req = GetWorker::all().with_host_addr(test_host_addr());
        let workers = catalog.get_worker(get_req).await.unwrap();
        assert_eq!(workers[0].current_state, WorkerState::Active);
    }

    #[tokio::test]
    async fn test_host_addr_grpc_addr_must_differ() {
        use model::worker::endpoint::HostAddr;

        let db = Database::for_test().await;
        let catalog = test_worker_catalog(db);

        // Same address for both host_addr and grpc_addr should fail
        let same_addr = HostAddr::new("localhost", 8080);
        let req = CreateWorker::new(same_addr.clone(), same_addr, 10);

        let result = catalog.create_worker(req).await;
        assert!(
            result.is_err(),
            "Worker creation should fail when host_addr equals grpc_addr"
        );
    }

    async fn prop_worker_host_addr_unique(req: CreateWorker) {
        let catalog = Catalog::for_test().await;

        catalog
            .worker
            .create_worker(req.clone())
            .await
            .expect("First worker creation should succeed");

        assert!(
            catalog.worker.create_worker(req.clone()).await.is_err(),
            "Duplicate worker host_addr '{}' should be rejected",
            req.host_addr
        );
    }

    async fn prop_worker_delete_with_physical_source_fails(req: PhysicalSourceWithRefs) {
        let catalog = Catalog::for_test().await;
        let source_catalog = catalog.source.clone();
        let worker_catalog = catalog.worker.clone();

        // Setup: create logical source, worker, and physical source
        source_catalog
            .create_logical_source(req.logical)
            .await
            .expect("Logical source creation should succeed");
        worker_catalog
            .create_worker(req.worker.clone())
            .await
            .expect("Worker creation should succeed");
        source_catalog
            .create_physical_source(req.physical)
            .await
            .expect("Physical source creation should succeed");

        // Property: Cannot delete worker while physical sources reference it
        assert!(
            worker_catalog
                .delete_worker(&req.worker.host_addr)
                .await
                .is_err(),
            "Should not be able to delete worker '{}' while physical sources reference it",
            req.worker.host_addr
        );
    }

    async fn prop_worker_delete_with_sink_fails(req: SinkWithRefs) {
        let catalog = Catalog::for_test().await;
        let sink_catalog = catalog.sink.clone();
        let worker_catalog = catalog.worker.clone();

        // Setup: create worker and sink
        worker_catalog
            .create_worker(req.worker.clone())
            .await
            .expect("Worker creation should succeed");
        sink_catalog
            .create_sink(req.sink)
            .await
            .expect("Sink creation should succeed");

        // Property: Cannot delete worker while sinks reference it
        assert!(
            worker_catalog
                .delete_worker(&req.worker.host_addr)
                .await
                .is_err(),
            "Should not be able to delete worker '{}' while sinks reference it",
            req.worker.host_addr
        );
    }

    async fn prop_create_delete_create_worker(req: CreateWorker) {
        let catalog = Catalog::for_test().await;

        catalog
            .worker
            .create_worker(req.clone())
            .await
            .expect("First creation should succeed");

        catalog
            .worker
            .delete_worker(&req.host_addr)
            .await
            .expect("Delete should succeed");

        catalog
            .worker
            .create_worker(req)
            .await
            .expect("Second creation after delete should succeed");
    }

    /// Property: get_mismatch correctly partitions workers by state match
    async fn prop_get_mismatch_correctness(workers: Vec<CreateWorker>) {
        let catalog = Catalog::for_test().await;

        // Create all workers (they start with Pending/Active - a mismatch)
        let mut created: Vec<worker::Model> = Vec::new();
        for worker in &workers {
            let model = catalog.worker.create_worker(worker.clone()).await.unwrap();
            created.push(model);
        }

        // Mark workers at even indices as Active to create matches (current=Active=desired)
        for (i, worker) in created.into_iter().enumerate() {
            if i % 2 == 0 {
                catalog
                    .worker
                    .update_worker_state(worker.into(), WorkerState::Active)
                    .await
                    .unwrap();
            }
        }

        let mismatched = catalog.worker.get_mismatch().await.unwrap();
        let all_workers = catalog.worker.get_worker(GetWorker::all()).await.unwrap();

        let mismatched_addrs: std::collections::HashSet<_> =
            mismatched.iter().map(|w| &w.host_addr).collect();

        for worker in &all_workers {
            let is_mismatched =
                worker.current_state.to_string() != worker.desired_state.to_string();
            let in_result = mismatched_addrs.contains(&worker.host_addr);

            assert_eq!(
                is_mismatched,
                in_result,
                "Worker '{}': is_mismatched={} but in_result={}. State: {:?}/{:?}",
                worker.host_addr,
                is_mismatched,
                in_result,
                worker.current_state,
                worker.desired_state
            );
        }
    }

    proptest! {
        #[test]
        fn worker_host_addr_unique(req in arb_create_worker()) {
            test_prop(|| async move {
                prop_worker_host_addr_unique(req).await;
            });
        }

        #[test]
        fn worker_delete_with_physical_source_fails(req in arb_physical_with_refs()) {
            test_prop(|| async move {
                prop_worker_delete_with_physical_source_fails(req).await;
            });
        }

        #[test]
        fn worker_delete_with_sink_fails(req in arb_sink_with_refs()) {
            test_prop(|| async move {
                prop_worker_delete_with_sink_fails(req).await;
            });
        }

        #[test]
        fn create_delete_create_worker_succeeds(req in arb_create_worker()) {
            test_prop(|| async move {
                prop_create_delete_create_worker(req).await;
            });
        }

        #[test]
        fn get_mismatch_correctness(workers in arb_unique_workers(10)) {
            test_prop(|| async move {
                prop_get_mismatch_correctness(workers).await;
            });
        }
    }
}
