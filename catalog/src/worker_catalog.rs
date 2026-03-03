use crate::database::Database;
use crate::notification::{NotificationChannel, Reconcilable};
use anyhow::{Result, ensure};
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
    listeners: NotificationChannel,
}

impl WorkerCatalog {
    pub fn from(db: Database) -> Arc<Self> {
        Arc::new(Self {
            db,
            listeners: NotificationChannel::new(),
        })
    }

    pub async fn create_worker(&self, req: CreateWorker) -> Result<worker::Model> {
        ensure!(
            !req.peers.contains(&req.host_addr),
            "Worker cannot reference itself as a peer: {}",
            req.host_addr
        );

        let txn = self.db.conn.begin().await?;

        let host_addr = req.host_addr.clone();
        let peers = req.peers.clone();
        let worker_model = worker::ActiveModel::from(req).insert(&txn).await?;

        if !peers.is_empty() {
            network_link::Entity::insert_many(peers.into_iter().map(|peer| {
                network_link::ActiveModel {
                    source_host_addr: Set(host_addr.clone()),
                    target_host_addr: Set(peer),
                }
            }))
            .exec(&txn)
            .await?;
        }

        txn.commit().await?;
        self.listeners.notify_intent();
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
        self.listeners.notify_intent();
        Ok(updated)
    }

    pub async fn delete_worker(&self, host_addr: &HostAddr) -> Result<Option<worker::Model>> {
        let model = WorkerEntity::find_by_id(host_addr.clone())
            .one(&self.db.conn)
            .await?;
        if model.is_some() {
            WorkerEntity::delete_by_id(host_addr.clone())
                .exec(&self.db.conn)
                .await?;
        }
        Ok(model)
    }

    pub async fn update_worker_state(
        &self,
        mut worker: worker::ActiveModel,
        new_state: WorkerState,
    ) -> Result<worker::Model> {
        worker.current_state = Set(new_state);
        let updated = worker.update(&self.db.conn).await?;
        self.listeners.notify_state();
        Ok(updated)
    }
}

impl Reconcilable for WorkerCatalog {
    type Model = worker::Model;

    fn subscribe_intent(&self) -> tokio::sync::watch::Receiver<()> {
        self.listeners.subscribe_intent()
    }

    fn subscribe_state(&self) -> tokio::sync::watch::Receiver<()> {
        self.listeners.subscribe_state()
    }

    async fn get_mismatch(&self) -> Result<Vec<worker::Model>> {
        Ok(WorkerEntity::find()
            .filter(Expr::cust("current_state <> desired_state"))
            .all(&self.db.conn)
            .await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Catalog;
    use crate::testing::test_prop;
    use model::query::CreateQuery;
    use model::query::fragment::CreateFragment;
    use model::testing::{
        PhysicalSourceWithRefs, SinkWithRefs, arb_create_worker, arb_physical_with_refs,
        arb_sink_with_refs, arb_unique_workers,
    };
    use proptest::prelude::*;

    async fn prop_create_and_get_worker(req: CreateWorker) {
        let catalog = Catalog::for_test().await;

        let created = catalog
            .worker
            .create_worker(req.clone())
            .await
            .expect("Worker creation should succeed");

        assert_eq!(created.host_addr, req.host_addr);
        assert_eq!(created.grpc_addr, req.grpc_addr);
        assert_eq!(created.capacity, req.capacity);
        assert_eq!(created.current_state, WorkerState::Pending);
        assert_eq!(created.desired_state, DesiredWorkerState::Active);

        let workers = catalog
            .worker
            .get_worker(GetWorker::all().with_host_addr(req.host_addr.clone()))
            .await
            .expect("Get worker should succeed");

        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].host_addr, req.host_addr);
    }

    async fn prop_drop_and_delete_worker(req: CreateWorker) {
        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(req.clone()).await.unwrap();

        let updated = catalog
            .worker
            .drop_worker(DropWorker::new(req.host_addr.clone()))
            .await
            .expect("Drop should succeed");
        assert_eq!(updated.desired_state, DesiredWorkerState::Removed);

        let deleted = catalog
            .worker
            .delete_worker(&req.host_addr)
            .await
            .expect("Delete should succeed");
        assert!(deleted.is_some());

        let workers = catalog
            .worker
            .get_worker(GetWorker::all().with_host_addr(req.host_addr))
            .await
            .unwrap();
        assert!(workers.is_empty(), "Worker should be deleted");
    }

    async fn prop_mark_worker_state(req: CreateWorker) {
        let catalog = Catalog::for_test().await;
        let created = catalog.worker.create_worker(req.clone()).await.unwrap();

        catalog
            .worker
            .update_worker_state(created.into(), WorkerState::Active)
            .await
            .expect("Mark should succeed");

        let workers = catalog
            .worker
            .get_worker(GetWorker::all().with_host_addr(req.host_addr))
            .await
            .unwrap();
        assert_eq!(workers[0].current_state, WorkerState::Active);
    }

    async fn prop_host_addr_grpc_addr_must_differ(req: CreateWorker) {
        let catalog = Catalog::for_test().await;
        let same_addr = CreateWorker::new(req.host_addr.clone(), req.host_addr, req.capacity);

        assert!(
            catalog.worker.create_worker(same_addr).await.is_err(),
            "Worker creation should fail when host_addr equals grpc_addr"
        );
    }

    async fn prop_grpc_addr_unique(w1: CreateWorker, mut w2: CreateWorker) {
        if w1.host_addr == w2.host_addr {
            return;
        }
        w2.grpc_addr = w1.grpc_addr.clone();
        if w2.host_addr == w2.grpc_addr {
            return;
        }

        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(w1).await.unwrap();
        assert!(
            catalog.worker.create_worker(w2).await.is_err(),
            "Duplicate grpc_addr should be rejected"
        );
    }

    async fn prop_grpc_addr_may_equal_other_host_addr(w1: CreateWorker, mut w2: CreateWorker) {
        if w1.host_addr == w2.host_addr {
            return;
        }
        w2.grpc_addr = w1.host_addr.clone();
        if w2.host_addr == w2.grpc_addr || w1.grpc_addr == w2.grpc_addr {
            return;
        }

        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(w1).await.unwrap();
        catalog
            .worker
            .create_worker(w2)
            .await
            .expect("grpc_addr matching another worker's host_addr should be allowed");
    }

    async fn prop_worker_delete_blocked_by_fragments(req: CreateWorker) {
        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(req.clone()).await.unwrap();

        let query = catalog
            .query
            .create_query(CreateQuery::new("SELECT x FROM y".to_string()))
            .await
            .unwrap();
        catalog
            .query
            .create_fragments(
                &query,
                vec![CreateFragment {
                    query_id: query.id,
                    host_addr: req.host_addr.clone(),
                    grpc_addr: req.grpc_addr.clone(),
                    plan: serde_json::json!({}),
                    used_capacity: 0,
                    has_source: false,
                }],
            )
            .await
            .unwrap();

        assert!(
            catalog.worker.delete_worker(&req.host_addr).await.is_err(),
            "Worker with fragments should not be deletable"
        );
    }

    async fn prop_worker_self_peer_rejected(req: CreateWorker) {
        let catalog = Catalog::for_test().await;
        let worker = req.clone().with_peers(vec![req.host_addr]);

        assert!(
            catalog.worker.create_worker(worker).await.is_err(),
            "Worker referencing itself as peer should be rejected"
        );
    }

    async fn prop_network_links_cascade_on_source_delete(w1: CreateWorker, w2: CreateWorker) {
        if w1.host_addr == w2.host_addr || w1.grpc_addr == w2.grpc_addr {
            return;
        }

        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(w2.clone()).await.unwrap();

        let w1_with_peer = w1.clone().with_peers(vec![w2.host_addr.clone()]);
        catalog.worker.create_worker(w1_with_peer).await.unwrap();

        catalog
            .worker
            .delete_worker(&w1.host_addr)
            .await
            .expect("Deleting worker with outgoing links should succeed via CASCADE");

        catalog
            .worker
            .delete_worker(&w2.host_addr)
            .await
            .expect("Peer should be deletable after source worker cascade");
    }

    async fn prop_network_links_cascade_on_target_delete(w1: CreateWorker, w2: CreateWorker) {
        if w1.host_addr == w2.host_addr || w1.grpc_addr == w2.grpc_addr {
            return;
        }

        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(w2.clone()).await.unwrap();

        let w1_with_peer = w1.clone().with_peers(vec![w2.host_addr.clone()]);
        catalog.worker.create_worker(w1_with_peer).await.unwrap();

        catalog
            .worker
            .delete_worker(&w2.host_addr)
            .await
            .expect("Deleting target of a link should succeed via CASCADE");

        catalog
            .worker
            .delete_worker(&w1.host_addr)
            .await
            .expect("Source worker should be deletable after target cascade");
    }

    async fn prop_duplicate_peer_rejected(w1: CreateWorker, w2: CreateWorker) {
        if w1.host_addr == w2.host_addr || w1.grpc_addr == w2.grpc_addr {
            return;
        }

        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(w2.clone()).await.unwrap();

        let w1_dup_peer =
            w1.with_peers(vec![w2.host_addr.clone(), w2.host_addr.clone()]);
        assert!(
            catalog.worker.create_worker(w1_dup_peer).await.is_err(),
            "Duplicate peer entries should violate composite PK"
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

        worker_catalog
            .create_worker(req.worker.clone())
            .await
            .expect("Worker creation should succeed");
        sink_catalog
            .create_sink(req.sink)
            .await
            .expect("Sink creation should succeed");

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

    async fn prop_get_mismatch_correctness(workers: Vec<CreateWorker>) {
        let catalog = Catalog::for_test().await;

        let mut created: Vec<worker::Model> = Vec::new();
        for worker in &workers {
            let model = catalog.worker.create_worker(worker.clone()).await.unwrap();
            created.push(model);
        }

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
        fn create_and_get_worker(req in arb_create_worker()) {
            test_prop(|| async move {
                prop_create_and_get_worker(req).await;
            });
        }

        #[test]
        fn drop_and_delete_worker(req in arb_create_worker()) {
            test_prop(|| async move {
                prop_drop_and_delete_worker(req).await;
            });
        }

        #[test]
        fn mark_worker_state(req in arb_create_worker()) {
            test_prop(|| async move {
                prop_mark_worker_state(req).await;
            });
        }

        #[test]
        fn host_addr_grpc_addr_must_differ(req in arb_create_worker()) {
            test_prop(|| async move {
                prop_host_addr_grpc_addr_must_differ(req).await;
            });
        }

        #[test]
        fn grpc_addr_unique((w1, w2) in (arb_create_worker(), arb_create_worker())) {
            test_prop(|| async move {
                prop_grpc_addr_unique(w1, w2).await;
            });
        }

        #[test]
        fn grpc_addr_may_equal_other_host_addr((w1, w2) in (arb_create_worker(), arb_create_worker())) {
            test_prop(|| async move {
                prop_grpc_addr_may_equal_other_host_addr(w1, w2).await;
            });
        }

        #[test]
        fn worker_delete_blocked_by_fragments(req in arb_create_worker()) {
            test_prop(|| async move {
                prop_worker_delete_blocked_by_fragments(req).await;
            });
        }

        #[test]
        fn worker_self_peer_rejected(req in arb_create_worker()) {
            test_prop(|| async move {
                prop_worker_self_peer_rejected(req).await;
            });
        }

        #[test]
        fn network_links_cascade_on_source_delete((w1, w2) in (arb_create_worker(), arb_create_worker())) {
            test_prop(|| async move {
                prop_network_links_cascade_on_source_delete(w1, w2).await;
            });
        }

        #[test]
        fn network_links_cascade_on_target_delete((w1, w2) in (arb_create_worker(), arb_create_worker())) {
            test_prop(|| async move {
                prop_network_links_cascade_on_target_delete(w1, w2).await;
            });
        }

        #[test]
        fn duplicate_peer_rejected((w1, w2) in (arb_create_worker(), arb_create_worker())) {
            test_prop(|| async move {
                prop_duplicate_peer_rejected(w1, w2).await;
            });
        }

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
