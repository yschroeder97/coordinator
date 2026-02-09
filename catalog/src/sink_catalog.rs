use crate::database::State;
use model::IntoCondition;
use model::sink;
use model::sink::{CreateSink, DropSink, Entity as SinkEntity, GetSink};
use sea_orm::{ActiveModelTrait, EntityTrait, QueryFilter};
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SinkCatalogError {
    #[error("Database error: {0}")]
    Database(#[from] sea_orm::DbErr),
}

pub struct SinkCatalog {
    db: State,
}

impl SinkCatalog {
    pub fn from(db: State) -> Arc<Self> {
        Arc::new(Self { db })
    }

    pub async fn create_sink(&self, req: CreateSink) -> Result<sink::Model, SinkCatalogError> {
        sink::ActiveModel::from(req)
            .insert(&self.db.conn)
            .await
            .map_err(Into::into)
    }

    pub async fn get_sink(&self, req: GetSink) -> Result<Vec<sink::Model>, SinkCatalogError> {
        SinkEntity::find()
            .filter(req.into_condition())
            .all(&self.db.conn)
            .await
            .map_err(Into::into)
    }

    pub async fn drop_sink(&self, req: DropSink) -> Result<Vec<sink::Model>, SinkCatalogError> {
        SinkEntity::delete_many()
            .filter(req.into_condition())
            .exec_with_returning(&self.db.conn)
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{test_grpc_addr, test_host_addr, test_prop};
    use crate::test_utils::test_worker_catalog;
    use model::sink::SinkType;
    use model::testing::{SinkWithRefs, arb_sink_with_refs};
    use model::worker::CreateWorker;
    use proptest::proptest;
    use sea_orm::sea_query::prelude::serde_json;

    #[tokio::test]
    async fn test_create_and_get_sink() {
        let db = State::for_test().await;
        let sink_catalog = SinkCatalog::from(db.clone());
        let worker_catalog = test_worker_catalog(db);

        let worker_req = CreateWorker::new(test_host_addr(), test_grpc_addr(), 10);
        worker_catalog
            .create_worker(worker_req)
            .await
            .expect("Worker creation should succeed");

        let sink_req = CreateSink {
            name: "test_sink".to_string(),
            host_addr: test_host_addr(),
            sink_type: SinkType::Print,
            config: serde_json::json!({}),
        };
        let created = sink_catalog
            .create_sink(sink_req)
            .await
            .expect("Sink creation should succeed");

        assert_eq!(created.name, "test_sink");
        assert_eq!(created.sink_type, SinkType::Print);

        let get_req = GetSink::new().by_name("test_sink".to_string());
        let sinks = sink_catalog
            .get_sink(get_req)
            .await
            .expect("Get sink should succeed");

        assert_eq!(sinks.len(), 1);
        assert_eq!(sinks[0].name, "test_sink");
    }

    #[tokio::test]
    async fn test_drop_sink() {
        let db = State::for_test().await;
        let sink_catalog = SinkCatalog::from(db.clone());
        let worker_catalog = test_worker_catalog(db);

        let worker_req = CreateWorker::new(test_host_addr(), test_grpc_addr(), 10);
        worker_catalog.create_worker(worker_req).await.unwrap();

        let sink_req = CreateSink {
            name: "sink_to_drop".to_string(),
            host_addr: test_host_addr(),
            sink_type: SinkType::File,
            config: serde_json::json!({}),
        };
        sink_catalog.create_sink(sink_req).await.unwrap();

        let drop_req = DropSink::new().with_name("sink_to_drop".to_string());
        let dropped = sink_catalog
            .drop_sink(drop_req)
            .await
            .expect("Drop should succeed");

        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].name, "sink_to_drop");

        let get_req = GetSink::new().by_name("sink_to_drop".to_string());
        let sinks = sink_catalog.get_sink(get_req).await.unwrap();
        assert!(sinks.is_empty(), "Sink should be deleted");
    }

    async fn prop_sink_name_unique(db: State, req: SinkWithRefs) {
        let sink_catalog = SinkCatalog::from(db.clone());
        let worker_catalog = test_worker_catalog(db);

        worker_catalog
            .create_worker(req.worker)
            .await
            .expect("Worker creation should succeed");

        sink_catalog
            .create_sink(req.sink.clone())
            .await
            .expect("First sink creation should succeed");

        assert!(
            sink_catalog.create_sink(req.sink.clone()).await.is_err(),
            "Duplicate sink name '{}' should be rejected",
            req.sink.name
        );
    }

    async fn prop_sink_worker_ref_required(db: State, req: SinkWithRefs) {
        let sink_catalog = SinkCatalog::from(db.clone());
        let worker_catalog = test_worker_catalog(db);

        // Without worker, sink creation should fail
        assert!(
            sink_catalog.create_sink(req.sink.clone()).await.is_err(),
            "Sink creation without worker should be rejected"
        );

        // After creating worker, sink creation should succeed
        worker_catalog
            .create_worker(req.worker)
            .await
            .expect("Worker creation should succeed");

        sink_catalog
            .create_sink(req.sink)
            .await
            .expect("Sink creation with valid worker ref should succeed");
    }

    async fn prop_create_drop_create_sink(db: State, req: SinkWithRefs) {
        let sink_catalog = SinkCatalog::from(db.clone());
        let worker_catalog = test_worker_catalog(db);

        worker_catalog
            .create_worker(req.worker)
            .await
            .expect("Worker creation should succeed");

        sink_catalog
            .create_sink(req.sink.clone())
            .await
            .expect("First sink creation should succeed");

        let drop_req = DropSink::new().with_name(req.sink.name.clone());
        sink_catalog
            .drop_sink(drop_req)
            .await
            .expect("Drop should succeed");

        sink_catalog
            .create_sink(req.sink)
            .await
            .expect("Second creation after drop should succeed");
    }

    proptest! {
        #[test]
        fn sink_name_unique(req in arb_sink_with_refs()) {
            test_prop(|db| async move {
                prop_sink_name_unique(db, req).await;
            });
        }

        #[test]
        fn sink_worker_ref_required(req in arb_sink_with_refs()) {
            test_prop(|db| async move {
                prop_sink_worker_ref_required(db, req).await;
            });
        }

        #[test]
        fn create_drop_create_sink_succeeds(req in arb_sink_with_refs()) {
            test_prop(|db| async move {
                prop_create_drop_create_sink(db, req).await;
            });
        }
    }
}
