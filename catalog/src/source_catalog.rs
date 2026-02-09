use crate::database::State;
use model::IntoCondition;
use model::source::logical_source::{
    self, CreateLogicalSource, DropLogicalSource, Entity as LogicalSourceEntity, GetLogicalSource,
};
use model::source::physical_source::{
    self, CreatePhysicalSource, DropPhysicalSource, Entity as PhysicalSourceEntity,
    GetPhysicalSource,
};
use sea_orm::{ActiveModelTrait, EntityTrait, QueryFilter};
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SourceCatalogError {
    #[error("Database error: {0}")]
    Database(#[from] sea_orm::DbErr),
}

pub struct SourceCatalog {
    db: State,
}

impl SourceCatalog {
    pub fn from(db: State) -> Arc<Self> {
        Arc::new(Self { db })
    }

    pub async fn create_logical_source(
        &self,
        req: CreateLogicalSource,
    ) -> Result<logical_source::Model, SourceCatalogError> {
        logical_source::ActiveModel::from(req)
            .insert(&self.db.conn)
            .await
            .map_err(Into::into)
    }

    pub async fn get_logical_source(
        &self,
        req: GetLogicalSource,
    ) -> Result<Option<logical_source::Model>, SourceCatalogError> {
        LogicalSourceEntity::find_by_id(req.with_name)
            .one(&self.db.conn)
            .await
            .map_err(Into::into)
    }

    pub async fn drop_logical_source(
        &self,
        req: DropLogicalSource,
    ) -> Result<Option<logical_source::Model>, SourceCatalogError> {
        LogicalSourceEntity::delete_by_id(req.with_name)
            .exec_with_returning(&self.db.conn)
            .await
            .map_err(Into::into)
    }

    pub async fn create_physical_source(
        &self,
        req: CreatePhysicalSource,
    ) -> Result<physical_source::Model, SourceCatalogError> {
        physical_source::ActiveModel::from(req)
            .insert(&self.db.conn)
            .await
            .map_err(Into::into)
    }

    pub async fn get_physical_source(
        &self,
        req: GetPhysicalSource,
    ) -> Result<Vec<physical_source::Model>, SourceCatalogError> {
        PhysicalSourceEntity::find()
            .filter(req.into_condition())
            .all(&self.db.conn)
            .await
            .map_err(Into::into)
    }

    pub async fn drop_physical_source(
        &self,
        req: DropPhysicalSource,
    ) -> Result<Vec<physical_source::Model>, SourceCatalogError> {
        PhysicalSourceEntity::delete_many()
            .filter(req.into_condition())
            .exec_with_returning(&self.db.conn)
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{test_grpc_addr, test_host_addr, test_prop, test_worker_catalog};
    use crate::worker_catalog::WorkerCatalog;
    use model::source::physical_source::SourceType;
    use model::source::schema::{DataType, Schema};
    use model::testing::{PhysicalSourceWithRefs, arb_physical_with_refs};
    use model::worker::CreateWorker;
    use proptest::proptest;

    #[tokio::test]
    async fn test_get_logical_source() {
        let catalog = SourceCatalog::from(State::for_test().await);

        let req = CreateLogicalSource {
            name: "source".to_string(),
            schema: Schema::from(vec![("a_bool".to_string(), DataType::BOOL)]),
        };

        let rsp = catalog.create_logical_source(req).await;
        assert!(rsp.is_ok(), "Logical source creation should succeed");
        assert_eq!(rsp.unwrap().name, "source");

        let rsp = catalog
            .get_logical_source(GetLogicalSource {
                with_name: "source".to_string(),
            })
            .await;

        let model = rsp.unwrap();
        assert!(model.is_some(), "Get request should return a value");
        assert_eq!(model.unwrap().name, "source");
    }

    #[tokio::test]
    async fn test_capacity_constraints() {
        let catalog = test_worker_catalog(State::for_test().await);

        let worker_req1 = CreateWorker::new(test_host_addr(), test_grpc_addr(), -1);
        let mut worker_req2 = worker_req1.clone();
        worker_req2.capacity = 0;

        assert!(
            catalog.create_worker(worker_req1).await.is_err(),
            "Cannot create worker with negative capacity"
        );
        assert!(
            catalog.create_worker(worker_req2).await.is_ok(),
            "Worker with capacity of zero is allowed"
        );
    }

    #[tokio::test]
    async fn test_get_physical_source() {
        let db = State::for_test().await;
        let source_catalog = SourceCatalog::from(db.clone());
        let worker_catalog = test_worker_catalog(db);

        let logical_req = CreateLogicalSource {
            name: "source".to_string(),
            schema: Schema::from(vec![("a_bool".to_string(), DataType::BOOL)]),
        };
        let worker_req = CreateWorker::new(test_host_addr(), test_grpc_addr(), 10);
        let physical_source_req = CreatePhysicalSource {
            logical_source: "source".to_string(),
            host_addr: test_host_addr(),
            source_type: SourceType::File,
            source_config: Default::default(),
            parser_config: Default::default(),
        };

        let _ = source_catalog
            .create_logical_source(logical_req)
            .await
            .unwrap();
        let _ = worker_catalog.create_worker(worker_req).await.unwrap();
        let _ = source_catalog
            .create_physical_source(physical_source_req)
            .await
            .unwrap();

        let get_req = GetPhysicalSource::new().with_logical_source("source".to_string());
        let rsp = source_catalog.get_physical_source(get_req).await.unwrap();
        assert_eq!(rsp.len(), 1);
        assert_eq!(rsp[0].logical_source, "source");
        assert_eq!(rsp[0].host_addr, test_host_addr());
        assert_eq!(rsp[0].source_type, SourceType::File);
    }

    async fn prop_logical_name_unique(db: State, create_source: CreateLogicalSource) {
        let catalog = SourceCatalog::from(db);
        let model = catalog
            .create_logical_source(create_source.clone())
            .await
            .expect("First logical source creation should succeed");

        assert_eq!(model.name, create_source.name);
        assert_eq!(model.schema, create_source.schema);

        assert!(
            catalog.create_logical_source(create_source).await.is_err(),
            "Duplicate logical source with name '{}' should be rejected",
            model.name
        );
    }

    async fn prop_physical_refs_exist(db: State, req: PhysicalSourceWithRefs) {
        let source_catalog = SourceCatalog::from(db.clone());
        let worker_catalog = test_worker_catalog(db);
        assert!(
            source_catalog
                .create_physical_source(req.physical.clone())
                .await
                .is_err(),
            "Physical source with missing refs should be rejected",
        );

        source_catalog
            .create_logical_source(req.logical)
            .await
            .expect("Logical source creation should succeed");

        assert!(
            source_catalog
                .create_physical_source(req.physical.clone())
                .await
                .is_err(),
            "Physical source with missing worker ref should be rejected",
        );

        worker_catalog
            .create_worker(req.worker)
            .await
            .expect("Worker creation should succeed");

        source_catalog
            .create_physical_source(req.physical)
            .await
            .expect("Physical source with valid refs should succeed");
    }

    async fn prop_drop_with_refs_fails(db: State, req: PhysicalSourceWithRefs) {
        let source_catalog = SourceCatalog::from(db.clone());
        let worker_catalog = test_worker_catalog(db);
        source_catalog
            .create_logical_source(req.logical.clone())
            .await
            .expect("Logical source creation should succeed");
        worker_catalog
            .create_worker(req.worker)
            .await
            .expect("Worker creation should succeed");
        source_catalog
            .create_physical_source(req.physical)
            .await
            .expect("Physical source creation should succeed");

        // Property: Cannot drop logical source while physical sources reference it
        let drop_request = DropLogicalSource {
            with_name: req.logical.name.clone(),
        };

        assert!(
            source_catalog
                .drop_logical_source(drop_request)
                .await
                .is_err(),
            "Should not be able to drop logical source '{}' while physical sources reference it",
            req.logical.name
        );
    }

    async fn prop_create_drop_create_logical(db: State, create_source: CreateLogicalSource) {
        let catalog = SourceCatalog::from(db);
        catalog
            .create_logical_source(create_source.clone())
            .await
            .expect("First create should succeed");

        let drop_request = DropLogicalSource {
            with_name: create_source.name.clone(),
        };

        catalog
            .drop_logical_source(drop_request)
            .await
            .expect("Drop should succeed");

        catalog
            .create_logical_source(create_source)
            .await
            .expect("Second create after drop should succeed");
    }

    proptest! {
        #[test]
        fn logical_name_unique(create_source: CreateLogicalSource) {
            test_prop(|db| async move {
                prop_logical_name_unique(db, create_source).await;
            });
        }

        #[test]
        fn physical_source_refs_exist(req in arb_physical_with_refs()) {
            test_prop(|db| async move {
                prop_physical_refs_exist(db, req).await;
            })
        }

        #[test]
        fn physical_source_drop_with_refs(req in arb_physical_with_refs()) {
            test_prop(|db| async move {
                prop_drop_with_refs_fails(db, req).await;
            })
        }

        #[test]
        fn create_drop_create_logical_succeeds(create_source: CreateLogicalSource) {
            test_prop(|db| async move {
                prop_create_drop_create_logical(db, create_source).await;
            })
        }
    }
}
