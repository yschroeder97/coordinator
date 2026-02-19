use crate::database::Database;
use anyhow::Result;
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

pub struct SourceCatalog {
    db: Database,
}

impl SourceCatalog {
    pub fn from(db: Database) -> Arc<Self> {
        Arc::new(Self { db })
    }

    pub async fn create_logical_source(
        &self,
        req: CreateLogicalSource,
    ) -> Result<logical_source::Model> {
        Ok(logical_source::ActiveModel::from(req)
            .insert(&self.db.conn)
            .await?)
    }

    pub async fn get_logical_source(
        &self,
        req: GetLogicalSource,
    ) -> Result<Option<logical_source::Model>> {
        Ok(LogicalSourceEntity::find_by_id(req.with_name)
            .one(&self.db.conn)
            .await?)
    }

    pub async fn drop_logical_source(
        &self,
        req: DropLogicalSource,
    ) -> Result<Option<logical_source::Model>> {
        Ok(LogicalSourceEntity::delete_by_id(req.with_name)
            .exec_with_returning(&self.db.conn)
            .await?)
    }

    pub async fn create_physical_source(
        &self,
        req: CreatePhysicalSource,
    ) -> Result<physical_source::Model> {
        Ok(physical_source::ActiveModel::from(req)
            .insert(&self.db.conn)
            .await?)
    }

    pub async fn get_physical_source(
        &self,
        req: GetPhysicalSource,
    ) -> Result<Vec<physical_source::Model>> {
        Ok(PhysicalSourceEntity::find()
            .filter(req.into_condition())
            .all(&self.db.conn)
            .await?)
    }

    pub async fn drop_physical_source(
        &self,
        req: DropPhysicalSource,
    ) -> Result<Vec<physical_source::Model>> {
        Ok(PhysicalSourceEntity::delete_many()
            .filter(req.into_condition())
            .exec_with_returning(&self.db.conn)
            .await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Catalog;
    use crate::test_utils::{test_grpc_addr, test_host_addr, test_prop};
    use model::source::physical_source::SourceType;
    use model::source::schema::{DataType, Schema};
    use model::testing::{PhysicalSourceWithRefs, arb_physical_with_refs};
    use model::worker::CreateWorker;
    use proptest::proptest;

    #[tokio::test]
    async fn test_get_logical_source() {
        let catalog = SourceCatalog::from(Database::for_test().await);

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
        let catalog = Catalog::for_test().await;

        let worker_req1 = CreateWorker::new(test_host_addr(), test_grpc_addr(), -1);
        let mut worker_req2 = worker_req1.clone();
        worker_req2.capacity = 0;

        assert!(
            catalog.worker.create_worker(worker_req1).await.is_err(),
            "Cannot create worker with negative capacity"
        );
        assert!(
            catalog.worker.create_worker(worker_req2).await.is_ok(),
            "Worker with capacity of zero is allowed"
        );
    }

    #[tokio::test]
    async fn test_get_physical_source() {
        let catalog = Catalog::for_test().await;

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

        let _ = catalog
            .source
            .create_logical_source(logical_req)
            .await
            .unwrap();
        let _ = catalog.worker.create_worker(worker_req).await.unwrap();
        let _ = catalog
            .source
            .create_physical_source(physical_source_req)
            .await
            .unwrap();

        let get_req = GetPhysicalSource::new().with_logical_source("source".to_string());
        let rsp = catalog.source.get_physical_source(get_req).await.unwrap();
        assert_eq!(rsp.len(), 1);
        assert_eq!(rsp[0].logical_source, "source");
        assert_eq!(rsp[0].host_addr, test_host_addr());
        assert_eq!(rsp[0].source_type, SourceType::File);
    }

    async fn prop_logical_name_unique(create_source: CreateLogicalSource) {
        let catalog = Catalog::for_test().await;
        let model = catalog
            .source
            .create_logical_source(create_source.clone())
            .await
            .expect("First logical source creation should succeed");

        assert_eq!(model.name, create_source.name);
        assert_eq!(model.schema, create_source.schema);

        assert!(
            catalog
                .source
                .create_logical_source(create_source)
                .await
                .is_err(),
            "Duplicate logical source with name '{}' should be rejected",
            model.name
        );
    }

    async fn prop_physical_refs_exist(req: PhysicalSourceWithRefs) {
        let catalog = Catalog::for_test().await;
        assert!(
            catalog
                .source
                .create_physical_source(req.physical.clone())
                .await
                .is_err(),
            "Physical source with missing refs should be rejected",
        );

        catalog
            .source
            .create_logical_source(req.logical)
            .await
            .expect("Logical source creation should succeed");

        assert!(
            catalog
                .source
                .create_physical_source(req.physical.clone())
                .await
                .is_err(),
            "Physical source with missing worker ref should be rejected",
        );

        catalog
            .worker
            .create_worker(req.worker)
            .await
            .expect("Worker creation should succeed");

        catalog
            .source
            .create_physical_source(req.physical)
            .await
            .expect("Physical source with valid refs should succeed");
    }

    async fn prop_drop_with_refs_fails(req: PhysicalSourceWithRefs) {
        let catalog = Catalog::for_test().await;
        catalog
            .source
            .create_logical_source(req.logical.clone())
            .await
            .expect("Logical source creation should succeed");
        catalog
            .worker
            .create_worker(req.worker)
            .await
            .expect("Worker creation should succeed");
        catalog
            .source
            .create_physical_source(req.physical)
            .await
            .expect("Physical source creation should succeed");

        // Property: Cannot drop logical source while physical sources reference it
        let drop_request = DropLogicalSource {
            with_name: req.logical.name.clone(),
        };

        assert!(
            catalog
                .source
                .drop_logical_source(drop_request)
                .await
                .is_err(),
            "Should not be able to drop logical source '{}' while physical sources reference it",
            req.logical.name
        );
    }

    async fn prop_create_drop_create_logical(create_source: CreateLogicalSource) {
        let catalog = Catalog::for_test().await;
        catalog
            .source
            .create_logical_source(create_source.clone())
            .await
            .expect("First create should succeed");

        let drop_request = DropLogicalSource {
            with_name: create_source.name.clone(),
        };

        catalog
            .source
            .drop_logical_source(drop_request)
            .await
            .expect("Drop should succeed");

        catalog
            .source
            .create_logical_source(create_source)
            .await
            .expect("Second create after drop should succeed");
    }

    proptest! {
        #[test]
        fn logical_name_unique(create_source: CreateLogicalSource) {
            test_prop(|| async move {
                prop_logical_name_unique(create_source).await;
            });
        }

        #[test]
        fn physical_source_refs_exist(req in arb_physical_with_refs()) {
            test_prop(|| async move {
                prop_physical_refs_exist(req).await;
            })
        }

        #[test]
        fn physical_source_drop_with_refs(req in arb_physical_with_refs()) {
            test_prop(|| async move {
                prop_drop_with_refs_fails(req).await;
            })
        }

        #[test]
        fn create_drop_create_logical_succeeds(create_source: CreateLogicalSource) {
            test_prop(|| async move {
                prop_create_drop_create_logical(create_source).await;
            })
        }
    }
}
