use crate::catalog::catalog_errors::CatalogError;
use crate::catalog::logical_source::DropLogicalSource;
use crate::catalog::physical_source::{CreatePhysicalSource, DropPhysicalSource, SourceType};
use crate::catalog::query::{CreateQuery, DropQuery};
use crate::catalog::sink::{DropSink, SinkType};
use crate::catalog::worker::DropWorker;
use crate::requests::{CreateLogicalSource, CreateSink, CreateWorker};
use sqlx::SqlitePool;
use std::env;

pub struct Catalog {
    pool: SqlitePool,
}

impl Catalog {
    pub async fn from(pool: SqlitePool) -> Result<Self, CatalogError> {
        sqlx::migrate!()
            .run(&pool)
            .await
            .map_err(|e| CatalogError::MigrationError {
                details: e.to_string(),
            })?;

        Ok(Catalog { pool })
    }

    pub async fn from_env() -> Result<Self, CatalogError> {
        let database_url = env::var("DATABASE_URL").map_err(|e| CatalogError::ConnectionError {
            reason: format!("DATABASE_URL not set: {}", e),
        })?;

        let pool = SqlitePool::connect(&database_url).await.map_err(|e| {
            CatalogError::ConnectionError {
                reason: e.to_string(),
            }
        })?;

        sqlx::migrate!()
            .run(&pool)
            .await
            .map_err(|e| CatalogError::MigrationError {
                details: e.to_string(),
            })?;

        Ok(Catalog { pool })
    }

    pub async fn create_logical_source(
        &self,
        logical: &CreateLogicalSource,
    ) -> Result<(), CatalogError> {
        let schema = sqlx::types::Json(&logical.schema);
        sqlx::query!(
            "INSERT INTO logical_sources (name, schema) VALUES (?, ?)",
            logical.source_name,
            schema as _,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn create_physical_source(
        &self,
        physical: &CreatePhysicalSource,
    ) -> Result<(), CatalogError> {
        let source_config = sqlx::types::Json(&physical.source_config);
        let parser_config = sqlx::types::Json(&physical.parser_config);

        sqlx::query!(
                "INSERT INTO physical_sources (logical_source, placement_host_name, placement_grpc_port, source_type, source_config, parser_config)
                 VALUES (?, ?, ?, ?, ?, ?)",
                physical.logical_source,
                physical.placement_host_name,
                physical.placement_grpc_port,
                physical.source_type,
                source_config as _,
                parser_config as _
            )
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn create_sink(&self, create_sink: &CreateSink) -> Result<(), CatalogError> {
        let sink_config = sqlx::types::Json(&create_sink.config);

        sqlx::query!(
            "INSERT INTO sinks (name, placement_host_name, placement_grpc_port, sink_type, config)
                 VALUES (?, ?, ?, ?, ?)",
            create_sink.name,
            create_sink.placement_host_name,
            create_sink.placement_grpc_port,
            create_sink.sink_type,
            sink_config as _,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn create_worker(&self, worker: &CreateWorker) -> Result<(), CatalogError> {
        let mut tx = self.pool.begin().await?;

        sqlx::query!(
            "INSERT INTO workers (host_name, grpc_port, data_port, capacity) VALUES (?, ?, ?, ?)",
            worker.host_name,
            worker.grpc_port,
            worker.data_port,
            worker.capacity
        )
        .execute(&mut *tx)
        .await?;

        for peer in &worker.peers {
            sqlx::query!(
                "INSERT INTO network_links (src_host_name, src_grpc_port, dst_host_name, dst_grpc_port) VALUES (?, ?, ?, ?)",
                worker.host_name,
                worker.grpc_port,
                peer.host(),
                peer.port()
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn create_query(&self, create_query: &CreateQuery) -> Result<(), CatalogError> {
        let mut tx = self.pool.begin().await?;

        sqlx::query!(
            "INSERT INTO queries (id, statement) VALUES (?, ?)",
            create_query.name,
            create_query.stmt
        )
        .execute(&mut *tx)
        .await?;

        for worker in &create_query.on_workers {
            let empty_plan = sqlx::types::Json(serde_json::json!({}));
            sqlx::query!(
                "INSERT INTO query_fragments (query_id, host_name, grpc_port, plan) VALUES (?, ?, ?, ?)",
                create_query.name,
                worker.host(),
                worker.port(),
                empty_plan as _
            )
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn drop_logical_source(
        &self,
        drop_logical: &DropLogicalSource,
    ) -> Result<(), CatalogError> {
        sqlx::query!(
            "DELETE FROM logical_sources WHERE name = ?",
            drop_logical.source_name
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn drop_physical_source(
        &self,
        drop_physical: &DropPhysicalSource,
    ) -> Result<(), CatalogError> {
        if drop_physical.with_logical_source.is_none()
            && drop_physical.on_worker.is_none()
            && drop_physical.with_type.is_none()
        {
            return Err(CatalogError::EmptyPredicate {});
        }

        let mut query = sqlx::QueryBuilder::new("DELETE FROM physical_sources WHERE 1=1");

        if let Some(logical) = &drop_physical.with_logical_source {
            query.push(" AND logical_source = ").push_bind(logical);
        }
        if let Some(worker) = &drop_physical.on_worker {
            query
                .push(" AND placement_host_name = ")
                .push_bind(worker.host());
            query
                .push(" AND placement_grpc_port = ")
                .push_bind(worker.port());
        }
        if let Some(source_type) = &drop_physical.with_type {
            query
                .push(" AND source_type = ")
                .push_bind(source_type.to_string());
        }

        query.build().execute(&self.pool).await?;
        Ok(())
    }

    pub async fn drop_sink(&self, drop_request: &DropSink) -> Result<(), CatalogError> {
        if drop_request.with_name.is_none()
            && drop_request.on_worker.is_none()
            && drop_request.with_type.is_none()
        {
            return Err(CatalogError::EmptyPredicate {});
        }

        let mut query = sqlx::QueryBuilder::new("DELETE FROM sinks WHERE 1=1");

        if let Some(name) = &drop_request.with_name {
            query.push(" AND name = ").push_bind(name);
        }
        if let Some(worker) = &drop_request.on_worker {
            query
                .push(" AND placement_host_name = ")
                .push_bind(worker.host());
            query
                .push(" AND placement_grpc_port = ")
                .push_bind(worker.port());
        }
        if let Some(sink_type) = &drop_request.with_type {
            query
                .push(" AND sink_type = ")
                .push_bind(sink_type.to_string());
        }

        query.build().execute(&self.pool).await?;
        Ok(())
    }

    pub async fn drop_query(&self, drop_request: &DropQuery) -> Result<(), CatalogError> {
        if drop_request.with_id.is_none()
            && drop_request.with_state.is_none()
            && drop_request.on_worker.is_none()
        {
            return Err(CatalogError::EmptyPredicate {});
        }

        let mut query = sqlx::QueryBuilder::new("DELETE FROM queries WHERE 1=1");

        if let Some(id) = &drop_request.with_id {
            query.push(" AND id = ").push_bind(id);
        }
        if let Some(state) = &drop_request.with_state {
            query.push(" AND current_state = ").push_bind(state);
        }
        if let Some(worker) = &drop_request.on_worker {
            query.push(" AND id IN (SELECT query_id FROM query_fragments WHERE host_name = ");
            query.push_bind(worker.host());
            query.push(" AND grpc_port = ");
            query.push_bind(worker.port());
            query.push(")");
        }

        query.build().execute(&self.pool).await?;
        Ok(())
    }

    pub async fn drop_worker(&self, drop_request: &DropWorker) -> Result<(), CatalogError> {
        sqlx::query!(
            "DELETE FROM workers WHERE host_name = ? AND grpc_port = ?",
            drop_request.host_name,
            drop_request.grpc_port
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod catalog_tests {
    use super::*;
    use crate::catalog::logical_source::DropLogicalSource;
    use crate::catalog::physical_source::SourceType;
    use crate::catalog::schema::{AttributeField, DataType, Schema};
    use crate::catalog::sink::SinkType;
    use crate::requests::{CreateLogicalSource, CreateSink, CreateWorker};
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::future::Future;
    use std::sync::OnceLock;
    use tokio::runtime::Runtime;

    static RUNTIME: OnceLock<Runtime> = OnceLock::new();

    fn get_runtime() -> &'static Runtime {
        RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_time()
                .enable_io()
                .build()
                .expect("Could not create tokio runtime")
        })
    }

    async fn run_async<F, Fut, T>(test_fn: F) -> T
    where
        F: FnOnce(Catalog) -> Fut,
        Fut: Future<Output = T>,
    {
        let catalog = Catalog::from(
            SqlitePoolOptions::new()
                .connect(":memory:")
                .await
                .expect("Could not create connection to database"),
        )
        .await
        .expect("Could not create catalog");

        test_fn(catalog).await
    }

    impl Arbitrary for DataType {
        fn arbitrary(g: &mut Gen) -> Self {
            use strum::IntoEnumIterator;
            let variants: Vec<DataType> = DataType::iter().collect();
            *g.choose(&variants).expect("choose value")
        }
    }

    impl Arbitrary for Schema {
        fn arbitrary(g: &mut Gen) -> Self {
            let size = usize::arbitrary(g) % 10 + 1;
            let fields: Vec<AttributeField> =
                (0..size).map(|_| AttributeField::arbitrary(g)).collect();
            Schema::from(fields)
        }
    }

    impl Arbitrary for CreateLogicalSource {
        fn arbitrary(g: &mut Gen) -> Self {
            CreateLogicalSource {
                source_name: String::arbitrary(g),
                schema: Schema::arbitrary(g),
            }
        }
    }

    impl Arbitrary for SourceType {
        fn arbitrary(g: &mut Gen) -> Self {
            use strum::IntoEnumIterator;
            let variants: Vec<SourceType> = SourceType::iter().collect();
            *g.choose(&variants).expect("choose value")
        }
    }

    impl Arbitrary for SinkType {
        fn arbitrary(g: &mut Gen) -> Self {
            use strum::IntoEnumIterator;
            let variants: Vec<SinkType> = SinkType::iter().collect();
            *g.choose(&variants).expect("choose value")
        }
    }

    impl Arbitrary for CreatePhysicalSource {
        fn arbitrary(g: &mut Gen) -> Self {
            CreatePhysicalSource {
                logical_source: String::arbitrary(g),
                placement_host_name: String::arbitrary(g),
                placement_grpc_port: u16::arbitrary(g),
                source_type: SourceType::arbitrary(g),
                source_config: Default::default(),
                parser_config: Default::default(),
            }
        }
    }

    impl Arbitrary for CreateWorker {
        fn arbitrary(g: &mut Gen) -> Self {
            let grpc = (u16::arbitrary(g) % 65535) + 1;
            let mut data = (u16::arbitrary(g) % 65535) + 1;

            if data == grpc {
                data = if grpc == 65535 { 1 } else { grpc + 1 }
            }
            CreateWorker {
                host_name: String::arbitrary(g),
                grpc_port: grpc,
                data_port: data,
                capacity: u32::arbitrary(g),
                peers: Vec::new(),
            }
        }
    }

    impl Arbitrary for CreateSink {
        fn arbitrary(g: &mut Gen) -> Self {
            CreateSink {
                name: String::arbitrary(g),
                placement_host_name: String::arbitrary(g),
                placement_grpc_port: (u16::arbitrary(g) % 65535) + 1,
                sink_type: SinkType::arbitrary(g),
                config: Default::default(),
            }
        }
    }

    #[derive(Debug, Clone)]
    struct CreatePhysicalSourceWithRefs {
        create_logical: CreateLogicalSource,
        create_worker: CreateWorker,
        create_physical: CreatePhysicalSource,
    }

    impl Arbitrary for CreatePhysicalSourceWithRefs {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut requests = CreatePhysicalSourceWithRefs {
                create_logical: CreateLogicalSource::arbitrary(g),
                create_worker: CreateWorker::arbitrary(g),
                create_physical: CreatePhysicalSource::arbitrary(g),
            };

            requests.create_physical.logical_source = requests.create_logical.source_name.clone();
            requests.create_physical.placement_host_name = requests.create_worker.host_name.clone();
            requests.create_physical.placement_grpc_port = requests.create_worker.grpc_port;
            requests
        }
    }

    #[derive(Debug, Clone)]
    struct CreateSinkWithRefs {
        create_sink: CreateSink,
        create_worker: CreateWorker,
    }

    impl Arbitrary for CreateSinkWithRefs {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut requests = CreateSinkWithRefs {
                create_sink: CreateSink::arbitrary(g),
                create_worker: CreateWorker::arbitrary(g),
            };

            requests.create_sink.placement_host_name = requests.create_worker.host_name.clone();
            requests.create_sink.placement_grpc_port = requests.create_worker.grpc_port;
            requests
        }
    }

    fn test_prop<F, Fut>(test_fn: F) -> bool
    where
        F: FnOnce(Catalog) -> Fut,
        Fut: Future<Output = ()>,
    {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            get_runtime().block_on(run_async(test_fn))
        }))
        .is_ok()
    }

    #[quickcheck]
    fn logical_name_is_unique(create_source: CreateLogicalSource) -> bool {
        test_prop(|catalog| async move {
            catalog
                .create_logical_source(&create_source)
                .await
                .expect("First logical source creation should succeed");

            assert!(
                catalog.create_logical_source(&create_source).await.is_err(),
                "Duplicate logical source name '{}' should be rejected",
                create_source.source_name
            );
        })
    }

    #[quickcheck]
    fn sink_name_is_unique(req: CreateSinkWithRefs) -> bool {
        test_prop(|catalog| async move {
            catalog
                .create_worker(&req.create_worker)
                .await
                .expect("Worker setup should succeed");

            catalog
                .create_sink(&req.create_sink)
                .await
                .expect("First sink creation should succeed");

            assert!(
                catalog.create_sink(&req.create_sink).await.is_err(),
                "Duplicate sink name '{}' should be rejected",
                req.create_sink.name
            );
        })
    }

    #[quickcheck]
    fn sink_worker_exists(create_worker: CreateWorker, mut create_sink: CreateSink) -> bool {
        test_prop(|catalog| async move {
            assert!(
                catalog.create_sink(&create_sink).await.is_err(),
                "CreateSink without prior worker creation should be rejected"
            );

            create_sink.placement_host_name = create_worker.host_name.clone();
            create_sink.placement_grpc_port = create_worker.grpc_port;
            catalog
                .create_worker(&create_worker)
                .await
                .expect("Worker setup should succeed");
            catalog
                .create_sink(&create_sink)
                .await
                .expect("CreateSink with valid worker ref should succeed");
        })
    }

    #[quickcheck]
    fn physical_source_refs_exist(req: CreatePhysicalSourceWithRefs) -> bool {
        test_prop(|catalog| async move {
            assert!(
                catalog
                    .create_physical_source(&req.create_physical)
                    .await
                    .is_err(),
                "Physical source with missing refs should be rejected",
            );

            catalog
                .create_logical_source(&req.create_logical)
                .await
                .expect("Logical source creation should succeed");

            assert!(
                catalog
                    .create_physical_source(&req.create_physical)
                    .await
                    .is_err(),
                "Physical source with missing worker ref should be rejected",
            );

            catalog
                .create_worker(&req.create_worker)
                .await
                .expect("Worker creation should succeed");

            catalog
                .create_physical_source(&req.create_physical)
                .await
                .expect("Physical source with valid refs should succeed");
        })
    }

    #[quickcheck]
    fn logical_source_drop_with_references_fails(create_req: CreatePhysicalSourceWithRefs) -> bool {
        test_prop(|catalog| async move {
            catalog
                .create_logical_source(&create_req.create_logical)
                .await
                .expect("CreateLogicalSource should succeed");
            catalog
                .create_worker(&create_req.create_worker)
                .await
                .expect("CreateWorker should succeed");
            catalog
                .create_physical_source(&create_req.create_physical)
                .await
                .expect("CreatePhysicalSource should succeed");

            // Property: Cannot drop logical source while physical sources reference it
            let drop_request = DropLogicalSource {
                source_name: create_req.create_logical.source_name.clone(),
            };

            assert!(
                catalog.drop_logical_source(&drop_request).await.is_err(),
                "Should not be able to drop logical source '{}' while physical sources reference it",
                create_req.create_logical.source_name
            );
        })
    }

    // ============================================================================
    // Test 3: Cascade/Cleanup Prevention Tests
    // ============================================================================

    #[quickcheck]
    fn worker_drop_with_physical_sources_fails(create_req: CreatePhysicalSourceWithRefs) -> bool {
        test_prop(|catalog| async move {
            catalog
                .create_logical_source(&create_req.create_logical)
                .await
                .expect("CreateLogicalSource should succeed");
            catalog
                .create_worker(&create_req.create_worker)
                .await
                .expect("CreateWorker should succeed");
            catalog
                .create_physical_source(&create_req.create_physical)
                .await
                .expect("CreatePhysicalSource should succeed");

            // Property: Cannot drop worker while physical sources reference it
            let drop_request = DropWorker {
                host_name: create_req.create_worker.host_name.clone(),
                grpc_port: create_req.create_worker.grpc_port,
            };

            assert!(
                catalog.drop_worker(&drop_request).await.is_err(),
                "Should not be able to drop worker '{}:{}' while physical sources reference it",
                create_req.create_worker.host_name,
                create_req.create_worker.grpc_port
            );
        })
    }

    #[quickcheck]
    fn worker_drop_with_sinks_fails(create_req: CreateSinkWithRefs) -> bool {
        test_prop(|catalog| async move {
            catalog
                .create_worker(&create_req.create_worker)
                .await
                .expect("CreateWorker should succeed");
            catalog
                .create_sink(&create_req.create_sink)
                .await
                .expect("CreateSink should succeed");

            // Property: Cannot drop worker while sinks reference it
            let drop_request = DropWorker {
                host_name: create_req.create_worker.host_name.clone(),
                grpc_port: create_req.create_worker.grpc_port,
            };

            assert!(
                catalog.drop_worker(&drop_request).await.is_err(),
                "Should not be able to drop worker '{}:{}' while sinks reference it",
                create_req.create_worker.host_name,
                create_req.create_worker.grpc_port
            );
        })
    }

    // ============================================================================
    // Test 5: Idempotency Tests
    // ============================================================================

    #[quickcheck]
    fn create_drop_create_logical_source_succeeds(create_source: CreateLogicalSource) -> bool {
        test_prop(|catalog| async move {
            // Property: Create-Drop-Create sequence is idempotent
            catalog
                .create_logical_source(&create_source)
                .await
                .expect("First create should succeed");

            let drop_request = DropLogicalSource {
                source_name: create_source.source_name.clone(),
            };

            catalog
                .drop_logical_source(&drop_request)
                .await
                .expect("Drop should succeed");

            catalog
                .create_logical_source(&create_source)
                .await
                .expect("Second create after drop should succeed");
        })
    }

    #[quickcheck]
    fn create_drop_create_worker_succeeds(create_worker: CreateWorker) -> bool {
        test_prop(|catalog| async move {
            // Property: Create-Drop-Create sequence is idempotent
            catalog
                .create_worker(&create_worker)
                .await
                .expect("First create should succeed");

            let drop_request = DropWorker {
                host_name: create_worker.host_name.clone(),
                grpc_port: create_worker.grpc_port,
            };

            catalog
                .drop_worker(&drop_request)
                .await
                .expect("Drop should succeed");

            catalog
                .create_worker(&create_worker)
                .await
                .expect("Second create after drop should succeed");
        })
    }

    #[quickcheck]
    fn drop_nonexistent_logical_source_succeeds(source_name: String) -> bool {
        test_prop(|catalog| async move {
            // Property: Dropping nonexistent entities is safe
            let drop_request = DropLogicalSource {
                source_name: source_name.clone(),
            };

            catalog
                .drop_logical_source(&drop_request)
                .await
                .expect("Dropping nonexistent logical source should succeed");
        })
    }

    #[quickcheck]
    fn drop_nonexistent_worker_succeeds(host_name: String, grpc_port: u16) -> bool {
        test_prop(|catalog| async move {
            // Property: Dropping nonexistent entities is safe
            let drop_request = DropWorker {
                host_name,
                grpc_port,
            };

            catalog
                .drop_worker(&drop_request)
                .await
                .expect("Dropping nonexistent worker should succeed");
        })
    }

    // ============================================================================
    // Test 6: JSON Schema Validation
    // ============================================================================

    #[quickcheck]
    fn logical_source_schema_is_valid_json(create_source: CreateLogicalSource) -> bool {
        test_prop(|catalog| async move {
            catalog
                .create_logical_source(&create_source)
                .await
                .expect("Create should succeed");

            // Property: Schema is stored as valid JSON object
            let result = sqlx::query!(
                "SELECT schema FROM logical_sources WHERE name = ?",
                create_source.source_name
            )
            .fetch_one(&catalog.pool)
            .await
            .expect("Should be able to query logical source");

            let schema_json: serde_json::Value = serde_json::from_str(&result.schema)
                .expect("Schema should be valid JSON");

            assert!(
                schema_json.is_object(),
                "Schema should be a JSON object, got: {:?}",
                schema_json
            );
        })
    }

    #[quickcheck]
    fn physical_source_configs_are_valid_json(create_req: CreatePhysicalSourceWithRefs) -> bool {
        test_prop(|catalog| async move {
            catalog
                .create_logical_source(&create_req.create_logical)
                .await
                .expect("CreateLogicalSource should succeed");
            catalog
                .create_worker(&create_req.create_worker)
                .await
                .expect("CreateWorker should succeed");
            catalog
                .create_physical_source(&create_req.create_physical)
                .await
                .expect("CreatePhysicalSource should succeed");

            // Property: Source and parser configs are stored as valid JSON
            let result = sqlx::query!(
                "SELECT source_config, parser_config FROM physical_sources WHERE logical_source = ?",
                create_req.create_physical.logical_source
            )
            .fetch_one(&catalog.pool)
            .await
            .expect("Should be able to query physical source");

            let source_config: serde_json::Value = serde_json::from_str(&result.source_config)
                .expect("Source config should be valid JSON");
            let parser_config: serde_json::Value = serde_json::from_str(&result.parser_config)
                .expect("Parser config should be valid JSON");

            assert!(
                source_config.is_object(),
                "Source config should be a JSON object"
            );
            assert!(
                parser_config.is_object(),
                "Parser config should be a JSON object"
            );
        })
    }

    #[quickcheck]
    fn sink_config_is_valid_json(create_req: CreateSinkWithRefs) -> bool {
        test_prop(|catalog| async move {
            catalog
                .create_worker(&create_req.create_worker)
                .await
                .expect("CreateWorker should succeed");
            catalog
                .create_sink(&create_req.create_sink)
                .await
                .expect("CreateSink should succeed");

            // Property: Sink config is stored as valid JSON
            let result = sqlx::query!(
                "SELECT config FROM sinks WHERE name = ?",
                create_req.create_sink.name
            )
            .fetch_one(&catalog.pool)
            .await
            .expect("Should be able to query sink");

            let config: serde_json::Value = serde_json::from_str(&result.config)
                .expect("Sink config should be valid JSON");

            assert!(
                config.is_object(),
                "Sink config should be a JSON object"
            );
        })
    }
}
