use crate::catalog::catalog_errors::CatalogError;
use crate::catalog::logical_source::DropLogicalSource;
use crate::catalog::physical_source::{CreatePhysicalSource, DropPhysicalSource, SourceType};
use crate::catalog::sink::{DropSink, SinkType};
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
                "INSERT INTO physical_sources (logical_source, placement, source_type, source_config, parser_config)
                 VALUES (?, ?, ?, ?, ?)",
                physical.logical_source,
                physical.placement,
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
            "INSERT INTO sinks (name, placement, sink_type, config)
                 VALUES (?, ?, ?, ?)",
            create_sink.name,
            create_sink.placement,
            create_sink.sink_type,
            sink_config as _,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn create_workers(&self, workers: &[CreateWorker]) -> Result<(), CatalogError> {
        let mut tx = self.pool.begin().await?;

        for worker in workers {
            sqlx::query!(
                "INSERT INTO workers (host_name, grpc_port, data_port, capacity) VALUES (?, ?, ?, ?)",
                worker.host_name,
                worker.grpc_port,
                worker.data_port,
                worker.num_slots
            )
            .execute(&mut *tx)
            .await?;
        }

        for worker in workers {
            if !worker.peers.is_empty() {
                let mut query_builder = sqlx::QueryBuilder::new(
                    "INSERT INTO network_links (src_host_name, dst_host_name) VALUES ",
                );

                query_builder.push_values(worker.peers.iter(), |mut b, peer| {
                    b.push_bind(&worker.host_name).push_bind(peer);
                });

                query_builder.build().execute(&mut *tx).await?;
            }
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn drop_logical_source(
        &self,
        drop_logical: DropLogicalSource,
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
            && drop_physical.on_node.is_none()
            && drop_physical.by_type.is_none()
        {
            return Err(CatalogError::EmptyPredicate {});
        }

        let mut query = sqlx::QueryBuilder::new("DELETE FROM physical_sources WHERE 1=1");

        if let Some(logical) = &drop_physical.with_logical_source {
            query.push(" AND logical_source = ").push_bind(logical);
        }
        if let Some(node) = &drop_physical.on_node {
            query.push(" AND placement = ").push_bind(node);
        }
        if let Some(source_type) = &drop_physical.by_type {
            query
                .push(" AND source_type = ")
                .push_bind(source_type.to_string());
        }

        query.build().execute(&self.pool).await?;
        Ok(())
    }

    pub async fn drop_sink(&self, drop_request: &DropSink) -> Result<(), CatalogError> {
        if drop_request.with_name.is_none()
            && drop_request.on_node.is_none()
            && drop_request.with_type.is_none()
        {
            return Err(CatalogError::EmptyPredicate {});
        }

        let mut query = sqlx::QueryBuilder::new("DELETE FROM sinks WHERE 1=1");

        if let Some(name) = &drop_request.with_name {
            query.push(" AND name = ").push_bind(name);
        }
        if let Some(node) = &drop_request.on_node {
            query.push(" AND placement = ").push_bind(node);
        }
        if let Some(sink_type) = &drop_request.with_type {
            query
                .push(" AND sink_type = ")
                .push_bind(sink_type.to_string());
        }

        query.build().execute(&self.pool).await?;
        Ok(())
    }
}

#[cfg(test)]
impl Catalog {
    async fn get_source_types(&self) -> Result<Vec<SourceType>, CatalogError> {
        let rows = sqlx::query_scalar::<_, SourceType>("SELECT type FROM source_types")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }

    async fn get_sink_types(&self) -> Result<Vec<SinkType>, CatalogError> {
        let rows = sqlx::query_scalar::<_, SinkType>("SELECT type FROM sink_types")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }
}

#[cfg(test)]
mod catalog_tests {
    use super::*;
    use crate::catalog::logical_source::DropLogicalSource;
    use crate::catalog::physical_source::SourceType;
    use crate::catalog::schema::{AttributeField, DataType, Schema};
    use crate::catalog::sink::SinkType;
    use crate::requests::{
        CreateLogicalSource, CreateSink, CreateWorker,
    };
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
                placement: String::arbitrary(g),
                source_type: SourceType::arbitrary(g),
                source_config: Default::default(),
                parser_config: Default::default(),
            }
        }
    }

    impl Arbitrary for CreateWorker {
        fn arbitrary(g: &mut Gen) -> Self {
            CreateWorker {
                host_name: String::arbitrary(g),
                grpc_port: u16::arbitrary(g),
                data_port: u16::arbitrary(g),
                num_slots: u32::arbitrary(g),
                peers: Vec::new(),
            }
        }
    }

    impl Arbitrary for CreateSink {
        fn arbitrary(g: &mut Gen) -> Self {
            CreateSink {
                name: String::arbitrary(g),
                placement: String::arbitrary(g),
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
            requests.create_physical.placement = requests.create_worker.host_name.clone();
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

            requests.create_sink.placement = requests.create_worker.host_name.clone();
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
                .create_workers(&[req.create_worker])
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

            create_sink.placement = create_worker.host_name.clone();
            catalog
                .create_workers(&[create_worker])
                .await
                .expect("Worker setup should succeed");
            catalog
                .create_sink(&create_sink)
                .await
                .expect("CreateSink with valid worker ref should succeed");
        })
    }

    #[quickcheck]
    fn physical_source_refs_exist(
        req: CreatePhysicalSourceWithRefs,
    ) -> bool {
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
                .create_workers(&[req.create_worker])
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
                .create_workers(&[create_req.create_worker.clone()])
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
                catalog.drop_logical_source(drop_request).await.is_err(),
                "Should not be able to drop logical source '{}' while physical sources reference it",
                create_req.create_logical.source_name
            );
        })
    }

    #[sqlx::test]
    async fn source_types_sync(pool: SqlitePool) {
        use std::collections::HashSet;
        use strum::IntoEnumIterator;

        let catalog = Catalog::from(pool)
            .await
            .expect("Failed to create catalog");

        let db_source_types = catalog
            .get_source_types()
            .await
            .expect("All DB source types should be valid Rust enum variants");

        let code_variants: HashSet<SourceType> = SourceType::iter().collect();
        let tbl_variants: HashSet<SourceType> = db_source_types.into_iter().collect();

        assert_eq!(
            code_variants, tbl_variants,
            "The static table 'source_types' is not in sync with its code counterpart"
        );
    }

    #[sqlx::test]
    async fn sink_types_sync(pool: SqlitePool) {
        use std::collections::HashSet;
        use strum::IntoEnumIterator;

        let catalog = Catalog::from(pool)
            .await
            .expect("Failed to create catalog");

        let db_sink_types = catalog
            .get_sink_types()
            .await
            .expect("All DB sink types should be valid Rust enum variants");

        let code_variants: HashSet<SinkType> = SinkType::iter().collect();
        let tbl_variants: HashSet<SinkType> = db_sink_types.into_iter().collect();

        assert_eq!(
            code_variants, tbl_variants,
            "The static table 'sink_types' is not in sync with its code counterpart"
        );
    }
}
