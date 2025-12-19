use std::env;
use std::sync::Arc;

use crate::catalog::catalog_errors::CatalogErr;
use crate::catalog::database::Database;
use crate::catalog::{SinkCatalog, SourceCatalog, WorkerCatalog, QueryCatalog};

pub struct Catalog {
    pub source: Arc<SourceCatalog>,
    pub sink: Arc<SinkCatalog>,
    pub worker: Arc<WorkerCatalog>,
    pub query: Arc<QueryCatalog>,
}

impl Catalog {
    fn from_db(db: Arc<Database>) -> Self {
        Self {
            source: Arc::new(SourceCatalog::new(db.clone())),
            sink: Arc::new(SinkCatalog::new(db.clone())),
            worker: Arc::new(WorkerCatalog::new(db.clone())),
            query: Arc::new(QueryCatalog::new(db.clone())),
        }
    }

    pub async fn from_env() -> Result<Self, CatalogErr> {
        let database_url = env::var("DATABASE_URL").map_err(|e| CatalogErr::ConnectionError {
            reason: format!("DATABASE_URL not set: {}", e),
        })?;

        let pool = sqlx::SqlitePool::connect(&database_url)
            .await
            .map_err(|e| CatalogErr::ConnectionError {
                reason: e.to_string(),
            })?;

        let db = Database::from_pool(pool).await.map_err(|e| CatalogErr::MigrationError {
            details: e.to_string(),
        })?;

        Ok(Self::from_db(db))
    }

    pub async fn from_pool(pool: sqlx::SqlitePool) -> Result<Self, CatalogErr> {
        let db = Database::from_pool(pool).await.map_err(|e| CatalogErr::MigrationError {
            details: e.to_string(),
        })?;

        Ok(Self::from_db(db))
    }

    pub fn source_catalog(&self) -> Arc<SourceCatalog> {
        self.source.clone()
    }

    pub fn worker_catalog(&self) -> Arc<WorkerCatalog> {
        self.worker.clone()
    }

    pub fn query_catalog(&self) -> Arc<QueryCatalog> {
        self.query.clone()
    }

    pub fn sink_catalog(&self) -> Arc<SinkCatalog> {
        self.sink.clone()
    }
}

// #[cfg(test)]
// mod catalog_tests {
//     use super::*;
//     use crate::catalog::logical_source::DropLogicalSource;
//     use crate::catalog::physical_source::SourceType;
//     use crate::catalog::schema::{AttributeField, DataType, Schema};
//     use crate::catalog::sink::SinkType;
//     use crate::catalog::worker::GrpcAddr;
//     use crate::requests::{CreateLogicalSource, CreatePhysicalSource, CreateSink, CreateWorker};
//     use quickcheck::{Arbitrary, Gen};
//     use quickcheck_macros::quickcheck;
//     use sqlx::sqlite::SqlitePoolOptions;
//     use std::future::Future;
//     use std::sync::OnceLock;
//     use tokio::runtime::Runtime;
// 
//     static RUNTIME: OnceLock<Runtime> = OnceLock::new();
// 
//     fn get_runtime() -> &'static Runtime {
//         RUNTIME.get_or_init(|| {
//             #[cfg(not(madsim))]
//             {
//                 tokio::runtime::Builder::new_multi_thread()
//                     .worker_threads(1)
//                     .enable_time()
//                     .enable_io()
//                     .build()
//                     .expect("Could not create tokio runtime")
//             }
//             #[cfg(madsim)]
//             {
//                 tokio::runtime::Builder::new_multi_thread()
//                     .worker_threads(1)
//                     .build()
//                     .expect("Could not create tokio runtime")
//             }
//         })
//     }
// 
//     async fn run_async<Fn, Fut, O>(test_fn: Fn) -> O
//     where
//         Fn: FnOnce(Arc<Catalog>) -> Fut,
//         Fut: Future<Output = O>,
//     {
//         let catalog = Catalog::from_pool(
//             SqlitePoolOptions::new()
//                 .connect(":memory:")
//                 .await
//                 .expect("Could not create connection to database"),
//         )
//             .await
//             .expect("Could not create catalog");
// 
//         test_fn(catalog).await
//     }
// 
//     impl Arbitrary for GrpcAddr {
//         fn arbitrary(g: &mut Gen) -> Self {
//             GrpcAddr::new(String::arbitrary(g), (u16::arbitrary(g) % 65535) + 1)
//         }
//     }
// 
//     impl Arbitrary for DataType {
//         fn arbitrary(g: &mut Gen) -> Self {
//             use strum::IntoEnumIterator;
//             let variants: Vec<DataType> = DataType::iter().collect();
//             *g.choose(&variants).expect("choose value")
//         }
//     }
// 
//     impl Arbitrary for Schema {
//         fn arbitrary(g: &mut Gen) -> Self {
//             let size = usize::arbitrary(g) % 10 + 1;
//             let fields: Vec<AttributeField> =
//                 (0..size).map(|_| AttributeField::arbitrary(g)).collect();
//             Schema::from(fields)
//         }
//     }
// 
//     impl Arbitrary for CreateLogicalSource {
//         fn arbitrary(g: &mut Gen) -> Self {
//             CreateLogicalSource {
//                 source_name: String::arbitrary(g),
//                 schema: Schema::arbitrary(g),
//             }
//         }
//     }
// 
//     impl Arbitrary for SourceType {
//         fn arbitrary(g: &mut Gen) -> Self {
//             use strum::IntoEnumIterator;
//             let variants: Vec<SourceType> = SourceType::iter().collect();
//             *g.choose(&variants).expect("choose value")
//         }
//     }
// 
//     impl Arbitrary for SinkType {
//         fn arbitrary(g: &mut Gen) -> Self {
//             use strum::IntoEnumIterator;
//             let variants: Vec<SinkType> = SinkType::iter().collect();
//             *g.choose(&variants).expect("choose value")
//         }
//     }
// 
//     impl Arbitrary for CreatePhysicalSource {
//         fn arbitrary(g: &mut Gen) -> Self {
//             CreatePhysicalSource {
//                 logical_source: String::arbitrary(g),
//                 placement_host_name: String::arbitrary(g),
//                 placement_grpc_port: u16::arbitrary(g),
//                 source_type: SourceType::arbitrary(g),
//                 source_config: Default::default(),
//                 parser_config: Default::default(),
//             }
//         }
//     }
// 
//     impl Arbitrary for CreateWorker {
//         fn arbitrary(g: &mut Gen) -> Self {
//             let grpc = (u16::arbitrary(g) % 65535) + 1;
//             let mut data = (u16::arbitrary(g) % 65535) + 1;
// 
//             if data == grpc {
//                 data = if grpc == 65535 { 1 } else { grpc + 1 }
//             }
//             CreateWorker {
//                 host_name: String::arbitrary(g),
//                 grpc_port: grpc,
//                 data_port: data,
//                 capacity: u32::arbitrary(g),
//                 peers: Vec::new(),
//             }
//         }
//     }
// 
//     impl Arbitrary for CreateSink {
//         fn arbitrary(g: &mut Gen) -> Self {
//             CreateSink {
//                 name: String::arbitrary(g),
//                 placement_host_name: String::arbitrary(g),
//                 placement_grpc_port: (u16::arbitrary(g) % 65535) + 1,
//                 sink_type: SinkType::arbitrary(g),
//                 config: Default::default(),
//             }
//         }
//     }
// 
//     #[derive(Debug, Clone)]
//     struct CreatePhysicalSourceWithRefs {
//         create_logical: CreateLogicalSource,
//         create_worker: CreateWorker,
//         create_physical: CreatePhysicalSource,
//     }
// 
//     impl Arbitrary for CreatePhysicalSourceWithRefs {
//         fn arbitrary(g: &mut Gen) -> Self {
//             let mut requests = CreatePhysicalSourceWithRefs {
//                 create_logical: CreateLogicalSource::arbitrary(g),
//                 create_worker: CreateWorker::arbitrary(g),
//                 create_physical: CreatePhysicalSource::arbitrary(g),
//             };
// 
//             requests.create_physical.logical_source = requests.create_logical.source_name.clone();
//             requests.create_physical.placement_host_name = requests.create_worker.host_name.clone();
//             requests.create_physical.placement_grpc_port = requests.create_worker.grpc_port;
//             requests
//         }
//     }
// 
//     #[derive(Debug, Clone)]
//     struct CreateSinkWithRefs {
//         create_sink: CreateSink,
//         create_worker: CreateWorker,
//     }
// 
//     impl Arbitrary for CreateSinkWithRefs {
//         fn arbitrary(g: &mut Gen) -> Self {
//             let mut requests = CreateSinkWithRefs {
//                 create_sink: CreateSink::arbitrary(g),
//                 create_worker: CreateWorker::arbitrary(g),
//             };
// 
//             requests.create_sink.placement_host_name = requests.create_worker.host_name.clone();
//             requests.create_sink.placement_grpc_port = requests.create_worker.grpc_port;
//             requests
//         }
//     }
// 
//     fn test_prop<F, Fut>(test_fn: F) -> bool
//     where
//         F: FnOnce(Arc<Catalog>) -> Fut,
//         Fut: Future<Output = ()>,
//     {
//         std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
//             get_runtime().block_on(run_async(test_fn))
//         }))
//             .is_ok()
//     }
// 
//     #[quickcheck]
//     fn logical_name_is_unique(create_source: CreateLogicalSource) -> bool {
//         test_prop(|catalog| async move {
//             catalog
//                 .create_logical_source(&create_source)
//                 .await
//                 .expect("First logical source creation should succeed");
// 
//             assert!(
//                 catalog.create_logical_source(&create_source).await.is_err(),
//                 "Duplicate logical source name '{}' should be rejected",
//                 create_source.source_name
//             );
//         })
//     }
// 
//     #[quickcheck]
//     fn sink_name_is_unique(req: CreateSinkWithRefs) -> bool {
//         test_prop(|catalog| async move {
//             catalog
//                 .create_worker(&req.create_worker)
//                 .await
//                 .expect("Worker setup should succeed");
// 
//             catalog
//                 .create_sink(&req.create_sink)
//                 .await
//                 .expect("First sink creation should succeed");
// 
//             assert!(
//                 catalog.create_sink(&req.create_sink).await.is_err(),
//                 "Duplicate sink name '{}' should be rejected",
//                 req.create_sink.name
//             );
//         })
//     }
// 
//     #[quickcheck]
//     fn sink_worker_exists(create_worker: CreateWorker, mut create_sink: CreateSink) -> bool {
//         test_prop(|catalog| async move {
//             assert!(
//                 catalog.create_sink(&create_sink).await.is_err(),
//                 "CreateSink without prior worker creation should be rejected"
//             );
// 
//             create_sink.placement_host_name = create_worker.host_name.clone();
//             create_sink.placement_grpc_port = create_worker.grpc_port;
//             catalog
//                 .create_worker(&create_worker)
//                 .await
//                 .expect("Worker setup should succeed");
//             catalog
//                 .create_sink(&create_sink)
//                 .await
//                 .expect("CreateSink with valid worker ref should succeed");
//         })
//     }
// 
//     #[quickcheck]
//     fn physical_source_refs_exist(req: CreatePhysicalSourceWithRefs) -> bool {
//         test_prop(|catalog| async move {
//             assert!(
//                 catalog
//                     .create_physical_source(&req.create_physical)
//                     .await
//                     .is_err(),
//                 "Physical source with missing refs should be rejected",
//             );
// 
//             catalog
//                 .create_logical_source(&req.create_logical)
//                 .await
//                 .expect("Logical source creation should succeed");
// 
//             assert!(
//                 catalog
//                     .create_physical_source(&req.create_physical)
//                     .await
//                     .is_err(),
//                 "Physical source with missing worker ref should be rejected",
//             );
// 
//             catalog
//                 .create_worker(&req.create_worker)
//                 .await
//                 .expect("Worker creation should succeed");
// 
//             catalog
//                 .create_physical_source(&req.create_physical)
//                 .await
//                 .expect("Physical source with valid refs should succeed");
//         })
//     }
// 
//     #[quickcheck]
//     fn logical_source_drop_with_references_fails(create_req: CreatePhysicalSourceWithRefs) -> bool {
//         test_prop(|catalog| async move {
//             catalog
//                 .create_logical_source(&create_req.create_logical)
//                 .await
//                 .expect("CreateLogicalSource should succeed");
//             catalog
//                 .create_worker(&create_req.create_worker)
//                 .await
//                 .expect("CreateWorker should succeed");
//             catalog
//                 .create_physical_source(&create_req.create_physical)
//                 .await
//                 .expect("CreatePhysicalSource should succeed");
// 
//             // Property: Cannot drop logical source while physical sources reference it
//             let drop_request = DropLogicalSource {
//                 source_name: create_req.create_logical.source_name.clone(),
//             };
// 
//             assert!(
//                 catalog.drop_logical_source(&drop_request).await.is_err(),
//                 "Should not be able to drop logical source '{}' while physical sources reference it",
//                 create_req.create_logical.source_name
//             );
//         })
//     }
// 
//     #[quickcheck]
//     fn worker_drop_with_physical_sources_fails(create_req: CreatePhysicalSourceWithRefs) -> bool {
//         test_prop(|catalog| async move {
//             catalog
//                 .create_logical_source(&create_req.create_logical)
//                 .await
//                 .expect("CreateLogicalSource should succeed");
//             catalog
//                 .create_worker(&create_req.create_worker)
//                 .await
//                 .expect("CreateWorker should succeed");
//             catalog
//                 .create_physical_source(&create_req.create_physical)
//                 .await
//                 .expect("CreatePhysicalSource should succeed");
// 
//             // Property: Cannot drop worker while physical sources reference it
//             let grpc_addr = GrpcAddr::new(
//                 create_req.create_worker.host_name,
//                 create_req.create_worker.grpc_port,
//             );
// 
//             assert!(
//                 catalog.drop_worker(&grpc_addr).await.is_err(),
//                 "Should not be able to drop worker '{}' while physical sources reference it",
//                 grpc_addr,
//             );
//         })
//     }
// 
//     #[quickcheck]
//     fn worker_drop_with_sinks_fails(create_req: CreateSinkWithRefs) -> bool {
//         test_prop(|catalog| async move {
//             catalog
//                 .create_worker(&create_req.create_worker)
//                 .await
//                 .expect("CreateWorker should succeed");
//             catalog
//                 .create_sink(&create_req.create_sink)
//                 .await
//                 .expect("CreateSink should succeed");
// 
//             // Property: Cannot drop worker while sinks reference it
//             let grpc_addr = GrpcAddr::new(
//                 create_req.create_worker.host_name,
//                 create_req.create_worker.grpc_port,
//             );
// 
//             assert!(
//                 catalog.drop_worker(&grpc_addr).await.is_err(),
//                 "Should not be able to drop worker '{}' while sinks reference it",
//                 grpc_addr
//             );
//         })
//     }
// 
//     #[quickcheck]
//     fn create_drop_create_logical_source_succeeds(create_source: CreateLogicalSource) -> bool {
//         test_prop(|catalog| async move {
//             // Property: Create-Drop-Create sequence is idempotent
//             catalog
//                 .create_logical_source(&create_source)
//                 .await
//                 .expect("First create should succeed");
// 
//             let drop_request = DropLogicalSource {
//                 source_name: create_source.source_name.clone(),
//             };
// 
//             catalog
//                 .drop_logical_source(&drop_request)
//                 .await
//                 .expect("Drop should succeed");
// 
//             catalog
//                 .create_logical_source(&create_source)
//                 .await
//                 .expect("Second create after drop should succeed");
//         })
//     }
// 
//     #[quickcheck]
//     fn create_drop_create_worker_succeeds(create_worker: CreateWorker) -> bool {
//         test_prop(|catalog| async move {
//             // Property: Create-Drop-Create sequence is idempotent
//             catalog
//                 .create_worker(&create_worker)
//                 .await
//                 .expect("First create should succeed");
// 
//             let grpc_addr = GrpcAddr::new(create_worker.host_name.clone(), create_worker.grpc_port);
// 
//             catalog
//                 .drop_worker(&grpc_addr)
//                 .await
//                 .expect("Drop should succeed");
// 
//             catalog
//                 .create_worker(&create_worker)
//                 .await
//                 .expect("Second create after drop should succeed");
//         })
//     }
// 
//     #[quickcheck]
//     fn drop_nonexistent_logical_source_succeeds(source_name: String) -> bool {
//         test_prop(|catalog| async move {
//             // Property: Dropping nonexistent entities is safe
//             let drop_request = DropLogicalSource {
//                 source_name: source_name.clone(),
//             };
// 
//             catalog
//                 .drop_logical_source(&drop_request)
//                 .await
//                 .expect("Dropping nonexistent logical source should succeed");
//         })
//     }
// 
//     #[quickcheck]
//     fn drop_nonexistent_worker_succeeds(grpc_addr: GrpcAddr) -> bool {
//         test_prop(|catalog| async move {
//             // Property: Dropping nonexistent entities is safe
//             catalog
//                 .drop_worker(&grpc_addr)
//                 .await
//                 .expect("Dropping nonexistent worker should succeed");
//         })
//     }
// }
