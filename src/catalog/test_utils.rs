use crate::catalog::query::{CreateQuery, QueryState};
use crate::catalog::sink::{CreateSink, SinkType};
use crate::catalog::source::logical_source::CreateLogicalSource;
use crate::catalog::source::physical_source::{CreatePhysicalSource, SourceType};
use crate::catalog::source::schema::{AttributeField, DataType, Schema};
use crate::catalog::worker::endpoint::GrpcAddr;
use crate::catalog::worker::{CreateWorker, DropWorker, WorkerState};
use crate::catalog::Catalog;
use quickcheck::{Arbitrary, Gen};
use sqlx::sqlite::SqlitePoolOptions;
use std::future::Future;
use std::sync::OnceLock;
use strum::IntoEnumIterator;
use tokio::runtime::Runtime;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        #[cfg(not(madsim))]
        {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_time()
                .enable_io()
                .build()
                .expect("Could not create tokio runtime")
        }
        #[cfg(madsim)]
        {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .build()
                .expect("Could not create tokio runtime")
        }
    })
}

pub async fn run_async<Fn, Fut, O>(test_fn: Fn) -> O
where
    Fn: FnOnce(Catalog) -> Fut,
    Fut: Future<Output = O>,
{
    let catalog = Catalog::from_pool(
        SqlitePoolOptions::new()
            .connect(":memory:")
            .await
            .expect("Could not create connection to database"),
    )
    .await
    .expect("Could not create catalog");

    test_fn(catalog).await
}

pub fn test_prop<F, Fut>(test_fn: F) -> bool
where
    F: FnOnce(Catalog) -> Fut,
    Fut: Future<Output = ()>,
{
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        get_runtime().block_on(run_async(test_fn))
    }))
    .is_ok()
}

impl Arbitrary for GrpcAddr {
    fn arbitrary(g: &mut Gen) -> Self {
        GrpcAddr::new(String::arbitrary(g), (u16::arbitrary(g) % 65535) + 1)
    }
}

impl Arbitrary for DataType {
    fn arbitrary(g: &mut Gen) -> Self {
        let variants: Vec<DataType> = DataType::iter().collect();
        *g.choose(&variants).expect("choose value")
    }
}

impl Arbitrary for Schema {
    fn arbitrary(g: &mut Gen) -> Self {
        let size = usize::arbitrary(g) % 10 + 1;
        let fields: Vec<AttributeField> = (0..size).map(|_| AttributeField::arbitrary(g)).collect();
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
        let variants: Vec<SourceType> = SourceType::iter().collect();
        *g.choose(&variants).expect("choose value")
    }
}

impl Arbitrary for SinkType {
    fn arbitrary(g: &mut Gen) -> Self {
        let variants: Vec<SinkType> = SinkType::iter().collect();
        *g.choose(&variants).expect("choose value")
    }
}

impl Arbitrary for WorkerState {
    fn arbitrary(g: &mut Gen) -> Self {
        let variants: Vec<WorkerState> = WorkerState::iter().collect();
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

impl Arbitrary for DropWorker {
    fn arbitrary(g: &mut Gen) -> Self {
        DropWorker {
            id: Option::arbitrary(g),
            with_current_state: Option::arbitrary(g),
            with_desired_state: Option::arbitrary(g),
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

impl Arbitrary for CreateQuery {
    fn arbitrary(g: &mut Gen) -> Self {
        CreateQuery::new_with_name(
            String::arbitrary(g),
            String::arbitrary(g),
            Vec::arbitrary(g),
        )
    }
}

#[derive(Debug, Clone)]
pub struct CreatePhysicalSourceWithRefs {
    pub create_logical: CreateLogicalSource,
    pub create_worker: CreateWorker,
    pub create_physical: CreatePhysicalSource,
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
pub struct CreateSinkWithRefs {
    pub create_sink: CreateSink,
    pub create_worker: CreateWorker,
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

impl Arbitrary for QueryState {
    fn arbitrary(g: &mut Gen) -> Self {
        let variants: Vec<QueryState> = QueryState::iter().collect();
        *g.choose(&variants).expect("choose value")
    }
}

#[derive(Clone, Debug)]
pub struct ValidQueryTransitionPath {
    pub states: Vec<QueryState>,
}

impl Arbitrary for ValidQueryTransitionPath {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut states = Vec::new();
        let mut current = QueryState::Pending;

        // We decide randomly how far to go down the path
        let steps = usize::arbitrary(g) % 5; // 0 to 4 steps

        for _ in 0..steps {
            current = match current {
                QueryState::Pending => QueryState::Deploying,
                QueryState::Deploying => {
                    // Deploying -> Running or Failed
                    if bool::arbitrary(g) {
                        QueryState::Running
                    } else {
                        QueryState::Failed
                    }
                }
                QueryState::Running => {
                    // Running -> Terminating, Failed, Completed, Stopped
                    let options = [
                        QueryState::Terminating,
                        QueryState::Failed,
                        QueryState::Completed,
                        QueryState::Stopped,
                    ];
                    *g.choose(&options).unwrap()
                }
                QueryState::Terminating => {
                    // Terminating -> Stopped
                    QueryState::Stopped
                }
                _ => break, // Terminal states have no next step
            };
            states.push(current);
        }

        ValidQueryTransitionPath { states }
    }
}

#[derive(Clone, Debug)]
pub struct InvalidQueryTransition {
    pub from: QueryState,
    pub to: QueryState,
}

impl Arbitrary for InvalidQueryTransition {
    fn arbitrary(g: &mut Gen) -> Self {
        loop {
            let from = QueryState::arbitrary(g);
            let to = QueryState::arbitrary(g);

            let is_valid = match from {
                QueryState::Pending => to == QueryState::Deploying,
                QueryState::Deploying => {
                    matches!(to, QueryState::Running | QueryState::Failed)
                }
                QueryState::Running => matches!(
                    to,
                    QueryState::Terminating
                        | QueryState::Failed
                        | QueryState::Completed
                        | QueryState::Stopped
                ),
                QueryState::Terminating => to == QueryState::Stopped,
                // From terminal states, any transition is invalid
                _ => false,
            };

            // We want invalid transitions where from != to
            if !is_valid && from != to {
                return InvalidQueryTransition { from, to };
            }
        }
    }
}
