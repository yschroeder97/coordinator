use crate::catalog::query::CreateQuery;
#[cfg(test)]
use crate::catalog::query::QueryState;
use crate::catalog::sink::{CreateSink, SinkType};
use crate::catalog::source::logical_source::CreateLogicalSource;
use crate::catalog::source::physical_source::{CreatePhysicalSource, SourceType};
use crate::catalog::worker::endpoint::{NetworkAddr, DEFAULT_DATA_PORT, DEFAULT_GRPC_PORT};
use crate::catalog::worker::CreateWorker;
use crate::catalog::Catalog;
use proptest::arbitrary::any;
use proptest::prelude::{prop, Rng};
use proptest::prop_compose;
use proptest::strategy::{Just, Strategy};
use sqlx::sqlite::SqlitePoolOptions;
use std::future::Future;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_time()
            .enable_io()
            .build()
            .expect("Could not create tokio runtime")
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

pub fn test_prop<F, Fut>(test_fn: F) -> ()
where
    F: FnOnce(Catalog) -> Fut,
    Fut: Future<Output = ()>,
{
    get_runtime().block_on(run_async(test_fn))
}

prop_compose! {
    pub fn arb_create_worker()(
        host_name in r"[a-z0-9]{3,15}",
        grpc_port in 1..65534u16, // Max - 1 to ensure space for data_port
        capacity in any::<u32>(),
        // Generate 0 to 10 random peers
        peers in prop::collection::vec(any::<NetworkAddr>(), 0..10)
    ) -> CreateWorker {
        CreateWorker {
            host_name,
            grpc_port,
            data_port: grpc_port + 1,
            capacity,
            peers,
        }
    }
}

#[cfg(test)]
pub fn arb_worker_custom(
    host: String,
    grpc: u16,
    data: u16,
) -> impl Strategy<Value = CreateWorker> {
    (1..100u32).prop_map(move |capacity| CreateWorker {
        host_name: host.clone(),
        grpc_port: grpc,
        data_port: data,
        capacity,
        peers: vec![],
    })
}

#[cfg(test)]
pub fn arb_worker_cluster_on_subnet(count: u8) -> impl Strategy<Value = Vec<CreateWorker>> {
    prop::collection::hash_set(1..255u8, count as usize).prop_flat_map(move |octets| {
        octets
            .into_iter()
            .map(|octet| {
                let ip = format!("192.168.1.{}", octet);
                arb_worker_custom(ip, DEFAULT_GRPC_PORT, DEFAULT_DATA_PORT)
            })
            .collect::<Vec<_>>()
    })
}
#[derive(Debug, Clone)]
pub(crate) struct PhysicalSourceWithRefs {
    pub(crate) logical: CreateLogicalSource,
    pub(crate) worker: CreateWorker,
    pub(crate) physical: CreatePhysicalSource,
}

prop_compose! {
    pub(crate) fn arb_physical_with_refs()(
        logical in any::<CreateLogicalSource>(),
        worker in arb_create_worker(),
        source_type in any::<SourceType>(),
    ) -> PhysicalSourceWithRefs {
        let physical = CreatePhysicalSource {
            logical_source: logical.source_name.clone(),
            placement_host_name: worker.host_name.clone(),
            placement_grpc_port: worker.grpc_port,
            source_type,
            source_config: Default::default(),
            parser_config: Default::default(),
        };
        PhysicalSourceWithRefs { logical, worker, physical }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SinkWithRefs {
    pub(crate) worker: CreateWorker,
    pub(crate) sink: CreateSink,
}

#[cfg(test)]
prop_compose! {
    pub fn arb_create_sink()(
        name in "[a-z]",
        worker in arb_create_worker(),
        sink_type in any::<SinkType>(),
    ) -> SinkWithRefs {
        let sink = CreateSink {
            name,
            placement_host_name: worker.host_name.clone(),
            placement_grpc_port: worker.grpc_port,
            sink_type,
            config: Default::default(),
        };
        SinkWithRefs { worker, sink }
    }
}

#[cfg(test)]
prop_compose! {
    pub fn arb_create_query()(
        name in "[a-z]{1,20}",
        stmt in "[a-z0-9]{10,50}",
        workers in prop::collection::vec(arb_create_worker(), 0..10)
    ) -> CreateQuery {
        let on_workers = workers.into_iter()
            .map(|w| NetworkAddr { host: w.host_name, port: w.grpc_port })
            .collect();
        CreateQuery {
            name,
            stmt,
            on_workers,
        }
    }
}

/// Strategy to produce a valid sequence of states starting from Pending
pub fn arb_valid_state_sequence() -> impl Strategy<Value = Vec<QueryState>> {
    Just(vec![QueryState::Pending]).prop_perturb(|mut states, mut rng| {
        while let Some(current) = states.last() {
            let options = current.transitions();
            if options.is_empty() || rng.random_bool(0.15) {
                break;
            }
            let next = options[rng.random_range(0..options.len())];
            states.push(next);
        }
        states
    })
}

/// Strategy to produce exactly one invalid (from, to) transition
pub fn arb_invalid_transition() -> impl Strategy<Value = (QueryState, QueryState)> {
    any::<(QueryState, QueryState)>().prop_filter(
        "Transition must be invalid and states must be different",
        |(from, to)| {
            // It's invalid if 'to' is NOT in the list of valid next states
            // AND the trigger only fires when NEW != OLD
            from != to && !from.transitions().contains(to)
        },
    )
}
