use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::{Code, Request, Response, Status};
use madsim::rand::Rng;
use tracing::{debug, error, instrument};

use controller::worker::health_monitor::health_proto;
use controller::worker::worker_client::worker_rpc_service;

use worker_rpc_service::worker_rpc_service_server::WorkerRpcService;
#[allow(unused_imports)]
pub use worker_rpc_service::worker_rpc_service_server::WorkerRpcServiceServer;
use worker_rpc_service::worker_status_response::{ActiveQuery, TerminatedQuery};
use worker_rpc_service::{
    QueryLogReply, QueryLogRequest, QueryStatusReply, QueryStatusRequest, RegisterQueryReply,
    RegisterQueryRequest, StartQueryRequest, StopQueryRequest, UnregisterQueryRequest,
    WorkerStatusRequest, WorkerStatusResponse,
};

#[allow(unused_imports)]
pub use health_proto::health_server::HealthServer;

type FragmentId = u64;

#[derive(Default, Debug, Clone, PartialEq)]
enum QueryFragmentState {
    #[default]
    Registered,
    Started,
    Running,
    Stopped,
    Failed,
}

impl From<QueryFragmentState> for i32 {
    fn from(state: QueryFragmentState) -> i32 {
        match state {
            QueryFragmentState::Registered => 0,
            QueryFragmentState::Started => 1,
            QueryFragmentState::Running => 2,
            QueryFragmentState::Stopped => 3,
            QueryFragmentState::Failed => 4,
        }
    }
}

#[derive(Default)]
pub struct HealthServiceImpl;

#[tonic::async_trait]
impl health_proto::health_server::Health for HealthServiceImpl {
    async fn check(
        &self,
        _request: Request<health_proto::HealthCheckRequest>,
    ) -> Result<Response<health_proto::HealthCheckResponse>, Status> {
        Ok(Response::new(health_proto::HealthCheckResponse {
            status: health_proto::health_check_response::ServingStatus::Serving as i32,
        }))
    }
}

#[derive(Default, Clone)]
struct QueryFragment {
    state: QueryFragmentState,
    started: u64,
    terminated: u64,
    error: bool,
}

pub struct SingleNodeWorker {
    fragments: Arc<RwLock<HashMap<FragmentId, QueryFragment>>>,
}

impl SingleNodeWorker {
    pub fn new() -> Self {
        Self {
            fragments: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn current_timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_millis() as u64
    }

    fn query_not_found_error(query_id: u64) -> Status {
        error!("Unknown query with id {}", query_id);
        Status::internal(format!("Unknown query with id {}", query_id))
    }

    fn with_query_mut<F, R>(&self, query_id: u64, f: F) -> Result<R, Status>
    where
        F: FnOnce(&mut QueryFragment) -> R,
    {
        match self.fragments.write().unwrap().get_mut(&query_id) {
            Some(query) => Ok(f(query)),
            None => Err(Self::query_not_found_error(query_id)),
        }
    }

    fn with_query<F, R>(&self, query_id: u64, f: F) -> Result<R, Status>
    where
        F: FnOnce(&QueryFragment) -> R,
    {
        match self.fragments.read().unwrap().get(&query_id) {
            Some(query) => Ok(f(query)),
            None => Err(Self::query_not_found_error(query_id)),
        }
    }

    fn maybe_fail() -> Result<(), Status> {
        if madsim::buggify::buggify() {
            error!("buggify: simulated RPC failure");
            return Err(Status::new(Code::Internal, "buggify: simulated RPC failure"));
        }
        Ok(())
    }

    async fn maybe_delay() {
        if madsim::buggify::buggify_with_prob(0.1) {
            let delay = Duration::from_millis(madsim::rand::thread_rng().gen_range(100..2000));
            debug!("buggify: injecting {delay:?} RPC delay");
            tokio::time::sleep(delay).await;
        }
    }
}

#[tonic::async_trait]
impl WorkerRpcService for SingleNodeWorker {
    #[instrument(skip(self, request))]
    async fn register_query(
        &self,
        request: Request<RegisterQueryRequest>,
    ) -> Result<Response<RegisterQueryReply>, Status> {
        Self::maybe_fail()?;
        Self::maybe_delay().await;

        let id = request.get_ref().query_id;
        self.fragments
            .write()
            .unwrap()
            .insert(id, QueryFragment::default());

        debug!("Registered new query: {}", id);
        Ok(Response::new(RegisterQueryReply {}))
    }

    #[instrument(skip(self), fields(query_id = %request.get_ref().query_id))]
    async fn unregister_query(
        &self,
        request: Request<UnregisterQueryRequest>,
    ) -> Result<Response<()>, Status> {
        Self::maybe_fail()?;
        Self::maybe_delay().await;
        let query_id = request.get_ref().query_id;
        match self.fragments.write().unwrap().remove(&query_id) {
            Some(_) => {
                debug!("Unregistered query");
                Ok(Response::new(()))
            }
            None => Err(Self::query_not_found_error(query_id)),
        }
    }

    #[instrument(skip(self, request), fields(query_id = %request.get_ref().query_id))]
    async fn start_query(
        &self,
        request: Request<StartQueryRequest>,
    ) -> Result<Response<()>, Status> {
        Self::maybe_fail()?;
        Self::maybe_delay().await;
        let query_id = request.get_ref().query_id;
        self.with_query_mut(query_id, |query| {
            if madsim::buggify::buggify_with_prob(0.05) {
                error!("buggify: query {query_id} failed on start");
                query.state = QueryFragmentState::Failed;
                query.terminated = Self::current_timestamp_ms();
            } else {
                query.state = QueryFragmentState::Running;
            }
            query.started = Self::current_timestamp_ms();
            debug!("Started query");
        })?;
        Ok(Response::new(()))
    }

    #[instrument(skip(self), fields(query_id = %request.get_ref().query_id))]
    async fn stop_query(&self, request: Request<StopQueryRequest>) -> Result<Response<()>, Status> {
        Self::maybe_fail()?;
        Self::maybe_delay().await;
        let query_id = request.get_ref().query_id;
        self.with_query_mut(query_id, |query| {
            query.state = QueryFragmentState::Stopped;
            query.terminated = Self::current_timestamp_ms();
            debug!("Stopped query");
        })?;
        Ok(Response::new(()))
    }

    #[instrument(skip(self), fields(query_id = %request.get_ref().query_id))]
    async fn request_query_status(
        &self,
        request: Request<QueryStatusRequest>,
    ) -> Result<Response<QueryStatusReply>, Status> {
        Self::maybe_fail()?;
        Self::maybe_delay().await;
        let query_id = request.get_ref().query_id;
        let reply = self.with_query(query_id, |query| QueryStatusReply {
            query_id,
            state: query.state.clone().into(),
            metrics: None,
        })?;
        Ok(Response::new(reply))
    }

    #[instrument(skip(self), fields(query_id = %request.get_ref().query_id))]
    async fn request_query_log(
        &self,
        request: Request<QueryLogRequest>,
    ) -> Result<Response<QueryLogReply>, Status> {
        Self::maybe_fail()?;
        Self::maybe_delay().await;
        let _ = request;
        Ok(Response::new(QueryLogReply { entries: vec![] }))
    }

    #[instrument(skip(self))]
    async fn request_status(
        &self,
        request: Request<WorkerStatusRequest>,
    ) -> Result<Response<WorkerStatusResponse>, Status> {
        Self::maybe_fail()?;
        Self::maybe_delay().await;
        let queries = self.fragments.read().unwrap();
        let (terminated, active): (Vec<_>, Vec<_>) = queries.iter().partition(|(_, query)| {
            query.state == QueryFragmentState::Failed || query.state == QueryFragmentState::Stopped
        });

        let active_queries = active
            .into_iter()
            .map(|(id, query)| ActiveQuery {
                query_id: *id,
                started_unix_timestamp_in_ms: query.started,
            })
            .collect();

        let terminated_queries = terminated
            .into_iter()
            .map(|(id, query)| TerminatedQuery {
                query_id: *id,
                started_unix_timestamp_in_ms: query.started,
                terminated_unix_timestamp_in_ms: query.terminated,
                error: None,
            })
            .collect();

        Ok(Response::new(WorkerStatusResponse {
            after_unix_timestamp_in_ms: request.get_ref().after_unix_timestamp_in_ms,
            until_unix_timestamp_in_ms: Self::current_timestamp_ms(),
            active_queries,
            terminated_queries,
        }))
    }
}
