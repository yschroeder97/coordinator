use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status};
use tracing::{error, info, instrument};
use uuid::Uuid;

use worker_rpc_service::worker_rpc_service_server::{WorkerRpcService};
use worker_rpc_service::worker_status_response::{ActiveQuery, TerminatedQuery};
use worker_rpc_service::{
    QueryLogReply, QueryLogRequest, QueryStatusReply, QueryStatusRequest, RegisterQueryReply,
    RegisterQueryRequest, StartQueryRequest, StopQueryRequest, UnregisterQueryRequest,
    WorkerStatusRequest, WorkerStatusResponse,
};
type QueryId = String;

#[derive(Default, Clone, PartialEq)]
enum QueryFragmentState {
    #[default]
    Registered,
    Running,
    Stopped,
    Failed,
}

impl From<QueryFragmentState> for i32 {
    fn from(state: QueryFragmentState) -> i32 {
        match state {
            QueryFragmentState::Registered => 0,
            QueryFragmentState::Running => 2,
            QueryFragmentState::Stopped => 3,
            QueryFragmentState::Failed => 4,
        }
    }
}

pub mod worker_rpc_service {
    tonic::include_proto!("worker_rpc");
}

#[derive(Default, Clone)]
struct Query {
    state: QueryFragmentState,
    started: u64,
    terminated: u64,
    error: bool,
}

#[derive(Default)]
pub struct SingleNodeWorker {
    queries: Arc<RwLock<HashMap<QueryId, Query>>>,
}

impl SingleNodeWorker {
    fn current_timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_millis() as u64
    }

    fn query_not_found_error(query_id: &str) -> Status {
        error!("Unknown query with id {}", query_id);
        Status::internal(format!("Unknown query with id {}", query_id))
    }

    fn with_query_mut<F, R>(&self, query_id: &str, f: F) -> Result<R, Status>
    where
        F: FnOnce(&mut Query) -> R,
    {
        match self.queries.write().unwrap().get_mut(query_id) {
            Some(query) => Ok(f(query)),
            None => Err(Self::query_not_found_error(query_id)),
        }
    }

    fn with_query<F, R>(&self, query_id: &str, f: F) -> Result<R, Status>
    where
        F: FnOnce(&Query) -> R,
    {
        match self.queries.read().unwrap().get(query_id) {
            Some(query) => Ok(f(query)),
            None => Err(Self::query_not_found_error(query_id)),
        }
    }
}

#[tonic::async_trait]
impl WorkerRpcService for SingleNodeWorker {
    #[instrument(skip(self, _request))]
    async fn register_query(
        &self,
        _request: Request<RegisterQueryRequest>,
    ) -> Result<Response<RegisterQueryReply>, Status> {
        let id = Uuid::new_v4();
        let id_string = id.to_string();
        self.queries
            .write()
            .unwrap()
            .insert(id_string.clone(), Query::default());

        info!("Registered new query: {}", id_string);
        Ok(Response::new(RegisterQueryReply {
            query_id: id_string,
        }))
    }

    #[instrument(skip(self), fields(query_id = %request.get_ref().query_id))]
    async fn unregister_query(
        &self,
        request: Request<UnregisterQueryRequest>,
    ) -> Result<Response<()>, Status> {
        let query_id = &request.get_ref().query_id;
        match self.queries.write().unwrap().remove(query_id) {
            Some(_) => {
                info!("Unregistered query");
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
        let query_id = &request.get_ref().query_id;
        self.with_query_mut(query_id, |query| {
            query.state = QueryFragmentState::Running;
            query.started = Self::current_timestamp_ms();
            info!("Started query");
        })?;
        Ok(Response::new(()))
    }

    #[instrument(skip(self), fields(query_id = %request.get_ref().query_id))]
    async fn stop_query(&self, request: Request<StopQueryRequest>) -> Result<Response<()>, Status> {
        let query_id = &request.get_ref().query_id;
        self.with_query_mut(query_id, |query| {
            query.state = QueryFragmentState::Stopped;
            query.terminated = Self::current_timestamp_ms();
            info!("Stopped query");
        })?;
        Ok(Response::new(()))
    }

    #[instrument(skip(self), fields(query_id = %request.get_ref().query_id))]
    async fn request_query_status(
        &self,
        request: Request<QueryStatusRequest>,
    ) -> Result<Response<QueryStatusReply>, Status> {
        let query_id = &request.get_ref().query_id;
        let reply = self.with_query(query_id, |query| QueryStatusReply {
            query_id: query_id.clone(),
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
        Ok(Response::new(QueryLogReply { entries: vec![] }))
    }

    #[instrument(skip(self))]
    async fn request_status(
        &self,
        request: Request<WorkerStatusRequest>,
    ) -> Result<Response<WorkerStatusResponse>, Status> {
        let queries = self.queries.read().unwrap();
        let (terminated, active): (Vec<_>, Vec<_>) = queries.iter().partition(|(_, query)| {
            query.state == QueryFragmentState::Failed || query.state == QueryFragmentState::Stopped
        });

        let active_queries = active
            .into_iter()
            .map(|(id, query)| ActiveQuery {
                query_id: id.clone(),
                started_unix_timestamp_in_ms: query.started,
            })
            .collect();

        let terminated_queries = terminated
            .into_iter()
            .map(|(id, query)| TerminatedQuery {
                query_id: id.clone(),
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
