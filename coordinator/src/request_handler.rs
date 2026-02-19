use crate::coordinator::{
    CoordinatorRequest, CreateQueryRequest, CreateWorkerRequest, DropQueryRequest,
    DropWorkerRequest,
};
use anyhow::Result;
use catalog::Catalog;
use catalog::NotifiableCatalog;
use controller::request::Request;
use model::query;
use model::query::QueryId;
use model::query::query_state::QueryState;
use model::worker::endpoint::HostAddr;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info, instrument};

macro_rules! dispatch {
    ($req:expr, $field:ident . $method:ident) => {{
        debug!("Received: {:?}", $req);
        let Request { payload, reply_to } = $req;
        let _ = reply_to.send(
            self.catalog
                .$field
                .$method(payload)
                .await
                .map_err(Into::into),
        );
    }};
}

macro_rules! dispatch_blocking {
    ($req:expr, $field:ident . $method:ident, |$result:ident, $request:ident| $store:expr) => {{
        debug!("Received: {:?}", $req);
        let Request { payload, reply_to } = $req;
        match self.catalog.$field.$method(payload.clone()).await {
            Ok($result) => {
                if payload.should_block() {
                    let $request = Request { payload, reply_to };
                    $store
                } else {
                    let _ = reply_to.send(Ok($result));
                }
            }
            Err(e) => {
                let _ = reply_to.send(Err(e.into()));
            }
        }
    }};
}

#[derive(Error, Debug)]
#[error("Query '{query_id}' terminated with state {state} before reaching target state")]
pub struct EarlyTermination {
    pub query_id: i64,
    pub state: QueryState,
    pub model: query::Model,
}

struct PendingQueryDrop {
    query_ids: Vec<QueryId>,
    request: DropQueryRequest,
}

enum PendingWorkerRequest {
    Create(CreateWorkerRequest),
    Drop(DropWorkerRequest),
}

pub(super) struct RequestHandler {
    receiver: flume::Receiver<CoordinatorRequest>,
    catalog: Arc<Catalog>,
    pending_query_creates: HashMap<QueryId, CreateQueryRequest>,
    pending_query_drops: Vec<PendingQueryDrop>,
    pending_worker_requests: HashMap<HostAddr, PendingWorkerRequest>,
}

impl RequestHandler {
    pub(super) fn new(
        receiver: flume::Receiver<CoordinatorRequest>,
        catalog: Arc<Catalog>,
    ) -> RequestHandler {
        Self {
            receiver,
            catalog,
            pending_query_creates: HashMap::new(),
            pending_query_drops: Vec::new(),
            pending_worker_requests: HashMap::new(),
        }
    }

    #[instrument(skip(self))]
    pub(super) async fn run(mut self) {
        let mut query_state_rx = self.catalog.query.subscribe_state();
        let mut worker_state_rx = self.catalog.worker.subscribe_state();

        loop {
            tokio::select! {
                recv_result = self.receiver.recv_async() => match recv_result {
                    Some(req) => self.handle_recv(req).await,
                    None => {
                        info!("All clients have been dropped");
                        return;
                    }
                },
                Ok(()) = query_state_rx.changed() => {
                    self.resolve_pending_queries().await;
                },
                Ok(()) = worker_state_rx.changed() => {
                    self.resolve_pending_workers().await;
                }
            }
        }
    }

    #[instrument(skip(self))]
    async fn resolve_pending_queries(&mut self) {
        // Skip fetching data when no outstanding requests exist
        if self.pending_query_creates.is_empty() && self.pending_query_drops.is_empty() {
            return;
        }

        let all_ids = self.pending_query_creates.keys().chain(
            self.pending_query_drops
                .iter()
                .flat_map(|drop| &drop.query_ids),
        );

        let Ok(queries) = self.catalog.query.get_queries_by_id(all_ids).await else {
            return;
        };
        let queries: HashMap<QueryId, query::Model> =
            queries.into_iter().map(|m| (m.id, m)).collect();

        self.find_remove_created(&queries);
        self.find_remove_dropped(&queries);
    }

    fn find_remove_created(&mut self, queries: &HashMap<QueryId, query::Model>) {
        let resolved_create_ids: Vec<QueryId> = queries
            .iter()
            .filter(|(id, model)| {
                self.pending_query_creates
                    .get(id)
                    .is_some_and(|req| model.current_state >= req.payload.block_until)
            })
            .map(|(&id, _)| id)
            .collect();

        for id in resolved_create_ids {
            let req = self.pending_query_creates.remove(&id).unwrap();
            let model = queries[&id].clone();

            if model.current_state.is_terminal() && model.current_state != req.payload.block_until {
                let _ = req.reply_to.send(Err(EarlyTermination {
                    query_id: model.id,
                    state: model.current_state,
                    model,
                }
                .into()));
            } else {
                let _ = req.reply_to.send(Ok(model));
            }
        }
    }

    fn find_remove_dropped(&mut self, queries: &HashMap<QueryId, query::Model>) {
        let resolved_drop_indices: Vec<usize> = self
            .pending_query_drops
            .iter()
            .enumerate()
            .filter(|(_, drop)| {
                drop.query_ids.iter().all(|id| {
                    queries
                        .get(id)
                        .is_some_and(|m| m.current_state.is_terminal())
                })
            })
            .map(|(i, _)| i)
            .collect();

        for i in resolved_drop_indices.into_iter().rev() {
            let drop = self.pending_query_drops.swap_remove(i);
            let drop_models = drop
                .query_ids
                .iter()
                .map(|id| queries[id].clone())
                .collect();
            let _ = drop.request.reply_to.send(Ok(drop_models));
        }
    }

    #[instrument(skip(self))]
    async fn resolve_pending_workers(&mut self) {
        // TODO: implement worker state resolution
    }

    #[instrument(skip(self))]
    async fn handle_recv(&mut self, req: CoordinatorRequest) {
        match req {
            CoordinatorRequest::CreateLogicalSource(r) => {
                dispatch!(r, source.create_logical_source)
            }
            CoordinatorRequest::CreatePhysicalSource(r) => {
                dispatch!(r, source.create_physical_source)
            }
            CoordinatorRequest::CreateSink(r) => {
                dispatch!(r, sink.create_sink)
            }
            CoordinatorRequest::DropLogicalSource(r) => {
                dispatch!(r, source.drop_logical_source)
            }
            CoordinatorRequest::DropPhysicalSource(r) => {
                dispatch!(r, source.drop_physical_source)
            }
            CoordinatorRequest::DropSink(r) => {
                dispatch!(r, sink.drop_sink)
            }
            CoordinatorRequest::GetLogicalSource(r) => {
                dispatch!(r, source.get_logical_source)
            }
            CoordinatorRequest::GetPhysicalSource(r) => {
                dispatch!(r, source.get_physical_source)
            }
            CoordinatorRequest::GetSink(r) => {
                dispatch!(r, sink.get_sink)
            }
            CoordinatorRequest::GetQuery(r) => {
                dispatch!(r, query.get_query)
            }
            CoordinatorRequest::GetWorker(r) => {
                dispatch!(r, worker.get_worker)
            }
            CoordinatorRequest::CreateQuery(r) => {
                dispatch_blocking!(r, query.create_query, |result, request| {
                    let prev = self.pending_query_creates.insert(result.id, request);
                    assert!(prev.is_none(), "Query id {} should be unique", result.id);
                })
            }
            CoordinatorRequest::CreateWorker(r) => {
                dispatch_blocking!(r, worker.create_worker, |result, request| {
                    let prev = self.pending_worker_requests.insert(
                        result.host_addr.clone(),
                        PendingWorkerRequest::Create(request),
                    );
                    assert!(
                        prev.is_none(),
                        "Worker {:?} should be unique",
                        result.host_addr
                    );
                })
            }
            CoordinatorRequest::DropQuery(r) => {
                dispatch_blocking!(r, query.drop_query, |result, request| {
                    self.pending_query_drops.push(PendingQueryDrop {
                        query_ids: result.iter().map(|q| q.id).collect(),
                        request,
                    });
                })
            }
            CoordinatorRequest::DropWorker(r) => {
                dispatch_blocking!(r, worker.drop_worker, |result, request| {
                    let prev = self.pending_worker_requests.insert(
                        result.host_addr.clone(),
                        PendingWorkerRequest::Drop(request),
                    );
                    assert!(
                        prev.is_none(),
                        "Worker {:?} should be unique",
                        result.host_addr
                    );
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::database::Database;
    use model::query::query_state::DesiredQueryState;
    use model::query::{CreateQuery, GetQuery};

    struct TestHandle {
        sender: flume::Sender<CoordinatorRequest>,
        catalog: Arc<Catalog>,
    }

    impl TestHandle {
        async fn new() -> Self {
            let catalog = Catalog::for_test().await;
            let (sender, receiver) = flume::bounded(16);
            let handler_catalog = catalog.clone();
            tokio::spawn(async move {
                RequestHandler::new(receiver, handler_catalog).run().await;
            });
            Self { sender, catalog }
        }

        async fn send<P, R>(&self, payload: P) -> tokio::sync::oneshot::Receiver<R>
        where
            P: std::fmt::Debug,
            Request<P, R>: Into<CoordinatorRequest>,
        {
            let (rx, request) = Request::new(payload);
            self.sender
                .send_async(request.into())
                .await
                .expect("Handler should be running");
            rx
        }
    }

    #[tokio::test]
    async fn non_blocking_create_query_returns_immediately() {
        let handle = TestHandle::new().await;

        let req = CreateQuery::new("SELECT 1".to_string());
        let rx = handle.send(req).await;

        let result = rx.await.unwrap().unwrap();
        assert_eq!(result.statement, "SELECT 1");
        assert_eq!(result.current_state, QueryState::Pending);
        assert_eq!(result.desired_state, DesiredQueryState::Completed);
    }

    #[tokio::test]
    async fn blocking_create_query_waits_for_target_state() {
        let handle = TestHandle::new().await;

        let req = CreateQuery::new("SELECT 1".to_string()).block_until(QueryState::Registered);
        let mut rx = handle.send(req).await;

        let queries = handle
            .catalog
            .query
            .get_query(GetQuery::new())
            .await
            .unwrap();
        assert_eq!(queries.len(), 1);
        let query = queries.into_iter().next().unwrap();

        let query = handle
            .catalog
            .query
            .advance_query_state(query)
            .await
            .unwrap();
        assert_eq!(query.current_state, QueryState::Planned);
        assert!(rx.try_recv().is_err(), "Should not have resolved yet");

        let query = handle
            .catalog
            .query
            .advance_query_state(query)
            .await
            .unwrap();
        assert_eq!(query.current_state, QueryState::Registered);

        let result = rx.await.unwrap().unwrap();
        assert_eq!(result.current_state, QueryState::Registered);
    }

    #[tokio::test]
    async fn blocking_create_query_early_termination() {
        let handle = TestHandle::new().await;

        let req = CreateQuery::new("SELECT 1".to_string()).block_until(QueryState::Running);
        let rx = handle.send(req).await;

        let queries = handle
            .catalog
            .query
            .get_query(GetQuery::new())
            .await
            .unwrap();
        let query = queries.into_iter().next().unwrap();

        handle
            .catalog
            .query
            .set_query_state(query, |_| QueryState::Failed)
            .await
            .unwrap();

        let result = rx.await.unwrap();
        assert!(result.is_err(), "Should fail with EarlyTermination");
    }
}
