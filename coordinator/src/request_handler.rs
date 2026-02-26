use crate::coordinator::{CoordinatorRequest, CreateQueryRequest, DropQueryRequest};
use catalog::Catalog;
use catalog::Reconcilable;
use controller::request::Request;
use model::query;
use model::query::query_state::QueryState;
use model::query::{GetQuery, QueryId};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info, instrument};

macro_rules! dispatch {
    ($self:ident, $req:expr, $field:ident . $method:ident) => {{
        debug!("Received: {:?}", $req);
        let Request { payload, reply_to } = $req;
        let _ = reply_to.send($self.catalog.$field.$method(payload).await);
    }};
}

macro_rules! dispatch_blocking {
    ($self:ident, $req:expr, $field:ident . $method:ident, |$result:ident, $request:ident| $store:expr) => {{
        debug!("Received: {:?}", $req);
        let Request { payload, reply_to } = $req;
        match $self.catalog.$field.$method(payload.clone()).await {
            Ok($result) => {
                if payload.should_block() {
                    let $request = Request { payload, reply_to };
                    $store
                } else {
                    let _ = reply_to.send(Ok($result));
                }
            }
            Err(e) => {
                let _ = reply_to.send(Err(e));
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

pub(super) struct RequestHandler {
    receiver: flume::Receiver<CoordinatorRequest>,
    catalog: Arc<Catalog>,
    pending_query_creates: HashMap<QueryId, CreateQueryRequest>,
    pending_query_drops: Vec<PendingQueryDrop>,
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
        }
    }

    #[instrument(skip(self))]
    pub(super) async fn run(mut self) {
        let mut query_state_rx = self.catalog.query.subscribe_state();

        loop {
            tokio::select! {
                recv_result = self.receiver.recv_async() => match recv_result {
                    Ok(req) => self.handle_recv(req).await,
                    Err(_) => {
                        info!("All clients have been dropped");
                        return;
                    }
                },
                Ok(()) = query_state_rx.changed() => {
                    self.resolve_pending_queries().await;
                },
            }
        }
    }

    #[instrument(skip(self))]
    async fn resolve_pending_queries(&mut self) {
        // Skip fetching data when no outstanding requests exist
        if self.pending_query_creates.is_empty() && self.pending_query_drops.is_empty() {
            return;
        }

        let all_ids: Vec<_> = self
            .pending_query_creates
            .keys()
            .copied()
            .chain(
                self.pending_query_drops
                    .iter()
                    .flat_map(|drop| &drop.query_ids)
                    .copied(),
            )
            .collect();

        let Ok(queries) = self
            .catalog
            .query
            .get_query(GetQuery::new().with_ids(all_ids))
            .await
        else {
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
    async fn handle_recv(&mut self, req: CoordinatorRequest) {
        match req {
            CoordinatorRequest::CreateLogicalSource(r) => {
                dispatch!(self, r, source.create_logical_source)
            }
            CoordinatorRequest::CreatePhysicalSource(r) => {
                dispatch!(self, r, source.create_physical_source)
            }
            CoordinatorRequest::CreateSink(r) => {
                dispatch!(self, r, sink.create_sink)
            }
            CoordinatorRequest::CreateWorker(r) => {
                dispatch!(self, r, worker.create_worker)
            }
            CoordinatorRequest::DropLogicalSource(r) => {
                dispatch!(self, r, source.drop_logical_source)
            }
            CoordinatorRequest::DropPhysicalSource(r) => {
                dispatch!(self, r, source.drop_physical_source)
            }
            CoordinatorRequest::DropSink(r) => {
                dispatch!(self, r, sink.drop_sink)
            }
            CoordinatorRequest::DropWorker(r) => {
                dispatch!(self, r, worker.drop_worker)
            }
            CoordinatorRequest::GetLogicalSource(r) => {
                dispatch!(self, r, source.get_logical_source)
            }
            CoordinatorRequest::GetPhysicalSource(r) => {
                dispatch!(self, r, source.get_physical_source)
            }
            CoordinatorRequest::GetSink(r) => {
                dispatch!(self, r, sink.get_sink)
            }
            CoordinatorRequest::GetQuery(r) => {
                dispatch!(self, r, query.get_query)
            }
            CoordinatorRequest::GetWorker(r) => {
                dispatch!(self, r, worker.get_worker)
            }
            CoordinatorRequest::CreateQuery(r) => {
                dispatch_blocking!(self, r, query.create_query, |result, request| {
                    self.pending_query_creates.insert(result.id, request);
                })
            }
            CoordinatorRequest::DropQuery(r) => {
                dispatch_blocking!(self, r, query.drop_query, |result, request| {
                    self.pending_query_drops.push(PendingQueryDrop {
                        query_ids: result.iter().map(|q| q.id).collect(),
                        request,
                    });
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::query::query_state::DesiredQueryState;
    use model::query::{CreateQuery, DropQuery, GetQuery, StopMode};

    struct TestHandle {
        sender: flume::Sender<CoordinatorRequest>,
        catalog: Arc<Catalog>,
    }

    impl TestHandle {
        async fn new() -> Self {
            let catalog = Catalog::for_test().await;
            let (sender, receiver) = flume::bounded(16);
            let handler_catalog = catalog.clone();
            tokio::spawn(RequestHandler::new(receiver, handler_catalog).run());
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
        tokio::task::yield_now().await;

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
            .set_query_state(&query, |_| QueryState::Planned)
            .await
            .unwrap();
        assert_eq!(query.current_state, QueryState::Planned);
        assert!(rx.try_recv().is_err(), "Should not have resolved yet");

        let query = handle
            .catalog
            .query
            .set_query_state(&query, |_| QueryState::Registered)
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
        tokio::task::yield_now().await;

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
            .set_query_state(&query, |_| QueryState::Failed)
            .await
            .unwrap();

        let result = rx.await.unwrap();
        assert!(result.is_err(), "Should fail with EarlyTermination");
    }

    #[tokio::test]
    async fn blocking_create_then_blocking_drop() {
        let handle = TestHandle::new().await;

        let create = CreateQuery::new("SELECT 1".to_string()).block_until(QueryState::Running);
        let create_rx = handle.send(create).await;
        tokio::task::yield_now().await;

        let queries = handle
            .catalog
            .query
            .get_query(GetQuery::new())
            .await
            .unwrap();
        let query = queries.into_iter().next().unwrap();
        let query_id = query.id;

        let query = handle
            .catalog
            .query
            .set_query_state(&query, |_| QueryState::Planned)
            .await
            .unwrap();
        let query = handle
            .catalog
            .query
            .set_query_state(&query, |_| QueryState::Registered)
            .await
            .unwrap();
        let query = handle
            .catalog
            .query
            .set_query_state(&query, |_| QueryState::Running)
            .await
            .unwrap();
        assert_eq!(query.current_state, QueryState::Running);

        let created = create_rx.await.unwrap().unwrap();
        assert_eq!(created.current_state, QueryState::Running);

        let drop = DropQuery::new()
            .with_filters(GetQuery::new().with_id(query_id))
            .stop_mode(StopMode::Forceful)
            .blocking();
        let drop_rx = handle.send(drop).await;
        tokio::task::yield_now().await;

        let query = handle
            .catalog
            .query
            .get_query(GetQuery::new().with_id(query_id))
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(query.desired_state, DesiredQueryState::Stopped);

        handle
            .catalog
            .query
            .set_query_state(&query, |_| QueryState::Stopped)
            .await
            .unwrap();

        let dropped = drop_rx.await.unwrap().unwrap();
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].current_state, QueryState::Stopped);
    }

    #[tokio::test]
    async fn blocking_drop_multiple_queries() {
        let handle = TestHandle::new().await;

        let mut query_ids = Vec::new();
        for i in 0..3 {
            let create = CreateQuery::new(format!("SELECT {i}"));
            let rx = handle.send(create).await;
            let result = rx.await.unwrap().unwrap();
            query_ids.push(result.id);
        }

        for &id in &query_ids {
            let query = handle
                .catalog
                .query
                .get_query(GetQuery::new().with_id(id))
                .await
                .unwrap()
                .into_iter()
                .next()
                .unwrap();
            let query = handle
                .catalog
                .query
                .set_query_state(&query, |_| QueryState::Planned)
                .await
                .unwrap();
            let query = handle
                .catalog
                .query
                .set_query_state(&query, |_| QueryState::Registered)
                .await
                .unwrap();
            let _ = handle
                .catalog
                .query
                .set_query_state(&query, |_| QueryState::Running)
                .await
                .unwrap();
        }

        let drop = DropQuery::new().stop_mode(StopMode::Graceful).blocking();
        let drop_rx = handle.send(drop).await;
        tokio::task::yield_now().await;

        for &id in &query_ids {
            let query = handle
                .catalog
                .query
                .get_query(GetQuery::new().with_id(id))
                .await
                .unwrap()
                .into_iter()
                .next()
                .unwrap();
            assert_eq!(query.desired_state, DesiredQueryState::Stopped);
            handle
                .catalog
                .query
                .set_query_state(&query, |_| QueryState::Stopped)
                .await
                .unwrap();
        }

        let dropped = drop_rx.await.unwrap().unwrap();
        assert_eq!(dropped.len(), 3);
        for model in &dropped {
            assert_eq!(model.current_state, QueryState::Stopped);
            assert!(query_ids.contains(&model.id));
        }
    }
}
