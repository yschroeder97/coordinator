use crate::coordinator::{
    CoordinatorRequest, CreateQueryRequest, CreateWorkerRequest, DropQueryRequest,
    DropWorkerRequest,
};
use crate::message_bus::CoordinatorReceiver;
use anyhow::Result;
use catalog::{Catalog, StateReceivers};
use model::query::active_query;
use model::query::query_state::QueryState;
use model::query::{GetQuery, Query, QueryId};
use model::worker;
use model::worker::endpoint::HostAddr;
use reconciler::request::Request;
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{debug, info, instrument};

macro_rules! dispatch {
    ($req:expr, $catalog:expr, $field:ident . $method:ident) => {{
        debug!("Received: {:?}", $req);
        let Request { payload, reply_to } = $req;
        let _ = reply_to.send($catalog.$field.$method(payload).await.map_err(Into::into));
    }};
}

#[derive(Error, Debug)]
#[error("Query '{query_id}' terminated with state {state} before reaching target state")]
pub struct EarlyTermination {
    pub query_id: QueryId,
    pub state: QueryState,
    pub model: active_query::Model,
}

// Requests that require asynchronous reconciliation are creating/dropping of queries
enum PendingQueryRequest {
    Create(CreateQueryRequest),
    Drop(DropQueryRequest),
}

impl PendingQueryRequest {
    fn reply(self, response: Result<Query>) {
        match self {
            Self::Create(req) => {
                let _ = req.reply_to.send(response);
            }
            Self::Drop(req) => {
                let _ = req.reply_to.send(response);
            }
        }
    }
}

enum PendingWorkerRequest {
    Create(CreateWorkerRequest),
    Drop(DropWorkerRequest),
}

pub(super) struct RequestListener {
    receiver: CoordinatorReceiver,
    catalog: Catalog,
    query_state_rx: mpsc::UnboundedReceiver<active_query::Model>,
    worker_state_rx: mpsc::UnboundedReceiver<worker::Model>,
    pending_query_requests: HashMap<QueryId, PendingQueryRequest>,
    pending_worker_requests: HashMap<HostAddr, PendingWorkerRequest>,
}

impl RequestListener {
    pub(super) fn new(
        receiver: CoordinatorReceiver,
        catalog: Catalog,
        state_receivers: StateReceivers,
    ) -> RequestListener {
        Self {
            receiver,
            catalog,
            query_state_rx: state_receivers.query,
            worker_state_rx: state_receivers.worker,
            pending_query_requests: HashMap::new(),
            pending_worker_requests: HashMap::new(),
        }
    }

    pub(super) async fn run(mut self) {
        loop {
            tokio::select! {
                recv_result = self.receiver.recv() => match recv_result {
                    Some(req) => self.handle_recv(req).await,
                    None => {
                        info!("All clients have been dropped");
                        return;
                    }
                },
                Some(model) = self.query_state_rx.recv() => {
                    self.handle_query_state_change(model);
                },
                Some(model) = self.worker_state_rx.recv() => {
                    self.handle_worker_state_change(model);
                }
            }
        }
    }

    #[instrument(skip(self))]
    fn handle_query_state_change(&mut self, model: active_query::Model) {
        let Some(pending) = self.pending_query_requests.get(&model.id) else {
            return;
        };

        let should_resolve = match pending {
            PendingQueryRequest::Create(req) => {
                let target = req.payload.block_until;
                model.current_state == target || model.current_state.is_terminal()
            }
            PendingQueryRequest::Drop(_) => model.current_state.is_terminal(),
        };

        if !should_resolve {
            return;
        }

        let pending = self.pending_query_requests.remove(&model.id).unwrap();

        // For Create requests that terminated before reaching the target state, return an error
        if let PendingQueryRequest::Create(ref req) = pending {
            if model.current_state.is_terminal() && model.current_state != req.payload.block_until {
                pending.reply(Err(EarlyTermination {
                    query_id: model.id.clone(),
                    state: model.current_state,
                    model,
                }
                .into()));
                return;
            }
        }

        pending.reply(Ok(Query::Active(model)));
    }

    #[instrument(skip(self))]
    fn handle_worker_state_change(&mut self, _model: worker::Model) {
        // TODO: implement worker state change handling
    }

    async fn handle_recv(&mut self, req: CoordinatorRequest) {
        match req {
            CoordinatorRequest::CreateLogicalSource(r) => {
                dispatch!(r, self.catalog, source.create_logical_source)
            }
            CoordinatorRequest::CreatePhysicalSource(r) => {
                dispatch!(r, self.catalog, source.create_physical_source)
            }
            CoordinatorRequest::CreateSink(r) => {
                dispatch!(r, self.catalog, sink.create_sink)
            }
            CoordinatorRequest::DropLogicalSource(r) => {
                dispatch!(r, self.catalog, source.drop_logical_source)
            }
            CoordinatorRequest::DropPhysicalSource(r) => {
                dispatch!(r, self.catalog, source.drop_physical_source)
            }
            CoordinatorRequest::DropSink(r) => {
                dispatch!(r, self.catalog, sink.drop_sink)
            }
            CoordinatorRequest::GetLogicalSource(r) => {
                dispatch!(r, self.catalog, source.get_logical_source)
            }
            CoordinatorRequest::GetPhysicalSource(r) => {
                dispatch!(r, self.catalog, source.get_physical_source)
            }
            CoordinatorRequest::GetSink(r) => {
                dispatch!(r, self.catalog, sink.get_sink)
            }
            CoordinatorRequest::GetQuery(r) => {
                dispatch!(r, self.catalog, query.get_query)
            }
            CoordinatorRequest::GetWorker(r) => {
                dispatch!(r, self.catalog, worker.get_worker)
            }
            CoordinatorRequest::CreateQuery(r) => {
                debug!("Received: {r:?}");
                let Request { payload, reply_to } = r;
                let insert_result = self
                    .catalog
                    .query
                    .create_query(payload.clone())
                    .await
                    .map(Query::Active)
                    .map_err(Into::into);

                match insert_result {
                    Ok(query) => {
                        if payload.block_until == QueryState::Pending {
                            let _ = reply_to.send(Ok(query));
                        } else {
                            // Store the request to be resolved when state changes arrive
                            if let Some(outstanding_req) = self.pending_query_requests.insert(
                                payload.id.clone(),
                                PendingQueryRequest::Create(Request { payload: payload.clone(), reply_to }),
                            ) {
                                let outstanding_query: Query = self
                                    .catalog
                                    .query
                                    .get_query(GetQuery::new().with_id(payload.id))
                                    .await
                                    .expect("Query should be in the catalog")
                                    .into_iter()
                                    .next()
                                    .expect("Query should have at least one entry");
                                // Must be in terminated state, otherwise the catalog insert would have failed
                                assert!(
                                    matches!(outstanding_query, Query::Terminated(_)),
                                    "Old query must be in a terminated state"
                                );
                                outstanding_req.reply(Ok(outstanding_query));
                            }
                        }
                    }
                    Err(e) => {
                        let _ = reply_to.send(Err(e));
                    }
                }
            }
            CoordinatorRequest::CreateWorker(r) => {
                debug!("Received: {r:?}");
                let Request { payload, reply_to } = r;

                let _ = reply_to.send(
                    self.catalog
                        .worker
                        .create_worker(payload)
                        .await
                        .map_err(Into::into),
                );
            }
            CoordinatorRequest::StopQuery(r) => {
                debug!("Received: {r:?}");
                let Request { payload, reply_to } = r;

                let drop_result = self
                    .catalog
                    .query
                    .stop_query(payload.clone())
                    .await
                    .map(Query::Active)
                    .map_err(Into::into);

                match drop_result {
                    Ok(query) => {
                        if !payload.should_block {
                            let _ = reply_to.send(Ok(query));
                        } else {
                            self.pending_query_requests.insert(
                                payload.id.clone(),
                                PendingQueryRequest::Drop(Request { payload, reply_to }),
                            );
                        }
                    }
                    Err(e) => {
                        let _ = reply_to.send(Err(e));
                    }
                }
            }
            CoordinatorRequest::DropWorker(r) => {
                debug!("Received: {r:?}");
                let Request { payload, reply_to } = r;

                if !payload.should_block {
                    let _ = reply_to.send(
                        self.catalog
                            .worker
                            .drop_worker(payload)
                            .await
                            .map_err(Into::into),
                    );
                }
            }
        }
    }
}
