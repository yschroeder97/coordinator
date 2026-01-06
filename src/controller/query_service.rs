use super::query::Query;
use crate::catalog::notification::Notifier;
use crate::catalog::query::query_catalog::QueryCatalog;
use crate::catalog::query::{GetQuery, QueryId, QueryState};
use crate::network::poly_join_set::JoinSet;
use crate::network::worker_registry::WorkerRegistryHandle;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::info;

#[derive(Error, Debug)]
pub enum QueryServiceErr {}

pub(crate) struct QueryService<S: super::query::QueryState> {
    catalog: Arc<QueryCatalog>,
    worker_registry: WorkerRegistryHandle,
    queries: HashMap<QueryId, Query<S>>,
    deploying: JoinSet<()>,
    terminating: JoinSet<()>,
}

impl<S: super::query::QueryState> QueryService<S> {
    pub fn new(catalog: Arc<QueryCatalog>, worker_registry: WorkerRegistryHandle) -> Self {
        QueryService {
            catalog,
            worker_registry,
            queries: HashMap::default(),
            deploying: JoinSet::new(),
            terminating: JoinSet::new(),
        }
    }

    pub async fn run(&mut self) -> Result<(), QueryServiceErr> {
        let mut queries = self.catalog.subscribe();
        info!("Starting");

        loop {
            tokio::select! {
                _ = queries.changed() => self.on_client_request().await,
            }
        }
    }

    async fn on_client_request(&mut self) {
        self.add_queries().await;
        self.drop_queries().await;
    }

    async fn add_queries(&mut self) {
        let to_add = self
            .catalog
            .get_queries(&GetQuery::new().with_desired_state(QueryState::Running))
            .await
            .unwrap();

        for query in to_add {}
    }

    async fn drop_queries(&mut self) {
        let to_drop = self
            .catalog
            .get_queries(&GetQuery::new().with_desired_state(QueryState::Stopped))
            .await
            .unwrap();

        for query in to_drop {}
    }
}
