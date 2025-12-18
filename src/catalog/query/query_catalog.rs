use super::query::{CreateQuery, DropQuery, GetQuery, Query, QueryId, QueryState};
use crate::catalog::database::{Database, DatabaseErr};
use crate::catalog::sink::sink::SinkName;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum QueryCatalogError {
    #[error("Query with id '{id}' already exists")]
    QueryAlreadyExists { id: QueryId },

    #[error("Query with id '{id}' not found")]
    QueryNotFound { id: QueryId },

    #[error("Sink '{sink_name}' not found for query '{query_id}'")]
    SinkNotFoundForQuery {
        sink_name: SinkName,
        query_id: QueryId,
    },

    #[error("Invalid query definition: {reason}")]
    InvalidQueryDefinition { reason: String },

    #[error("Database error: {0}")]
    Database(#[from] DatabaseErr),

    #[error("At least one of the predicates must be `Some`")]
    EmptyPredicate {},
}

pub struct QueryCatalog {
    db: Arc<Database>,
}

impl QueryCatalog {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    pub async fn create_query(&self, query: &CreateQuery) -> Result<(), QueryCatalogError> {
        let query_sql = sqlx::query!(
            "INSERT INTO queries (id, statement) VALUES (?, ?)",
            query.name,
            query.stmt
        );

        self.db.insert(query_sql).await?;
        Ok(())
    }

    pub async fn mark_query_for_deletion(
        &self,
        drop_request: &DropQuery,
    ) -> Result<(), QueryCatalogError> {
        todo!()
    }

    pub async fn drop_query(
        &self,
        drop_request: &DropQuery,
    ) -> Result<Vec<Query>, QueryCatalogError> {
        todo!()
    }
}
