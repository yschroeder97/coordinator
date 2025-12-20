use crate::catalog::query::query::GetQuery;
use crate::catalog::query_builder::ToSql;
use crate::catalog::notification::Notifier;
use tokio::sync::watch;
use super::query::{CreateQuery, DropQuery, QueryId};
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
    notifier_tx: watch::Sender<()>,
    notifier_rx: watch::Receiver<()>,
}

impl Notifier for QueryCatalog {
    type Notification = ();

    fn subscribe(&self) -> watch::Receiver<()> {
        self.notifier_rx.clone()
    }

    fn notify(&self) {
        let _ = self.notifier_tx.send(());
    }
}

impl QueryCatalog {
    pub fn new(db: Arc<Database>) -> Self {
        let (notifier_tx, notifier_rx) = watch::channel(());
        Self {
            db,
            notifier_tx,
            notifier_rx,
        }
    }

    pub async fn create_query(&self, query: &CreateQuery) -> Result<(), QueryCatalogError> {
        let query_sql = sqlx::query!(
            "INSERT INTO queries (id, statement) VALUES (?, ?)",
            query.name,
            query.stmt
        );
        self.db.execute(query_sql).await?;
        self.notify();
        Ok(())
    }

    pub async fn drop_query(
        &self,
        drop_req: &DropQuery,
    ) -> Result<(), QueryCatalogError> {
        let (sql, args) = drop_req.to_sql();
        self.db.update(&sql, args).await?;
        self.notify();
        Ok(())
    }

    pub async fn get_queries(
        &self,
        get_req: &GetQuery,
    ) -> Result<(), QueryCatalogError> {
        let (sql, args) = get_req.to_sql();
        self.db.update(&sql, args).await?;
        self.notify();
        Ok(())
    }
}
