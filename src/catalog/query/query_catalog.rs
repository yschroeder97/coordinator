use super::{
    ActiveQuery, CreateQuery, DropQuery, GetQuery, MarkQuery, QueryId, QueryLogEntry,
    TerminatedQuery,
};
use crate::catalog::database::{Database, DatabaseErr};
use crate::catalog::notification::Notifier;
use crate::catalog::query_builder::ToSql;
use crate::catalog::sink::SinkName;
use sqlx::sqlite::SqliteArguments;
use sqlx::Arguments;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;

#[derive(Error, Debug)]
pub enum QueryCatalogErr {
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

    pub async fn create_query(&self, query: &CreateQuery) -> Result<(), QueryCatalogErr> {
        let query_sql = sqlx::query!(
            "INSERT INTO active_queries (id, statement) VALUES (?, ?)",
            query.name,
            query.stmt
        );
        self.db.execute(query_sql).await?;
        self.notify();
        Ok(())
    }

    pub async fn drop_query(&self, drop_req: &DropQuery) -> Result<(), QueryCatalogErr> {
        let (sql, args) = drop_req.to_sql();
        self.db.update(&sql, args).await?;
        self.notify();
        Ok(())
    }

    /// This triggers an insert into the `terminated_queries` table.
    /// For more info, refer to ./migrations/003_triggers.sql.
    /// Therefore, this "moves" rather than deletes.
    pub(crate) async fn move_to_terminated(&self, name: &QueryId) -> Result<(), QueryCatalogErr> {
        self.db
            .execute(sqlx::query!(
                "DELETE FROM active_queries WHERE id = ?",
                name,
            ))
            .await?;

        Ok(())
    }

    pub(crate) async fn get_terminated_queries(
        &self,
    ) -> Result<Vec<TerminatedQuery>, QueryCatalogErr> {
        self.db
            .select(
                "SELECT * FROM terminated_queries",
                SqliteArguments::default(),
            )
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn get_log_for_query(
        &self,
        name: &QueryId,
    ) -> Result<Vec<QueryLogEntry>, QueryCatalogErr> {
        let sql = "SELECT * FROM query_changelog WHERE query_id = ? ORDER BY timestamp";
        let mut args = SqliteArguments::default();
        args.add(name);

        self.db.select(sql, args).await.map_err(Into::into)
    }

    pub(crate) async fn get_active_queries(
        &self,
        get_req: &GetQuery,
    ) -> Result<Vec<ActiveQuery>, QueryCatalogErr> {
        let (sql, args) = get_req.to_sql();
        self.db.select(&sql, args).await.map_err(Into::into)
    }

    pub(crate) async fn get_mismatch(&self) -> Result<Vec<ActiveQuery>, QueryCatalogErr> {
        self.db
            .select(
                "SELECT * FROM active_queries WHERE current_state != desired_state",
                SqliteArguments::default(),
            )
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn update_query_state(&self, mark: &MarkQuery) -> Result<(), QueryCatalogErr> {
        let (sql, args) = mark.to_sql();
        self.db.update(&sql, args).await?;
        self.notify();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::test_utils::{test_prop, InvalidQueryTransition, ValidQueryTransitionPath};
    use crate::catalog::query::{QueryState, CreateQuery, MarkQuery, GetQuery};
    use quickcheck_macros::quickcheck;

    #[quickcheck]
    fn query_id_is_unique(req: CreateQuery) -> bool {
        test_prop(|catalog| async move {
            catalog
                .query
                .create_query(&req)
                .await
                .expect("First query creation should succeed");

            assert!(
                catalog.query.create_query(&req).await.is_err(),
                "Duplicate query id '{}' should be rejected",
                req.name
            );
        })
    }

    #[quickcheck]
    fn query_insert_has_changelog_entry(create_query: CreateQuery) {
        test_prop(|catalog| async move {
            // This should have the side effect that we have a corresponding entry in the `query_log` table.
            catalog
                .query
                .create_query(&create_query)
                .await
                .expect("Insert should succeed");

            let log = catalog
                .query
                .get_log_for_query(&create_query.name)
                .await
                .expect("Changelog fetch should succeed");

            assert_eq!(
                log.len(),
                1,
                "Log should have length of 1 after a single insert"
            );
            let entry = log.first().unwrap();
            assert_eq!(entry.query_id, create_query.name);
            assert_eq!(entry.statement, create_query.stmt);
            assert_eq!(entry.current_state, QueryState::Pending);
            assert_eq!(entry.desired_state, QueryState::Running);
        });
    }

    #[quickcheck]
    fn query_transition_stopped(create_query: CreateQuery) {
        test_prop(|catalog| async move {
            catalog.query.create_query(&create_query).await.unwrap();

            // Mark query as Stopped first (trigger constraint)
            catalog
                .query
                .update_query_state(&MarkQuery {
                    id: create_query.name.clone(),
                    new_current: QueryState::Stopped,
                })
                .await
                .expect("State update should succeed");

            // This should have two side effects:
            // 1. Row is deleted from `active_queries`
            // 2. New entry in the `terminated_queries` table
            catalog
                .query
                .move_to_terminated(&create_query.name)
                .await
                .expect("Move should succeed");

            let active = catalog
                .query
                .get_active_queries(&GetQuery::default())
                .await
                .unwrap();
            let terminated = catalog.query.get_terminated_queries().await.unwrap();

            assert!(
                active.is_empty(),
                "After the move, no query should be active"
            );
            assert_eq!(terminated.len(), 1);
            let t = terminated.first().unwrap();
            assert_eq!(t.id.as_ref().unwrap(), &create_query.name);
            assert_eq!(t.termination_state, QueryState::Stopped);
        });
    }

    #[quickcheck]
    fn changelog_tracks_all_updates(create_query: CreateQuery, path: ValidQueryTransitionPath) {
        test_prop(|catalog| async move {
            catalog
                .query
                .create_query(&create_query)
                .await
                .expect("Create failed");

            for state in &path.states {
                catalog
                    .query
                    .update_query_state(&MarkQuery {
                        id: create_query.name.clone(),
                        new_current: *state,
                    })
                    .await
                    .expect("Valid update failed");
            }

            let log = catalog
                .query
                .get_log_for_query(&create_query.name)
                .await
                .expect("Get log failed");
            assert_eq!(
                log.len(),
                1 + path.states.len(),
                "Log should contain creation + all updates"
            );
        });
    }

    #[quickcheck]
    fn changelog_preserves_statement(create_query: CreateQuery, path: ValidQueryTransitionPath) {
        test_prop(|catalog| async move {
            catalog
                .query
                .create_query(&create_query)
                .await
                .expect("Create failed");

            for state in &path.states {
                catalog
                    .query
                    .update_query_state(&MarkQuery {
                        id: create_query.name.clone(),
                        new_current: *state,
                    })
                    .await
                    .expect("Valid update failed");
            }

            let log = catalog
                .query
                .get_log_for_query(&create_query.name)
                .await
                .expect("Get log failed");
            for entry in log {
                assert_eq!(
                    entry.statement, create_query.stmt,
                    "Statement in changelog mutated"
                );
            }
        });
    }

    #[quickcheck]
    fn changelog_is_monotonic(create_query: CreateQuery, path: ValidQueryTransitionPath) {
        test_prop(|catalog| async move {
            catalog
                .query
                .create_query(&create_query)
                .await
                .expect("Create failed");

            for state in &path.states {
                catalog
                    .query
                    .update_query_state(&MarkQuery {
                        id: create_query.name.clone(),
                        new_current: *state,
                    })
                    .await
                    .expect("Valid update failed");
            }

            let log = catalog
                .query
                .get_log_for_query(&create_query.name)
                .await
                .expect("Get log failed");

            for window in log.windows(2) {
                let (prev, next) = (&window[0], &window[1]);
                assert!(
                    prev.timestamp <= next.timestamp,
                    "Changelog not monotonic: {:?} > {:?}",
                    prev.timestamp,
                    next.timestamp
                );
            }
        });
    }

    #[quickcheck]
    fn invalid_state_transitions_rejected(
        create_query: CreateQuery,
        invalid: InvalidQueryTransition,
    ) {
        test_prop(|catalog| async move {
            catalog
                .query
                .create_query(&create_query)
                .await
                .expect("Create failed");

            // Now we are at 'invalid.from'. Try to update to 'invalid.to'.
            let result = catalog
                .query
                .update_query_state(&MarkQuery {
                    id: create_query.name.clone(),
                    new_current: invalid.to,
                })
                .await;

            assert!(
                result.is_err(),
                "Invalid transition {:?} -> {:?} should have been rejected",
                invalid.from,
                invalid.to
            );
        });
    }
}
