use super::{
    ActiveQuery, CreateQuery, DropQuery, GetQuery, MarkQuery, QueryId, QueryLogEntry,
    TerminatedQuery,
};
use crate::catalog::database::{Database, DatabaseErr};
use crate::catalog::notification::Notifier;
use crate::catalog::query::fragment::{
    CreateQueryFragment, FragmentId, GetFragment, MarkFragment, QueryFragment,
};
use crate::catalog::query_builder::{InsertBuilder, ToSql};
use crate::catalog::sink::SinkName;
use crate::catalog::tables::{query_fragments, table};
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

    pub(crate) async fn get_fragments(
        &self,
        get_fragments: &GetFragment,
    ) -> Result<Vec<QueryFragment>, QueryCatalogErr> {
        let (sql, args) = get_fragments.to_sql();
        self.db.select(&sql, args).await.map_err(Into::into)
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
        args.add(name)
            .map_err(|e| DatabaseErr::Database(sqlx::Error::Protocol(e.to_string())))?;

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

    pub(crate) async fn move_to_next_state(&self, mark: &MarkQuery) -> Result<(), QueryCatalogErr> {
        let (sql, args) = mark.to_sql();
        self.db.update(&sql, args).await?;
        self.notify();
        Ok(())
    }

    pub(crate) async fn update_fragment_states(
        &self,
        marks: &[MarkFragment],
    ) -> Result<(), QueryCatalogErr> {
        if marks.is_empty() {
            return Ok(());
        }

        // Build a batch update using CASE expression:
        // UPDATE query_fragments SET current_state = CASE rowid
        //   WHEN id1 THEN 'State1'
        //   WHEN id2 THEN 'State2'
        // END WHERE rowid IN (id1, id2)
        let case_branches: Vec<String> = marks
            .iter()
            .map(|m| format!("WHEN {} THEN '{}'", m.id, m.new_state))
            .collect();

        let ids: Vec<String> = marks.iter().map(|m| m.id.to_string()).collect();

        let sql = format!(
            "UPDATE query_fragments SET current_state = CASE rowid {} END WHERE rowid IN ({})",
            case_branches.join(" "),
            ids.join(", ")
        );

        self.db.execute(sqlx::query(&sql)).await?;
        Ok(())
    }

    pub async fn insert_fragments(
        &self,
        fragments: &Vec<CreateQueryFragment>,
    ) -> Result<Vec<FragmentId>, QueryCatalogErr> {
        if fragments.is_empty() {
            return Ok(Vec::new());
        }

        let mut builder = InsertBuilder::new(
            table::QUERY_FRAGMENTS,
            &[
                query_fragments::QUERY_ID,
                query_fragments::HOST_NAME,
                query_fragments::GRPC_PORT,
                query_fragments::CURRENT_STATE,
                query_fragments::DESIRED_STATE,
                query_fragments::PLAN,
                query_fragments::USED_CAPACITY,
            ],
        );

        for fragment in fragments {
            builder.add_row(|row| {
                row.push(fragment.query_id.clone())
                    .push(fragment.host_name.clone())
                    .push(fragment.grpc_port)
                    .push(fragment.current_state.clone())
                    .push(fragment.desired_state.clone())
                    .push(fragment.plan.clone())
                    .push(fragment.used_capacity);
            });
        }

        builder.returning(&["rowid"]);

        let (sql, args) = builder.into_parts();
        self.db.select_scalar(&sql, args).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::query::query_state::QueryStateTransitionType;
    use crate::catalog::query::{CreateQuery, DesiredQueryState, GetQuery, MarkQuery, QueryState};
    use crate::catalog::test_utils::{arb_create_query, arb_state_sequence, test_prop};
    use crate::catalog::Catalog;
    use proptest::proptest;

    async fn prop_query_id_unique(catalog: Catalog, req: CreateQuery) {
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
    }

    async fn prop_insert_has_changelog_entry(catalog: Catalog, create_query: CreateQuery) {
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
        assert_eq!(entry.desired_state, DesiredQueryState::Running);
    }

    async fn prop_query_lifecycle(catalog: Catalog, create_query: CreateQuery) {
        catalog.query.create_query(&create_query).await.unwrap();

        catalog
            .query
            .move_to_next_state(&MarkQuery {
                id: create_query.name.clone(),
                new_state: QueryState::Planned,
            })
            .await
            .expect("State update to Planned should succeed");

        catalog
            .query
            .move_to_next_state(&MarkQuery {
                id: create_query.name.clone(),
                new_state: QueryState::Registered,
            })
            .await
            .expect("State update to Registered should succeed");

        catalog
            .query
            .move_to_next_state(&MarkQuery {
                id: create_query.name.clone(),
                new_state: QueryState::Running,
            })
            .await
            .expect("State update to Running should succeed");

        // Mark query as Stopped first (trigger constraint)
        catalog
            .query
            .move_to_next_state(&MarkQuery {
                id: create_query.name.clone(),
                new_state: QueryState::Stopped,
            })
            .await
            .expect("State update to Stopped should succeed");

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
    }

    async fn prop_changelog_tracks_all_updates(
        catalog: Catalog,
        create_query: CreateQuery,
        path: Vec<QueryState>,
    ) {
        catalog
            .query
            .create_query(&create_query)
            .await
            .expect("Create failed");

        for state in &path {
            catalog
                .query
                .move_to_next_state(&MarkQuery {
                    id: create_query.name.clone(),
                    new_state: *state,
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
            1 + path.len(),
            "Log should contain creation + all updates"
        );
    }

    async fn prop_changelog_is_monotonic(
        catalog: Catalog,
        create_query: CreateQuery,
        path: Vec<QueryState>,
    ) {
        catalog
            .query
            .create_query(&create_query)
            .await
            .expect("Valid query creation should not fail");

        for state in &path {
            catalog
                .query
                .move_to_next_state(&MarkQuery {
                    id: create_query.name.clone(),
                    new_state: *state,
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
    }

    async fn prop_illegal_transitions_rejected(
        catalog: Catalog,
        create_query: CreateQuery,
        path: Vec<QueryState>,
    ) {
        assert!(
            path.len() >= 2,
            "BUG: Path should always have a length at least 2, but was {:?}",
            path
        );

        catalog
            .query
            .create_query(&create_query)
            .await
            .expect("Valid query creation should not fail");
        let inserted = catalog
            .query
            .get_active_queries(&GetQuery::new().with_id(create_query.name.clone()))
            .await
            .expect("Query should be in the catalog");
        assert_eq!(
            inserted.len(),
            1,
            "Query should have a single corresponding row"
        );
        assert_eq!(
            inserted.first().unwrap().current_state,
            QueryState::Pending,
            "Query should initially be in Pending state"
        );

        for window in path[..path.len() - 1].windows(2) {
            let (_prev, next) = (&window[0], &window[1]);
            catalog
                .query
                .move_to_next_state(&MarkQuery {
                    id: create_query.name.clone(),
                    new_state: *next,
                })
                .await
                .expect("Valid query state transition should succeed");
        }

        let last_transition = catalog
            .query
            .move_to_next_state(&MarkQuery {
                id: create_query.name,
                new_state: *path.last().unwrap(),
            })
            .await;
        assert!(
            last_transition.is_err(),
            "Last transition {} -> {} is illegal, but succeeded",
            path[path.len() - 2],
            path.last().unwrap()
        );
    }

    proptest! {
        #[test]
        fn query_id_unique(req in arb_create_query()) {
            test_prop(|catalog| async move {
                prop_query_id_unique(catalog, req).await;
            });
        }

        #[test]
        fn insert_has_changelog_entry(req in arb_create_query()) {
            test_prop(|catalog| async move {
                prop_insert_has_changelog_entry(catalog, req).await;
            })
        }

        #[test]
        fn query_transition_stopped(req in arb_create_query()) {
            test_prop(|catalog| async move {
                prop_query_lifecycle(catalog, req).await;
            })
        }

        #[test]
        fn changelog_tracks_all_updates(req in arb_create_query(), state_changes in arb_state_sequence(QueryStateTransitionType::Valid)) {
            test_prop(|catalog| async move {
                prop_changelog_tracks_all_updates(catalog, req, state_changes).await;
            })
        }

        #[test]
        fn changelog_is_monotonic(req in arb_create_query(), state_changes in arb_state_sequence(QueryStateTransitionType::Valid)) {
            test_prop(|catalog| async move {
                prop_changelog_is_monotonic(catalog, req, state_changes).await;
            })
        }

        #[test]
        fn illegal_transitions_rejected(req in arb_create_query(), state_changes in arb_state_sequence(QueryStateTransitionType::Invalid)) {
            test_prop(|catalog| async move {
                prop_illegal_transitions_rejected(catalog, req, state_changes).await;
            })
        }
    }
}
