use crate::database::Database;
use crate::notification::{NotifiableCatalog, NotificationChannel};
use anyhow::Result;
use model::query::fragment::CreateFragment;
use model::query::query_state::{DesiredQueryState, QueryState};
use model::query::{CreateQuery, DropQuery, GetQuery, QueryId, fragment};
use model::{IntoCondition, query};
use sea_orm::sea_query::Expr;
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set, TransactionTrait};
use std::sync::Arc;
use tokio::sync::watch;

pub struct QueryCatalog {
    db: Database,
    notifications: NotificationChannel,
}

impl QueryCatalog {
    pub fn new(db: Database) -> Arc<Self> {
        Arc::new(Self {
            db,
            notifications: NotificationChannel::new(),
        })
    }

    pub async fn create_query(&self, req: CreateQuery) -> Result<query::Model> {
        let model = query::ActiveModel::from(req).insert(&self.db.conn).await?;
        self.notifications.notify_intent();
        Ok(model)
    }

    pub async fn drop_query(&self, req: DropQuery) -> Result<Vec<query::Model>> {
        let updated = query::Entity::update_many()
            .col_expr(
                query::Column::DesiredState,
                Expr::value(DesiredQueryState::Stopped),
            )
            .col_expr(query::Column::StopMode, Expr::value(req.stop_mode))
            .filter(req.filters.clone().into_condition())
            .exec_with_returning(&self.db.conn)
            .await?;
        self.notifications.notify_intent();
        Ok(updated)
    }

    pub async fn get_queries_by_id<I>(&self, ids: I) -> Result<Vec<query::Model>>
    where
        I: IntoIterator<Item = QueryId>,
    {
        Ok(query::Entity::find()
            .filter(query::Column::Id.is_in(ids))
            .all(&self.db.conn)
            .await?)
    }

    pub async fn get_query(&self, req: GetQuery) -> Result<Vec<query::Model>> {
        Ok(query::Entity::find()
            .filter(req.into_condition())
            .all(&self.db.conn)
            .await?)
    }

    pub async fn create_fragments(
        &self,
        requests: Vec<CreateFragment>,
    ) -> Result<Vec<fragment::Model>> {
        Ok(self
            .db
            .conn
            .transaction::<_, Vec<fragment::Model>, sea_orm::DbErr>(|txn| {
                Box::pin(async move {
                    fragment::Entity::insert_many(
                        requests.into_iter().map(fragment::ActiveModel::from),
                    )
                    .exec_with_returning(txn)
                    .await
                })
            })
            .await?)
    }

    pub async fn get_fragments(&self, query_id: i64) -> Result<Vec<fragment::Model>> {
        let (_, fragments) = query::Entity::find_by_id(query_id)
            .find_with_related(fragment::Entity)
            .all(&self.db.conn)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("Query with id {query_id} not found"))?;
        Ok(fragments)
    }

    pub async fn get_mismatch(&self) -> Result<Vec<query::Model>> {
        Ok(query::Entity::find()
            .filter(Expr::cust("current_state <> desired_state"))
            .all(&self.db.conn)
            .await?)
    }

    pub async fn advance_query_state(&self, query: query::Model) -> Result<query::Model> {
        self.set_query_state(query, |current| current.next()).await
    }

    pub async fn set_query_state(
        &self,
        query: query::Model,
        f: impl FnOnce(QueryState) -> QueryState,
    ) -> Result<query::Model> {
        let new_state = f(query.current_state);
        let mut query: query::ActiveModel = query.into();
        query.current_state = Set(new_state);
        let updated = query.update(&self.db.conn).await?;
        self.notifications.notify_state();
        Ok(updated)
    }
}

impl NotifiableCatalog for QueryCatalog {
    fn subscribe_intent(&self) -> watch::Receiver<()> {
        self.notifications.subscribe_intent()
    }

    fn subscribe_state(&self) -> watch::Receiver<()> {
        self.notifications.subscribe_state()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Catalog;
    use crate::test_utils::test_prop;
    use model::query::StopMode;
    use model::query::fragment::FragmentState;
    use model::query::query_state::QueryState;
    use model::testing::{arb_create_query, arb_fragment_setup, arb_valid_state_path};
    use proptest::proptest;

    fn test_query_name() -> String {
        "test-query-1".to_string()
    }

    fn test_statement() -> String {
        "SELECT * FROM test".to_string()
    }

    /// Walk a query through a state path, returning the model at each step.
    /// The first element of `path` is Pending (initial state on creation), so
    /// transitions start from path[1].
    async fn walk_path(
        catalog: &QueryCatalog,
        created: &query::Model,
        path: &[QueryState],
    ) -> Vec<query::Model> {
        let mut models = vec![created.clone()];
        for next_state in &path[1..] {
            let target = *next_state;
            let updated = catalog
                .set_query_state(models.last().unwrap().to_owned(), |_| target)
                .await
                .expect("Valid transition should succeed");
            models.push(updated);
        }
        models
    }

    // -- Unit tests --

    #[tokio::test]
    async fn test_create_and_get_query() {
        let catalog = Catalog::for_test().await.query.clone();

        let req = CreateQuery::new(test_statement()).name(test_query_name());

        let created = catalog
            .create_query(req)
            .await
            .expect("Query creation should succeed");

        assert_eq!(created.name, test_query_name());
        assert_eq!(created.statement, test_statement());
        assert_eq!(created.current_state, QueryState::Pending);
        assert_eq!(created.desired_state, DesiredQueryState::Completed);

        let get_req = GetQuery::new().with_name(test_query_name());
        let queries = catalog
            .get_query(get_req)
            .await
            .expect("Get query should succeed");

        assert_eq!(queries.len(), 1);
    }

    #[tokio::test]
    async fn test_drop_query() {
        let catalog = Catalog::for_test().await.query.clone();

        let req = CreateQuery::new(test_statement()).name(test_query_name());
        catalog.create_query(req).await.unwrap();

        let drop_req = DropQuery::new().with_filters(GetQuery::new().with_name(test_query_name()));
        let updated = catalog.drop_query(drop_req).await.unwrap();

        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0].desired_state, DesiredQueryState::Stopped);
        assert_eq!(updated[0].stop_mode, Some(StopMode::Graceful));
    }

    // -- Query state property tests --

    /// Walking a valid state path results in the expected state at each step,
    /// and the query remains retrievable throughout.
    async fn prop_query_state_path_valid(req: CreateQuery, path: Vec<QueryState>) {
        let catalog = Catalog::for_test().await.query.clone();
        let created = catalog.create_query(req.clone()).await.unwrap();
        let models = walk_path(&catalog, &created, &path).await;

        for (model, expected_state) in models.iter().zip(path.iter()) {
            assert_eq!(model.current_state, *expected_state);
        }

        // Query is still retrievable
        let results = catalog
            .get_query(GetQuery::new().with_id(created.id))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].current_state, *path.last().unwrap());
    }

    /// After reaching a terminal state, the query's name and statement are preserved.
    async fn prop_terminal_state_preserves_identity(req: CreateQuery, path: Vec<QueryState>) {
        let catalog = Catalog::for_test().await.query.clone();
        let created = catalog.create_query(req.clone()).await.unwrap();
        let models = walk_path(&catalog, &created, &path).await;
        let final_model = models.last().unwrap();

        assert_eq!(final_model.name, req.name);
        assert_eq!(final_model.statement, req.sql_statement);
        assert!(final_model.current_state.is_terminal());
    }

    /// Invalid state transitions are rejected by the DB trigger. The query state
    /// is unchanged after a failed transition attempt.
    async fn prop_invalid_transitions_rejected(req: CreateQuery, path: Vec<QueryState>) {
        let catalog = Catalog::for_test().await.query.clone();
        let created = catalog.create_query(req.clone()).await.unwrap();

        // Walk to a non-terminal state (all but the last element of path)
        let non_terminal_path = &path[..path.len() - 1];
        let models = walk_path(&catalog, &created, non_terminal_path).await;
        let current = models.last().unwrap();

        let invalid_states = current.current_state.invalid_transitions();
        for invalid_target in invalid_states {
            assert!(
                catalog
                    .set_query_state(current.clone(), |_| invalid_target)
                    .await
                    .is_err(),
                "Transition {:?} -> {:?} should be rejected",
                current.current_state,
                invalid_target
            );
        }

        // Verify state is unchanged
        let refetched = catalog
            .get_query(GetQuery::new().with_id(created.id))
            .await
            .unwrap();
        assert_eq!(refetched.len(), 1);
        assert_eq!(
            refetched[0].current_state, current.current_state,
            "State must be unchanged after failed transition"
        );
    }

    // -- Fragment property tests --

    /// Every returned fragment has correct defaults (state=Pending, no timestamps,
    /// no error), all input fields are preserved, and fragment IDs are unique.
    async fn prop_fragments_stored_with_correct_defaults(
        req: CreateQuery,
        setup: model::testing::FragmentSetup,
    ) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let created_query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(created_query.id);

        let created = catalog
            .query
            .create_fragments(fragment_reqs.clone())
            .await
            .expect("Fragment creation with valid refs should succeed");

        assert_eq!(created.len(), fragment_reqs.len());

        let mut seen_ids = std::collections::HashSet::new();
        for (model, req) in created.iter().zip(fragment_reqs.iter()) {
            assert_eq!(model.query_id, created_query.id);
            assert_eq!(model.host_addr, req.host_addr);
            assert_eq!(model.grpc_addr, req.grpc_addr);
            assert_eq!(model.used_capacity, req.used_capacity);
            assert_eq!(model.has_source, req.has_source);
            assert_eq!(model.plan, req.plan);
            assert_eq!(model.current_state, FragmentState::Pending);
            assert!(model.start_timestamp.is_none());
            assert!(model.stop_timestamp.is_none());
            assert!(model.error.is_none());

            assert!(
                seen_ids.insert(model.id),
                "Duplicate fragment_id {}",
                model.id
            );
        }
    }

    /// Inserting fragments that reference a non-existent query is rejected (FK violation).
    async fn prop_fragments_reject_missing_query(setup: model::testing::FragmentSetup) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        // Do NOT create the query — use a non-existent query_id
        let fragment_reqs = setup.create_fragments(i64::MAX);

        assert!(
            catalog.query.create_fragments(fragment_reqs).await.is_err(),
            "Fragments referencing non-existent query should be rejected"
        );
    }

    /// Inserting fragments that reference non-existent workers is rejected (FK violation).
    async fn prop_fragments_reject_missing_worker(
        req: CreateQuery,
        setup: model::testing::FragmentSetup,
    ) {
        let catalog = Catalog::for_test().await.query.clone();
        // Do NOT create workers
        let created_query = catalog.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(created_query.id);

        assert!(
            catalog.create_fragments(fragment_reqs).await.is_err(),
            "Fragments referencing non-existent workers should be rejected"
        );
    }

    /// Total worker capacity is conserved across the query lifecycle:
    /// fragment creation reserves capacity, query terminal state releases it.
    async fn prop_capacity_conserved_on_query_terminal(
        req: CreateQuery,
        setup: model::testing::FragmentSetup,
        path: Vec<QueryState>,
    ) {
        let catalog = Catalog::for_test().await;

        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let initial_total: i32 = setup.workers.iter().map(|w| w.capacity).sum();

        // Create query and fragments (reserve_worker_capacity trigger fires)
        let created_query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(created_query.id);
        catalog.query.create_fragments(fragment_reqs).await.unwrap();

        // Walk to terminal state (release_worker_capacity trigger fires)
        walk_path(&catalog.query, &created_query, &path).await;

        let workers = catalog
            .worker
            .get_worker(model::worker::GetWorker::all())
            .await
            .unwrap();
        let final_total: i32 = workers.iter().map(|w| w.capacity).sum();

        assert_eq!(
            initial_total, final_total,
            "Total capacity must be conserved: initial={}, final={}",
            initial_total, final_total,
        );
    }

    proptest! {
        #[test]
        fn query_state_path_valid(req in arb_create_query(), path in arb_valid_state_path()) {
            test_prop(|| async move {
                prop_query_state_path_valid(req, path).await;
            });
        }

        #[test]
        fn terminal_state_preserves_identity(req in arb_create_query(), path in arb_valid_state_path()) {
            test_prop(|| async move {
                prop_terminal_state_preserves_identity(req, path).await;
            });
        }

        #[test]
        fn invalid_transitions_rejected(req in arb_create_query(), path in arb_valid_state_path()) {
            test_prop(|| async move {
                prop_invalid_transitions_rejected(req, path).await;
            });
        }

        #[test]
        fn fragments_stored_with_correct_defaults(req in arb_create_query(), setup in arb_fragment_setup(5)) {
            test_prop(|| async move {
                prop_fragments_stored_with_correct_defaults(req, setup).await;
            });
        }

        #[test]
        fn fragments_reject_missing_query(setup in arb_fragment_setup(5)) {
            test_prop(|| async move {
                prop_fragments_reject_missing_query(setup).await;
            });
        }

        #[test]
        fn fragments_reject_missing_worker(req in arb_create_query(), setup in arb_fragment_setup(5)) {
            test_prop(|| async move {
                prop_fragments_reject_missing_worker(req, setup).await;
            });
        }

        #[test]
        fn capacity_conserved_on_query_terminal(
            req in arb_create_query(),
            setup in arb_fragment_setup(5),
            path in arb_valid_state_path(),
        ) {
            test_prop(|| async move {
                prop_capacity_conserved_on_query_terminal(req, setup, path).await;
            });
        }
    }
}
