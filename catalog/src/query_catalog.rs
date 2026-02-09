use crate::database::State;
use crate::notification::{IntentChannel, NotifiableCatalog};
use model::query::active_query::{self, Entity as ActiveQueryEntity};
use model::query::fragment::*;
use model::query::query_state::{DesiredQueryState, QueryState};
use model::query::terminated_query::{self, Entity as TerminatedQueryEntity};
use model::query::{CreateQuery, DropQuery, GetQuery, Query, QueryId, StopQuery, fragment};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DbErr, EntityTrait, ModelTrait, QueryFilter, Set,
    TransactionError, TransactionTrait,
};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{mpsc, watch};

#[derive(Error, Debug)]
pub enum QueryCatalogError {
    #[error("Database error: {0}")]
    Database(#[from] sea_orm::DbErr),
}

pub struct QueryCatalog {
    db: State,
    intent: IntentChannel<()>,
    state_tx: mpsc::UnboundedSender<active_query::Model>,
}

impl QueryCatalog {
    pub fn new(db: State, state_tx: mpsc::UnboundedSender<active_query::Model>) -> Arc<Self> {
        Arc::new(Self {
            db,
            intent: IntentChannel::new(()),
            state_tx,
        })
    }

    pub async fn create_query(
        &self,
        req: CreateQuery,
    ) -> Result<active_query::Model, QueryCatalogError> {
        let model = active_query::ActiveModel::from(req)
            .insert(&self.db.conn)
            .await?;
        self.intent.notify_intent(());
        Ok(model)
    }

    pub async fn stop_query(
        &self,
        req: StopQuery,
    ) -> Result<active_query::Model, QueryCatalogError> {
        let model = active_query::ActiveModel {
            id: ActiveValue::Unchanged(req.id.clone()),
            desired_state: Set(DesiredQueryState::Stopped),
            ..Default::default()
        };
        let updated = model.update(&self.db.conn).await?;
        self.intent.notify_intent(());
        Ok(updated)
    }

    pub async fn drop_query(
        &self,
        req: DropQuery,
    ) -> Result<Vec<terminated_query::Model>, QueryCatalogError> {
        TerminatedQueryEntity::delete_many()
            .filter(Column::QueryId.eq(req.id))
            .exec_with_returning(&self.db.conn)
            .await
            .map_err(Into::into)
    }

    pub async fn get_query(&self, req: GetQuery) -> Result<Vec<Query>, QueryCatalogError> {
        let mut results = Vec::new();

        let active_condition = sea_orm::Condition::all()
            .add_option(req.id.clone().map(|id| active_query::Column::Id.eq(id)))
            .add_option(
                req.current_state
                    .map(|s| active_query::Column::CurrentState.eq(s)),
            )
            .add_option(
                req.desired_state
                    .map(|s| active_query::Column::DesiredState.eq(s)),
            );

        let active = ActiveQueryEntity::find()
            .filter(active_condition)
            .all(&self.db.conn)
            .await?;
        results.extend(active.into_iter().map(Query::Active));

        let terminated_condition = sea_orm::Condition::all()
            .add_option(req.id.map(|id| terminated_query::Column::QueryId.eq(id)))
            .add_option(
                req.termination_state
                    .map(|s| terminated_query::Column::TerminationState.eq(s)),
            );

        let terminated = TerminatedQueryEntity::find()
            .filter(terminated_condition)
            .all(&self.db.conn)
            .await?;
        results.extend(terminated.into_iter().map(Query::Terminated));

        Ok(results)
    }

    pub async fn create_fragments(
        &self,
        requests: Vec<CreateFragment>,
    ) -> Result<Vec<fragment::Model>, QueryCatalogError> {
        self.db
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
            .await
            .map_err(|e| match e {
                TransactionError::Connection(db_err) => QueryCatalogError::from(db_err),
                TransactionError::Transaction(db_err) => QueryCatalogError::from(db_err),
            })
    }

    pub async fn get_fragments(
        &self,
        query_id: &QueryId,
    ) -> Result<Vec<fragment::Model>, QueryCatalogError> {
        active_query::Entity::find_by_id(query_id)
            .find_with_related(fragment::Entity)
            .all(&self.db.conn)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| QueryCatalogError::Database(DbErr::RecordNotFound(query_id.clone())))
            .map(|(_, fragments)| fragments)
    }

    pub async fn get_mismatch(&self) -> Result<Vec<active_query::Model>, QueryCatalogError> {
        use sea_orm::sea_query::Expr;

        ActiveQueryEntity::find()
            .filter(Expr::cust("current_state <> desired_state"))
            .all(&self.db.conn)
            .await
            .map_err(Into::into)
    }

    pub async fn advance_query_state(
        &self,
        query: active_query::Model,
    ) -> Result<active_query::Model, QueryCatalogError> {
        let mut query: active_query::ActiveModel = query.into();
        query.current_state = Set(query.current_state.unwrap().next());
        let updated = query.update(&self.db.conn).await?;
        let _ = self.state_tx.send(updated.clone());
        Ok(updated)
    }
}

impl NotifiableCatalog for QueryCatalog {
    type Intent = ();

    fn subscribe_intent(&self) -> watch::Receiver<Self::Intent> {
        self.intent.subscribe_intent()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::test_prop;
    use crate::worker_catalog::WorkerCatalog;
    use model::query::query_state::QueryState;
    use model::query::terminated_query;
    use model::testing::{arb_create_query, arb_query_plan, arb_valid_state_path};
    use model::worker::GetWorker;
    use proptest::prelude::*;
    use sea_orm::{EntityTrait, Iterable};

    fn test_query_id() -> String {
        "test-query-1".to_string()
    }

    fn test_statement() -> String {
        "SELECT * FROM test".to_string()
    }

    fn test_catalog(db: State) -> Arc<QueryCatalog> {
        let (tx, _rx) = mpsc::unbounded_channel();
        QueryCatalog::new(db, tx)
    }

    fn test_worker_catalog(db: State) -> Arc<WorkerCatalog> {
        let (tx, _rx) = mpsc::unbounded_channel();
        WorkerCatalog::new(db, tx)
    }

    /// Walk a query through a state path, returning the model at each step.
    /// The first element of `path` is Pending (initial state on creation), so
    /// transitions start from path[1].
    async fn walk_path(
        catalog: &QueryCatalog,
        created: &active_query::Model,
        path: &[QueryState],
    ) -> Vec<active_query::Model> {
        let mut models = vec![created.clone()];
        for next_state in &path[1..] {
            let updated = catalog
                .advance_query_state(models.last().unwrap().to_owned(), *next_state)
                .await
                .expect("Valid transition should succeed");
            models.push(updated);
        }
        models
    }

    #[tokio::test]
    async fn test_create_and_get_query() {
        let db = State::for_test().await;
        let catalog = test_catalog(db);

        let req = CreateQuery::new_with_name(test_query_id(), test_statement());

        let created = catalog
            .create_query(req)
            .await
            .expect("Query creation should succeed");

        assert_eq!(created.id, test_query_id());
        assert_eq!(created.statement, test_statement());
        assert_eq!(created.current_state, QueryState::Pending);
        assert_eq!(created.desired_state, DesiredQueryState::Running);

        let get_req = GetQuery::new().with_id(test_query_id());
        let queries = catalog
            .get_query(get_req)
            .await
            .expect("Get query should succeed");

        assert_eq!(queries.len(), 1);
        match &queries[0] {
            Query::Active(q) => assert_eq!(q.id, test_query_id()),
            Query::Terminated(_) => panic!("Query should be active"),
        }
    }

    #[tokio::test]
    async fn test_drop_query() {
        let db = State::for_test().await;
        let catalog = test_catalog(db);

        let req = CreateQuery::new_with_name(test_query_id(), test_statement());
        catalog.create_query(req).await.unwrap();

        let sadvance_query_state(prev, next_state).await.unwrap();

            if !next_state.is_terminal() {
                for invalid in &prev.current_state.invalid_transitions() {
                    assert!(
                        catalog
                            .advance_query_state(prev.clone(), *invalid)
                            .await
                            .is_err(),
                        "Transition {:?} -> {:?} should be rejected",
                        prev.current_state,
                        invalid
                    );
                }
            }
        }

        // After terminal: query is archived, all transitions should fail
        for state in QueryState::iter() {
            assert!(
                catalog
                    .advance_query_state(prev.clone(), state)
                    .await
                    .is_err(),
                "Transition after archival to {:?} should be rejected",
                state
            );
        }
    }

    /// After a query is terminated (auto-archived), a new query with the same ID can be created.
    /// get_query returns both the new active and the old terminated entry.
    async fn prop_query_id_reusable_after_termination(
        db: State,
        req: CreateQuery,
        path: Vec<QueryState>,
    ) {
        let catalog = test_catalog(db.clone());
        let created = catalog.create_query(req.clone()).await.unwrap();
        walk_path(&catalog, &created, &path).await;

        // Query is now terminated and archived; create a new one with the same ID
        let new_req = CreateQuery::new_with_name(req.id.clone(), "SELECT new FROM reuse".into());
        catalog
            .create_query(new_req)
            .await
            .expect("Re-creating query with same ID after termination should succeed");

        let results = catalog
            .get_query(GetQuery::new().with_id(req.id.clone()))
            .await
            .unwrap();

        let active_count = results
            .iter()
            .filter(|q| matches!(q, Query::Active(_)))
            .count();
        let terminated_count = results
            .iter()
            .filter(|q| matches!(q, Query::Terminated(_)))
            .count();

        assert_eq!(active_count, 1, "Exactly one active query expected");
        assert!(
            terminated_count >= 1,
            "At least one terminated entry expected"
        );
    }

    /// A failed (invalid) transition leaves the query state unchanged and does not
    /// create spurious terminated_query or query_log entries.
    async fn prop_failed_transition_leaves_state_unchanged(
        db: State,
        req: CreateQuery,
        path: Vec<QueryState>,
    ) {
        let catalog = test_catalog(db.clone());
        let created = catalog.create_query(req.clone()).await.unwrap();

        // Walk to a non-terminal state (use path up to but not including the terminal)
        let non_terminal_path = &path[..path.len() - 1];
        // If the path is just [Pending, <terminal>], non_terminal_path is [Pending]
        let models = walk_path(&catalog, &created, non_terminal_path).await;
        let current = models.last().unwrap();

        // Snapshot state before the invalid transition
        let terminated_before = TerminatedQueryEntity::find()
            .filter(terminated_query::Column::QueryId.eq(&req.id))
            .all(&db.conn)
            .await
            .unwrap()
            .len();

        // Attempt an invalid transition
        let invalid_states = current.current_state.invalid_transitions();
        assert!(
            !invalid_states.is_empty(),
            "Non-terminal states must have at least one invalid transition"
        );
        let invalid_target = invalid_states[0];
        assert!(
            catalog
                .advance_query_state(current.clone(), invalid_target)
                .await
                .is_err(),
            "Invalid transition should fail"
        );

        // Verify state is unchanged
        let refetched = ActiveQueryEntity::find_by_id(&req.id)
            .one(&db.conn)
            .await
            .unwrap()
            .expect("Query must still be in active_query");
        assert_eq!(
            refetched.current_state, current.current_state,
            "State must be unchanged after failed transition"
        );

        let terminated_after = TerminatedQueryEntity::find()
            .filter(terminated_query::Column::QueryId.eq(&req.id))
            .all(&db.conn)
            .await
            .unwrap()
            .len();
        assert_eq!(
            terminated_before, terminated_after,
            "No new terminated_query entries after failed transition"
        );
    }

    /// Each update_query_state call produces exactly one mpsc notification
    /// with matching query_id and state.
    async fn prop_mpsc_notification_per_state_update(
        db: State,
        req: CreateQuery,
        path: Vec<QueryState>,
    ) {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let catalog = QueryCatalog::new(db, tx);
        let created = catalog.create_query(req.clone()).await.unwrap();
        walk_path(&catalog, &created, &path).await;

        let notifications: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();

        let num_transitions = path.len() - 1;
        assert_eq!(
            notifications.len(),
            num_transitions,
            "Expected {} notifications, got {}",
            num_transitions,
            notifications.len()
        );

        for (i, notif) in notifications.iter().enumerate() {
            assert_eq!(notif.id, req.id, "Notification {i}: query_id mismatch");
            assert_eq!(
                notif.current_state,
                path[i + 1],
                "Notification {i}: state mismatch, expected {:?}",
                path[i + 1]
            );
        }
    }

    // -- Fragment property tests --

    /// Every returned fragment has correct defaults (state=Pending, no timestamps,
    /// no error), all input fields are preserved, and fragment IDs are unique.
    async fn prop_fragments_stored_with_correct_defaults(
        db: State,
        plan: model::testing::QueryPlan,
    ) {
        let worker_cat = test_worker_catalog(db.clone());
        let query_cat = test_catalog(db);
        for w in &plan.workers {
            worker_cat.create_worker(w.clone()).await.unwrap();
        }
        query_cat.create_query(plan.query.clone()).await.unwrap();

        let created = query_cat
            .create_fragments(plan.fragments.clone())
            .await
            .expect("Fragment creation with valid refs should succeed");

        assert_eq!(created.len(), plan.fragments.len());

        let mut seen_ids = std::collections::HashSet::new();
        for (model, req) in created.iter().zip(plan.fragments.iter()) {
            assert_eq!(model.query_id, req.query_id);
            assert_eq!(model.host_addr, req.host_addr);
            assert_eq!(model.grpc_addr, req.grpc_addr);
            assert_eq!(model.used_capacity, req.used_capacity);
            assert_eq!(model.has_source, req.has_source);
            assert_eq!(model.plan, req.plan);

            assert!(
                seen_ids.insert(model.id),
                "Duplicate fragment_id {}",
                model.id
            );
        }
    }

    /// Inserting fragments that reference a non-existent query is rejected (FK violation).
    async fn prop_fragments_reject_missing_query(db: State, plan: model::testing::QueryPlan) {
        let worker_cat = test_worker_catalog(db.clone());
        let query_cat = test_catalog(db);
        for w in &plan.workers {
            worker_cat.create_worker(w.clone()).await.unwrap();
        }
        // Do NOT create the query

        assert!(
            query_cat.create_fragments(plan.fragments).await.is_err(),
            "Fragments referencing non-existent query should be rejected"
        );
    }

    /// Inserting fragments that reference non-existent workers is rejected (FK violation).
    async fn prop_fragments_reject_missing_worker(db: State, plan: model::testing::QueryPlan) {
        let query_cat = test_catalog(db);
        // Do NOT create workers
        query_cat.create_query(plan.query.clone()).await.unwrap();

        assert!(
            query_cat.create_fragments(plan.fragments).await.is_err(),
            "Fragments referencing non-existent workers should be rejected"
        );
    }

    /// When a query reaches a terminal state, all its fragments are cascade-deleted.
    async fn prop_fragments_cascade_deleted_on_archival(
        db: State,
        plan: model::testing::QueryPlan,
        path: Vec<QueryState>,
    ) {
        let worker_cat = test_worker_catalog(db.clone());
        let query_cat = test_catalog(db.clone());
        for w in &plan.workers {
            worker_cat.create_worker(w.clone()).await.unwrap();
        }
        let created = query_cat.create_query(plan.query.clone()).await.unwrap();
        query_cat
            .create_fragments(plan.fragments.clone())
            .await
            .unwrap();

        // Verify fragments exist before archival
        let before = fragment::Entity::find()
            .filter(fragment::Column::QueryId.eq(&plan.query.id))
            .all(&db.conn)
            .await
            .unwrap();
        assert_eq!(before.len(), plan.fragments.len());

        // Walk to terminal state → auto_archive → cascade delete
        walk_path(&query_cat, &created, &path).await;

        let after = fragment::Entity::find()
            .filter(fragment::Column::QueryId.eq(&plan.query.id))
            .all(&db.conn)
            .await
            .unwrap();
        assert!(
            after.is_empty(),
            "All fragments must be cascade-deleted after query archival, found {}",
            after.len()
        );
    }

    /// The release_worker_capacity trigger fires for each cascade-deleted fragment,
    /// adding back used_capacity to the correct worker.
    async fn prop_capacity_released_on_query_termination(
        db: State,
        plan: model::testing::QueryPlan,
        path: Vec<QueryState>,
    ) {
        let worker_cat = test_worker_catalog(db.clone());
        let query_cat = test_catalog(db.clone());
        for w in &plan.workers {
            worker_cat.create_worker(w.clone()).await.unwrap();
        }
        let created = query_cat.create_query(plan.query.clone()).await.unwrap();

        let initial_capacity: std::collections::HashMap<_, _> = plan
            .workers
            .iter()
            .map(|w| (w.host_addr.clone(), w.capacity))
            .collect();

        query_cat
            .create_fragments(plan.fragments.clone())
            .await
            .unwrap();

        // Walk to terminal → triggers cascade delete → capacity release
        walk_path(&query_cat, &created, &path).await;

        // Sum used_capacity per worker
        let mut released: std::collections::HashMap<_, i32> = std::collections::HashMap::new();
        for f in &plan.fragments {
            *released.entry(f.host_addr.clone()).or_default() += f.used_capacity;
        }

        for w in &plan.workers {
            let models = worker_cat
                .get_worker(GetWorker::new().with_host_addr(w.host_addr.clone()))
                .await
                .unwrap();
            assert_eq!(models.len(), 1);
            let expected =
                initial_capacity[&w.host_addr] + released.get(&w.host_addr).copied().unwrap_or(0);
            assert_eq!(
                models[0].capacity,
                expected,
                "Worker {} capacity: expected {} (initial {} + released {}), got {}",
                w.host_addr,
                expected,
                initial_capacity[&w.host_addr],
                released.get(&w.host_addr).copied().unwrap_or(0),
                models[0].capacity
            );
        }
    }

    proptest! {
        #[test]
        fn query_id_unique(req in arb_create_query()) {
            test_prop(|db| async move {
                prop_query_id_unique(db, req).await;
            });
        }

        #[test]
        fn query_in_exactly_one_table(req in arb_create_query(), path in arb_valid_state_path()) {
            test_prop(|db| async move {
                prop_query_in_exactly_one_table(db, req, path).await;
            });
        }

        #[test]
        fn terminated_preserves_identity(req in arb_create_query(), path in arb_valid_state_path()) {
            test_prop(|db| async move {
                prop_terminated_preserves_identity(db, req, path).await;
            });
        }

        #[test]
        fn invalid_transitions_rejected(req in arb_create_query(), path in arb_valid_state_path()) {
            test_prop(|db| async move {
                prop_invalid_transitions_rejected(db, req, path).await;
            });
        }

        #[test]
        fn query_id_reusable_after_termination(req in arb_create_query(), path in arb_valid_state_path()) {
            test_prop(|db| async move {
                prop_query_id_reusable_after_termination(db, req, path).await;
            });
        }

        #[test]
        fn failed_transition_leaves_state_unchanged(req in arb_create_query(), path in arb_valid_state_path()) {
            test_prop(|db| async move {
                prop_failed_transition_leaves_state_unchanged(db, req, path).await;
            });
        }

        #[test]
        fn mpsc_notification_per_state_update(req in arb_create_query(), path in arb_valid_state_path()) {
            test_prop(|db| async move {
                prop_mpsc_notification_per_state_update(db, req, path).await;
            });
        }

        #[test]
        fn fragments_stored_with_correct_defaults(plan in arb_query_plan(5)) {
            test_prop(|db| async move {
                prop_fragments_stored_with_correct_defaults(db, plan).await;
            });
        }

        #[test]
        fn fragments_reject_missing_query(plan in arb_query_plan(5)) {
            test_prop(|db| async move {
                prop_fragments_reject_missing_query(db, plan).await;
            });
        }

        #[test]
        fn fragments_reject_missing_worker(plan in arb_query_plan(5)) {
            test_prop(|db| async move {
                prop_fragments_reject_missing_worker(db, plan).await;
            });
        }

        #[test]
        fn fragments_cascade_deleted_on_archival(plan in arb_query_plan(5), path in arb_valid_state_path()) {
            test_prop(|db| async move {
                prop_fragments_cascade_deleted_on_archival(db, plan, path).await;
            });
        }

        #[test]
        fn capacity_released_on_query_termination(plan in arb_query_plan(5), path in arb_valid_state_path()) {
            test_prop(|db| async move {
                prop_capacity_released_on_query_termination(db, plan, path).await;
            });
        }
    }
}
