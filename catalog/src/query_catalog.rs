use crate::database::Database;
use crate::notification::{NotificationChannel, Reconcilable};
use anyhow::Result;
use model::query::QueryError;
use model::query::fragment::{CreateFragment, FragmentError, FragmentId, FragmentUpdate};
use model::query::query_state::{DesiredQueryState, QueryState};
use model::query::{CreateQuery, DropQuery, GetQuery, fragment};
use model::{IntoCondition, query};
use sea_orm::sea_query::{CaseStatement, Expr};
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set, TransactionTrait};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;

pub struct QueryCatalog {
    db: Database,
    listeners: NotificationChannel,
}

impl QueryCatalog {
    pub fn from(db: Database) -> Arc<Self> {
        Arc::new(Self {
            db,
            listeners: NotificationChannel::new(),
        })
    }

    pub async fn create_query(&self, req: CreateQuery) -> Result<query::Model> {
        let model = self
            .db
            .with_retry(|conn| {
                let req = req.clone();
                async move { query::ActiveModel::from(req).insert(&conn).await }
            })
            .await?;
        self.listeners.notify_intent();
        Ok(model)
    }

    pub async fn drop_query(&self, req: DropQuery) -> Result<Vec<query::Model>> {
        let updated = self
            .db
            .with_retry(|conn| {
                let req = req.clone();
                async move {
                    conn.transaction::<_, Vec<query::Model>, sea_orm::DbErr>(|txn| {
                        Box::pin(async move {
                            query::Entity::update_many()
                                .col_expr(
                                    query::Column::DesiredState,
                                    Expr::value(DesiredQueryState::Stopped),
                                )
                                .col_expr(
                                    query::Column::StopMode,
                                    Expr::value(req.stop_mode),
                                )
                                .filter(req.filters.clone().into_condition())
                                .exec(txn)
                                .await?;
                            query::Entity::find()
                                .filter(req.filters.into_condition())
                                .all(txn)
                                .await
                        })
                    })
                    .await
                    .map_err(|e| match e {
                        sea_orm::TransactionError::Connection(e) => e,
                        sea_orm::TransactionError::Transaction(e) => e,
                    })
                }
            })
            .await?;
        self.listeners.notify_intent();
        Ok(updated)
    }

    pub async fn get_query(&self, req: GetQuery) -> Result<Vec<query::Model>> {
        Ok(self
            .db
            .with_retry(|conn| {
                let req = req.clone();
                async move {
                    query::Entity::find()
                        .filter(req.into_condition())
                        .all(&conn)
                        .await
                }
            })
            .await?)
    }

    pub async fn create_fragments(
        &self,
        query: &query::Model,
        requests: Vec<CreateFragment>,
    ) -> Result<(query::Model, Vec<fragment::Model>)> {
        assert!(!requests.is_empty(), "Query requires at least one fragment");
        
        let result = self
            .db
            .with_retry(|conn| {
                let requests = requests.clone();
                let query = query.clone();
                async move {
                    conn.transaction::<_, (query::Model, Vec<fragment::Model>), sea_orm::DbErr>(
                        |txn| {
                            Box::pin(async move {
                                fragment::Entity::insert_many(
                                    requests.into_iter().map(fragment::ActiveModel::from),
                                )
                                .exec(txn)
                                .await?;
                                
                                let mut am: query::ActiveModel = query.into();
                                am.current_state = Set(QueryState::Planned);
                                let updated = am.update(txn).await?;
                                let fragments = fragment::Entity::find()
                                    .filter(fragment::Column::QueryId.eq(updated.id))
                                    .all(txn)
                                    .await?;
                                Ok((updated, fragments))
                            })
                        },
                    )
                    .await
                    .map_err(|e| match e {
                        sea_orm::TransactionError::Connection(e) => e,
                        sea_orm::TransactionError::Transaction(e) => e,
                    })
                }
            })
            .await?;
        self.listeners.notify_state();
        Ok(result)
    }

    pub async fn get_fragments(&self, query_id: i64) -> Result<Vec<fragment::Model>> {
        let (_, fragments) = self
            .db
            .with_retry(|conn| async move {
                query::Entity::find_by_id(query_id)
                    .find_with_related(fragment::Entity)
                    .all(&conn)
                    .await
            })
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("Query with id {query_id} not found"))?;
        Ok(fragments)
    }

    pub async fn fail_query(&self, query: query::Model, error: QueryError) -> Result<query::Model> {
        let updated = self
            .db
            .with_retry(|conn| {
                let query = query.clone();
                let error = error.clone();
                async move {
                    let mut am: query::ActiveModel = query.into();
                    am.current_state = Set(QueryState::Failed);
                    am.error = Set(Some(error));
                    am.update(&conn).await
                }
            })
            .await?;
        self.listeners.notify_state();
        Ok(updated)
    }

    pub async fn update_fragment_states(
        &self,
        query_id: i64,
        updates: Vec<FragmentUpdate>,
    ) -> Result<query::Model> {
        let result = self
            .db
            .with_retry(|conn| {
                let updates = updates.clone();
                async move {
                    conn.transaction::<_, query::Model, sea_orm::DbErr>(|txn| {
                        Box::pin(async move {
                            let pre_state = query::Entity::find_by_id(query_id)
                                .one(txn)
                                .await?
                                .ok_or(sea_orm::DbErr::RecordNotFound(format!(
                                    "Query {query_id} not found"
                                )))?
                                .current_state;

                            if !updates.is_empty() {
                                let ids: Vec<FragmentId> = updates.iter().map(|u| u.id).collect();

                                let mut state_case = CaseStatement::new();
                                let mut start_ts_case = CaseStatement::new();
                                let mut stop_ts_case = CaseStatement::new();
                                let mut error_case = CaseStatement::new();
                                let mut has_start_ts = false;
                                let mut has_stop_ts = false;
                                let mut has_errors = false;

                                for u in &updates {
                                    state_case = state_case
                                        .case(fragment::Column::Id.eq(u.id), Expr::value(u.state));
                                    if let Some(ref ts) = u.start_timestamp {
                                        start_ts_case = start_ts_case.case(
                                            fragment::Column::Id.eq(u.id),
                                            Expr::value(ts.to_rfc3339()),
                                        );
                                        has_start_ts = true;
                                    }
                                    if let Some(ref ts) = u.stop_timestamp {
                                        stop_ts_case = stop_ts_case.case(
                                            fragment::Column::Id.eq(u.id),
                                            Expr::value(ts.to_rfc3339()),
                                        );
                                        has_stop_ts = true;
                                    }
                                    if let Some(ref error) = u.error {
                                        error_case = error_case.case(
                                            fragment::Column::Id.eq(u.id),
                                            Expr::value(serde_json::to_value(error).unwrap()),
                                        );
                                        has_errors = true;
                                    }
                                }

                                let mut update = fragment::Entity::update_many()
                                    .col_expr(fragment::Column::CurrentState, state_case.into())
                                    .filter(fragment::Column::Id.is_in(ids));

                                if has_start_ts {
                                    start_ts_case = start_ts_case
                                        .finally(Expr::col(fragment::Column::StartTimestamp));
                                    update = update.col_expr(
                                        fragment::Column::StartTimestamp,
                                        start_ts_case.into(),
                                    );
                                }
                                if has_stop_ts {
                                    stop_ts_case = stop_ts_case
                                        .finally(Expr::col(fragment::Column::StopTimestamp));
                                    update = update.col_expr(
                                        fragment::Column::StopTimestamp,
                                        stop_ts_case.into(),
                                    );
                                }

                                if has_errors {
                                    error_case =
                                        error_case.finally(Expr::col(fragment::Column::Error));
                                    update =
                                        update.col_expr(fragment::Column::Error, error_case.into());
                                }

                                update.exec(txn).await?;
                            }

                            let updated_query = query::Entity::find_by_id(query_id)
                                .one(txn)
                                .await?
                                .ok_or(sea_orm::DbErr::RecordNotFound(format!(
                                    "Query {query_id} not found"
                                )))?;

                            if updated_query.current_state == QueryState::Failed
                                && pre_state != QueryState::Failed
                            {
                                let failed_fragments = fragment::Entity::find()
                                    .filter(fragment::Column::QueryId.eq(query_id))
                                    .filter(fragment::Column::Error.is_not_null())
                                    .all(txn)
                                    .await?;

                                let errors: HashMap<FragmentId, FragmentError> = failed_fragments
                                    .into_iter()
                                    .filter_map(|f| f.error.map(|e| (f.id, e)))
                                    .collect();

                                if !errors.is_empty() {
                                    let query_error = match pre_state {
                                        QueryState::Planned => QueryError::Registering(errors),
                                        QueryState::Registered => QueryError::Starting(errors),
                                        _ => QueryError::Running(errors),
                                    };
                                    let mut am: query::ActiveModel = updated_query.into();
                                    am.error = Set(Some(query_error));
                                    return am.update(txn).await;
                                }
                            }

                            Ok(updated_query)
                        })
                    })
                    .await
                    .map_err(|e| match e {
                        sea_orm::TransactionError::Connection(e) => e,
                        sea_orm::TransactionError::Transaction(e) => e,
                    })
                }
            })
            .await?;
        self.listeners.notify_state();
        Ok(result)
    }

    pub async fn set_query_state(
        &self,
        query: &query::Model,
        f: impl FnOnce(QueryState) -> QueryState,
    ) -> Result<query::Model> {
        let new_state = f(query.current_state);
        let updated = self
            .db
            .with_retry(|conn| {
                let query = query.clone();
                async move {
                    let mut am: query::ActiveModel = query.into();
                    am.current_state = Set(new_state);
                    am.update(&conn).await
                }
            })
            .await?;
        self.listeners.notify_state();
        Ok(updated)
    }
}

impl Reconcilable for QueryCatalog {
    type Model = query::Model;

    fn subscribe_intent(&self) -> watch::Receiver<()> {
        self.listeners.subscribe_intent()
    }

    fn subscribe_state(&self) -> watch::Receiver<()> {
        self.listeners.subscribe_state()
    }

    async fn get_mismatch(&self) -> Result<Vec<query::Model>> {
        Ok(self
            .db
            .with_retry(|conn| async move {
                query::Entity::find()
                    .filter(Expr::cust("current_state <> desired_state"))
                    .all(&conn)
                    .await
            })
            .await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Catalog;
    use crate::test_utils::test_prop;
    use model::query::StopMode;
    use model::query::fragment::{FragmentError, FragmentState, FragmentUpdate};
    use model::query::query_state::QueryState;
    use model::testing::{
        arb_create_query, arb_create_worker, arb_fragment_setup, arb_valid_state_path,
    };
    use model::worker::{CreateWorker, GetWorker};
    use proptest::prelude::*;
    use sea_orm::sqlx::types::chrono;

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
                .set_query_state(models.last().unwrap(), |_| target)
                .await
                .expect("Valid transition should succeed");
            models.push(updated);
        }
        models
    }

    async fn prop_create_and_get_query(req: CreateQuery) {
        let catalog = Catalog::for_test().await.query.clone();
        let created = catalog.create_query(req.clone()).await.unwrap();

        assert_eq!(created.name, req.name);
        assert_eq!(created.statement, req.sql_statement);
        assert_eq!(created.current_state, QueryState::Pending);
        assert_eq!(created.desired_state, DesiredQueryState::Completed);

        let queries = catalog
            .get_query(GetQuery::new().with_name(req.name))
            .await
            .unwrap();
        assert_eq!(queries.len(), 1);
    }

    async fn prop_drop_query(req: CreateQuery, stop_mode: StopMode) {
        let catalog = Catalog::for_test().await.query.clone();
        catalog.create_query(req.clone()).await.unwrap();

        let drop_req = DropQuery::new()
            .stop_mode(stop_mode)
            .with_filters(GetQuery::new().with_name(req.name));
        let updated = catalog.drop_query(drop_req).await.unwrap();

        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0].desired_state, DesiredQueryState::Stopped);
        assert_eq!(updated[0].stop_mode, Some(stop_mode));
    }

    async fn prop_get_fragments_missing_query_errors(req: CreateQuery) {
        let catalog = Catalog::for_test().await;
        catalog.query.create_query(req).await.unwrap();
        assert!(
            catalog.query.get_fragments(i64::MAX).await.is_err(),
            "get_fragments for non-existent query should error"
        );
    }

    async fn prop_update_fragment_states_empty_noop(req: CreateQuery) {
        let catalog = Catalog::for_test().await;
        let query = catalog.query.create_query(req).await.unwrap();
        catalog
            .query
            .update_fragment_states(query.id, vec![])
            .await
            .expect("Empty update should succeed as no-op");
    }

    async fn prop_fragment_negative_capacity_rejected(worker: CreateWorker, req: CreateQuery) {
        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(worker.clone()).await.unwrap();
        let query = catalog.query.create_query(req).await.unwrap();

        assert!(
            catalog
                .query
                .create_fragments(
                    &query,
                    vec![CreateFragment {
                        query_id: query.id,
                        host_addr: worker.host_addr,
                        grpc_addr: worker.grpc_addr,
                        plan: serde_json::json!({}),
                        used_capacity: -1,
                        has_source: false,
                    }]
                )
                .await
                .is_err(),
            "Fragment with negative used_capacity should be rejected"
        );
    }

    async fn prop_fragment_exceeding_capacity_rejected(worker: CreateWorker, req: CreateQuery) {
        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(worker.clone()).await.unwrap();
        let query = catalog.query.create_query(req).await.unwrap();

        assert!(
            catalog
                .query
                .create_fragments(
                    &query,
                    vec![CreateFragment {
                        query_id: query.id,
                        host_addr: worker.host_addr,
                        grpc_addr: worker.grpc_addr,
                        plan: serde_json::json!({}),
                        used_capacity: worker.capacity + 1,
                        has_source: false,
                    }]
                )
                .await
                .is_err(),
            "Fragment exceeding worker capacity should be rejected"
        );
    }

    async fn prop_fragment_exactly_exhausts_capacity(worker: CreateWorker, req: CreateQuery) {
        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(worker.clone()).await.unwrap();
        let query = catalog.query.create_query(req).await.unwrap();

        let half = worker.capacity / 2;
        let rest = worker.capacity - half;

        catalog
            .query
            .create_fragments(
                &query,
                vec![
                    CreateFragment {
                        query_id: query.id,
                        host_addr: worker.host_addr.clone(),
                        grpc_addr: worker.grpc_addr.clone(),
                        plan: serde_json::json!({}),
                        used_capacity: half,
                        has_source: false,
                    },
                    CreateFragment {
                        query_id: query.id,
                        host_addr: worker.host_addr.clone(),
                        grpc_addr: worker.grpc_addr.clone(),
                        plan: serde_json::json!({}),
                        used_capacity: rest,
                        has_source: false,
                    },
                ],
            )
            .await
            .expect("Fragments exactly exhausting capacity should succeed");

        let workers = catalog
            .worker
            .get_worker(GetWorker::all().with_host_addr(worker.host_addr))
            .await
            .unwrap();
        assert_eq!(workers[0].capacity, 0);
    }

    async fn prop_fragment_zero_capacity(worker: CreateWorker, req: CreateQuery) {
        let zero_worker = CreateWorker::new(worker.host_addr.clone(), worker.grpc_addr.clone(), 0);
        let catalog = Catalog::for_test().await;
        catalog.worker.create_worker(zero_worker).await.unwrap();
        let query = catalog.query.create_query(req).await.unwrap();

        catalog
            .query
            .create_fragments(
                &query,
                vec![CreateFragment {
                    query_id: query.id,
                    host_addr: worker.host_addr,
                    grpc_addr: worker.grpc_addr,
                    plan: serde_json::json!({}),
                    used_capacity: 0,
                    has_source: false,
                }],
            )
            .await
            .expect("Fragment with zero capacity on zero-capacity worker should succeed");
    }

    async fn prop_fragment_creation_reserves_capacity(
        req: CreateQuery,
        setup: model::testing::FragmentSetup,
    ) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let initial_capacities: std::collections::HashMap<_, _> = setup
            .workers
            .iter()
            .map(|w| (w.host_addr.clone(), w.capacity))
            .collect();

        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let expected_usage: std::collections::HashMap<_, i32> =
            fragment_reqs.iter().fold(std::collections::HashMap::new(), |mut acc, f| {
                *acc.entry(f.host_addr.clone()).or_default() += f.used_capacity;
                acc
            });

        catalog
            .query
            .create_fragments(&query, fragment_reqs)
            .await
            .unwrap();

        let workers = catalog
            .worker
            .get_worker(GetWorker::all())
            .await
            .unwrap();
        for w in &workers {
            let initial = initial_capacities.get(&w.host_addr).copied().unwrap_or(0);
            let used = expected_usage.get(&w.host_addr).copied().unwrap_or(0);
            assert_eq!(
                w.capacity,
                initial - used,
                "Worker {} capacity should be {} - {} = {}",
                w.host_addr,
                initial,
                used,
                initial - used
            );
        }
    }

    async fn prop_capacity_released_on_fragment_terminal(
        req: CreateQuery,
        setup: model::testing::FragmentSetup,
        terminal_state: FragmentState,
    ) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let initial_total: i32 = setup.workers.iter().map(|w| w.capacity).sum();

        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let total_used: i32 = fragment_reqs.iter().map(|f| f.used_capacity).sum();
        let (_, fragments) = catalog
            .query
            .create_fragments(&query, fragment_reqs)
            .await
            .unwrap();

        let current_total = || {
            let catalog = catalog.clone();
            async move {
                let workers = catalog.worker.get_worker(GetWorker::all()).await.unwrap();
                workers.iter().map(|w| w.capacity).sum::<i32>()
            }
        };

        assert_eq!(
            current_total().await,
            initial_total - total_used,
            "Capacity should be reduced after fragment creation"
        );

        walk_fragments_to(&catalog.query, query.id, &fragments, FragmentState::Running).await;
        assert_eq!(
            current_total().await,
            initial_total - total_used,
            "Capacity should be unchanged during non-terminal transitions"
        );

        let terminal_updates: Vec<_> = fragments
            .iter()
            .map(|f| FragmentUpdate {
                id: f.id,
                state: terminal_state,
                ..Default::default()
            })
            .collect();
        catalog
            .query
            .update_fragment_states(query.id, terminal_updates)
            .await
            .unwrap();
        assert_eq!(
            current_total().await,
            initial_total,
            "Capacity should be fully restored after terminal state"
        );
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
                    .set_query_state(&current, |_| invalid_target)
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

        let (_, created) = catalog
            .query
            .create_fragments(&created_query, fragment_reqs.clone())
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
        let fake_query = query::Model {
            id: i64::MAX,
            name: "fake".to_string(),
            statement: "fake".to_string(),
            current_state: QueryState::Pending,
            desired_state: DesiredQueryState::Completed,
            start_timestamp: None,
            stop_timestamp: None,
            stop_mode: None,
            error: None,
        };
        let fragment_reqs = setup.create_fragments(i64::MAX);

        assert!(
            catalog
                .query
                .create_fragments(&fake_query, fragment_reqs)
                .await
                .is_err(),
            "Fragments referencing non-existent query should be rejected"
        );
    }

    /// Inserting fragments that reference non-existent workers is rejected (FK violation).
    async fn prop_fragments_reject_missing_worker(
        req: CreateQuery,
        setup: model::testing::FragmentSetup,
    ) {
        let catalog = Catalog::for_test().await.query.clone();
        let created_query = catalog.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(created_query.id);

        assert!(
            catalog
                .create_fragments(&created_query, fragment_reqs)
                .await
                .is_err(),
            "Fragments referencing non-existent workers should be rejected"
        );
    }

    async fn prop_capacity_conserved_on_fragment_terminal(
        req: CreateQuery,
        setup: model::testing::FragmentSetup,
        terminal_state: FragmentState,
    ) {
        let catalog = Catalog::for_test().await;

        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let initial_total: i32 = setup.workers.iter().map(|w| w.capacity).sum();

        let created_query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(created_query.id);
        let (_, fragments) = catalog
            .query
            .create_fragments(&created_query, fragment_reqs)
            .await
            .unwrap();

        walk_fragments_to(&catalog.query, created_query.id, &fragments, terminal_state).await;

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

    async fn walk_fragments_to(
        catalog: &QueryCatalog,
        query_id: i64,
        fragments: &[fragment::Model],
        target: FragmentState,
    ) {
        let path = match target {
            FragmentState::Pending => vec![],
            FragmentState::Registered => vec![FragmentState::Registered],
            FragmentState::Started => vec![FragmentState::Registered, FragmentState::Started],
            FragmentState::Running => {
                vec![
                    FragmentState::Registered,
                    FragmentState::Started,
                    FragmentState::Running,
                ]
            }
            FragmentState::Completed => vec![
                FragmentState::Registered,
                FragmentState::Started,
                FragmentState::Running,
                FragmentState::Completed,
            ],
            FragmentState::Stopped => vec![FragmentState::Stopped],
            FragmentState::Failed => vec![FragmentState::Failed],
        };
        for state in path {
            let updates: Vec<_> = fragments
                .iter()
                .map(|f| FragmentUpdate {
                    id: f.id,
                    state,
                    ..Default::default()
                })
                .collect();
            catalog
                .update_fragment_states(query_id, updates)
                .await
                .unwrap();
        }
    }

    fn arb_fragment_state() -> impl Strategy<Value = FragmentState> {
        prop_oneof![
            Just(FragmentState::Pending),
            Just(FragmentState::Registered),
            Just(FragmentState::Started),
            Just(FragmentState::Running),
            Just(FragmentState::Completed),
            Just(FragmentState::Stopped),
            Just(FragmentState::Failed),
        ]
    }

    fn arb_forward_fragment_state() -> impl Strategy<Value = FragmentState> {
        prop_oneof![
            Just(FragmentState::Started),
            Just(FragmentState::Running),
            Just(FragmentState::Completed),
            Just(FragmentState::Stopped),
            Just(FragmentState::Failed),
        ]
    }

    // -- fail_query property tests --

    async fn prop_fail_query_from_non_terminal(
        req: CreateQuery,
        path: Vec<QueryState>,
        error_msg: String,
    ) {
        let catalog = Catalog::for_test().await;
        let created = catalog.query.create_query(req).await.unwrap();

        let non_terminal_path = &path[..path.len() - 1];
        let models = walk_path(&catalog.query, &created, non_terminal_path).await;
        let current = models.last().unwrap().clone();

        let error = QueryError::Planning(error_msg.clone());
        let failed = catalog
            .query
            .fail_query(current.clone(), error.clone())
            .await
            .expect("fail_query from non-terminal state should succeed");

        assert_eq!(failed.current_state, QueryState::Failed);
        assert_eq!(failed.error, Some(error));
        assert_eq!(failed.name, current.name);
        assert_eq!(failed.statement, current.statement);

        let refetched = catalog
            .query
            .get_query(GetQuery::new().with_id(created.id))
            .await
            .unwrap();
        assert_eq!(refetched[0].current_state, QueryState::Failed);
        assert_eq!(refetched[0].error, Some(QueryError::Planning(error_msg)));
    }

    async fn prop_fail_query_from_terminal_rejected(req: CreateQuery, path: Vec<QueryState>) {
        let catalog = Catalog::for_test().await;
        let created = catalog.query.create_query(req).await.unwrap();

        let models = walk_path(&catalog.query, &created, &path).await;
        let terminal = models.last().unwrap().clone();

        let error = QueryError::Planning("should fail".to_string());
        assert!(
            catalog
                .query
                .fail_query(terminal.clone(), error)
                .await
                .is_err(),
            "fail_query from {:?} should be rejected",
            terminal.current_state
        );
    }

    async fn prop_update_fragment_states_applied(
        req: CreateQuery,
        setup: model::testing::FragmentSetup,
        target_state: FragmentState,
    ) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let (_, created) = catalog
            .query
            .create_fragments(&query, fragment_reqs)
            .await
            .unwrap();

        walk_fragments_to(&catalog.query, query.id, &created, target_state).await;

        let fetched = catalog.query.get_fragments(query.id).await.unwrap();
        for fragment in &fetched {
            assert_eq!(
                fragment.current_state, target_state,
                "Fragment {} should be {:?} but was {:?}",
                fragment.id, target_state, fragment.current_state
            );
        }
    }

    async fn prop_update_fragment_states_partial(
        req: CreateQuery,
        setup: model::testing::FragmentSetup,
        target_state: FragmentState,
    ) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let (_, created) = catalog
            .query
            .create_fragments(&query, fragment_reqs)
            .await
            .unwrap();

        walk_fragments_to(&catalog.query, query.id, &created, FragmentState::Running).await;

        let updated_ids: std::collections::HashSet<FragmentId> = created
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == 0)
            .map(|(_, f)| f.id)
            .collect();

        let updates: Vec<FragmentUpdate> = updated_ids
            .iter()
            .map(|&id| FragmentUpdate {
                id,
                state: target_state,
                ..Default::default()
            })
            .collect();

        catalog
            .query
            .update_fragment_states(query.id, updates)
            .await
            .unwrap();

        let fetched = catalog.query.get_fragments(query.id).await.unwrap();
        for fragment in &fetched {
            let expected = if updated_ids.contains(&fragment.id) {
                target_state
            } else {
                FragmentState::Running
            };
            assert_eq!(
                fragment.current_state, expected,
                "Fragment {} should be {:?} but was {:?}",
                fragment.id, expected, fragment.current_state
            );
        }
    }

    async fn prop_get_mismatch_query_correctness(queries: Vec<CreateQuery>) {
        let catalog = Catalog::for_test().await;

        let mut created = Vec::new();
        for req in &queries {
            created.push(catalog.query.create_query(req.clone()).await.unwrap());
        }

        for (i, query) in created.clone().into_iter().enumerate() {
            if i % 2 == 0 {
                let planned = catalog
                    .query
                    .set_query_state(&query, |_| QueryState::Planned)
                    .await
                    .unwrap();
                let registered = catalog
                    .query
                    .set_query_state(&planned, |_| QueryState::Registered)
                    .await
                    .unwrap();
                let running = catalog
                    .query
                    .set_query_state(&registered, |_| QueryState::Running)
                    .await
                    .unwrap();
                catalog
                    .query
                    .set_query_state(&running, |_| QueryState::Completed)
                    .await
                    .unwrap();
            }
        }

        let mismatched = catalog.query.get_mismatch().await.unwrap();
        let all_queries = catalog.query.get_query(GetQuery::new()).await.unwrap();

        let mismatched_ids: std::collections::HashSet<_> =
            mismatched.iter().map(|q| q.id).collect();

        for query in &all_queries {
            let is_mismatched = query.current_state.to_string() != query.desired_state.to_string();
            let in_result = mismatched_ids.contains(&query.id);

            assert_eq!(
                is_mismatched,
                in_result,
                "Query '{}' (id={}): current={:?}, desired={:?}, \
                 is_mismatched={}, in_result={}",
                query.name,
                query.id,
                query.current_state,
                query.desired_state,
                is_mismatched,
                in_result
            );
        }
    }

    async fn prop_create_fragments_transitions_to_planned(
        req: CreateQuery,
        setup: model::testing::FragmentSetup,
    ) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let query = catalog.query.create_query(req).await.unwrap();
        assert_eq!(query.current_state, QueryState::Pending);

        let fragment_reqs = setup.create_fragments(query.id);
        let (updated_query, _) = catalog
            .query
            .create_fragments(&query, fragment_reqs)
            .await
            .unwrap();

        assert_eq!(updated_query.current_state, QueryState::Planned);

        let refetched = catalog
            .query
            .get_query(GetQuery::new().with_id(query.id))
            .await
            .unwrap();
        assert_eq!(refetched[0].current_state, QueryState::Planned);
    }

    async fn prop_fragment_state_derives_query_state(
        req: CreateQuery,
        setup: model::testing::FragmentSetup,
    ) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let (_, fragments) = catalog
            .query
            .create_fragments(&query, fragment_reqs)
            .await
            .unwrap();

        let check_derived = |catalog: &Catalog, expected: QueryState| {
            let catalog = catalog.clone();
            let query_id = query.id;
            async move {
                let q = catalog
                    .query
                    .get_query(GetQuery::new().with_id(query_id))
                    .await
                    .unwrap();
                assert_eq!(
                    q[0].current_state, expected,
                    "Expected query state {:?} but got {:?}",
                    expected, q[0].current_state
                );
            }
        };

        let advance_all = |catalog: &Catalog, state: FragmentState| {
            let catalog = catalog.clone();
            let fragments = fragments.clone();
            let query_id = query.id;
            async move {
                let updates: Vec<_> = fragments
                    .iter()
                    .map(|f| FragmentUpdate {
                        id: f.id,
                        state,
                        ..Default::default()
                    })
                    .collect();
                catalog
                    .query
                    .update_fragment_states(query_id, updates)
                    .await
                    .unwrap();
            }
        };

        advance_all(&catalog, FragmentState::Registered).await;
        check_derived(&catalog, QueryState::Registered).await;

        advance_all(&catalog, FragmentState::Started).await;
        check_derived(&catalog, QueryState::Running).await;

        advance_all(&catalog, FragmentState::Running).await;
        check_derived(&catalog, QueryState::Running).await;

        advance_all(&catalog, FragmentState::Completed).await;
        check_derived(&catalog, QueryState::Completed).await;
    }

    async fn prop_one_failed_fragment_fails_query(
        req: CreateQuery,
        setup: model::testing::FragmentSetup,
    ) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let (_, fragments) = catalog
            .query
            .create_fragments(&query, fragment_reqs)
            .await
            .unwrap();

        walk_fragments_to(&catalog.query, query.id, &fragments, FragmentState::Running).await;

        let mut updates: Vec<FragmentUpdate> = fragments
            .iter()
            .map(|f| FragmentUpdate {
                id: f.id,
                state: FragmentState::Running,
                ..Default::default()
            })
            .collect();
        updates[0] = FragmentUpdate {
            id: fragments[0].id,
            state: FragmentState::Failed,
            error: Some(FragmentError::WorkerCommunication {
                msg: "connection lost".to_string(),
            }),
            ..Default::default()
        };

        let result = catalog
            .query
            .update_fragment_states(query.id, updates)
            .await
            .unwrap();
        assert_eq!(result.current_state, QueryState::Failed);
        assert!(result.error.is_some(), "Query should have aggregated error");

        match result.error.unwrap() {
            QueryError::Running(errors) => {
                assert!(
                    errors.contains_key(&fragments[0].id),
                    "Error map should contain the failed fragment"
                );
            }
            other => panic!("Expected QueryError::Running, got {:?}", other),
        }
    }

    async fn prop_error_variant_matches_pre_state(
        req: CreateQuery,
        setup: model::testing::FragmentSetup,
        pre_state: QueryState,
    ) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let query = catalog.query.create_query(req).await.unwrap();
        let reqs = setup.create_fragments(query.id);
        let (_, frags) = catalog.query.create_fragments(&query, reqs).await.unwrap();

        match pre_state {
            QueryState::Registered => {
                walk_fragments_to(
                    &catalog.query,
                    query.id,
                    &frags,
                    FragmentState::Registered,
                )
                .await;
            }
            QueryState::Running => {
                walk_fragments_to(
                    &catalog.query,
                    query.id,
                    &frags,
                    FragmentState::Running,
                )
                .await;
            }
            _ => {}
        }

        let updates = vec![FragmentUpdate {
            id: frags[0].id,
            state: FragmentState::Failed,
            error: Some(FragmentError::WorkerCommunication {
                msg: "fail".to_string(),
            }),
            ..Default::default()
        }];
        let result = catalog
            .query
            .update_fragment_states(query.id, updates)
            .await
            .unwrap();
        assert_eq!(result.current_state, QueryState::Failed);
        let error = result.error.expect("Should have aggregated error");
        match pre_state {
            QueryState::Planned => assert!(
                matches!(error, QueryError::Registering(_)),
                "From Planned: expected Registering, got {:?}",
                error
            ),
            QueryState::Registered => assert!(
                matches!(error, QueryError::Starting(_)),
                "From Registered: expected Starting, got {:?}",
                error
            ),
            _ => assert!(
                matches!(error, QueryError::Running(_)),
                "From {:?}: expected Running, got {:?}",
                pre_state, error
            ),
        }
    }

    fn ts_from_ms(ms: i64) -> chrono::DateTime<chrono::Local> {
        chrono::DateTime::from_timestamp_millis(ms)
            .unwrap()
            .with_timezone(&chrono::Local)
    }

    async fn prop_timestamps_propagated(
        req: CreateQuery,
        setup: model::testing::FragmentSetup,
    ) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let (_, fragments) = catalog
            .query
            .create_fragments(&query, fragment_reqs)
            .await
            .unwrap();

        walk_fragments_to(&catalog.query, query.id, &fragments, FragmentState::Running).await;

        const START_BASE_MS: i64 = 1_700_000_000_000;
        let updates: Vec<FragmentUpdate> = fragments
            .iter()
            .enumerate()
            .map(|(i, f)| FragmentUpdate {
                id: f.id,
                state: FragmentState::Running,
                start_timestamp: Some(ts_from_ms(START_BASE_MS + (i as i64) * 1000)),
                ..Default::default()
            })
            .collect();

        catalog
            .query
            .update_fragment_states(query.id, updates)
            .await
            .unwrap();

        let q = catalog
            .query
            .get_query(GetQuery::new().with_id(query.id))
            .await
            .unwrap();

        let expected_max_start =
            ts_from_ms(START_BASE_MS + (fragments.len() as i64 - 1) * 1000);
        assert_eq!(
            q[0].start_timestamp,
            Some(expected_max_start),
            "Query start_timestamp should be MAX of fragment start timestamps"
        );
        assert!(
            q[0].stop_timestamp.is_none(),
            "Query stop_timestamp should still be None (not terminal yet)"
        );

        const STOP_BASE_MS: i64 = 1_700_001_000_000;
        let stop_updates: Vec<FragmentUpdate> = fragments
            .iter()
            .enumerate()
            .map(|(i, f)| FragmentUpdate {
                id: f.id,
                state: FragmentState::Completed,
                stop_timestamp: Some(ts_from_ms(STOP_BASE_MS + (i as i64) * 1000)),
                ..Default::default()
            })
            .collect();

        catalog
            .query
            .update_fragment_states(query.id, stop_updates)
            .await
            .unwrap();

        let q = catalog
            .query
            .get_query(GetQuery::new().with_id(query.id))
            .await
            .unwrap();

        assert_eq!(q[0].current_state, QueryState::Completed);
        assert_eq!(
            q[0].start_timestamp,
            Some(expected_max_start),
            "start_timestamp should be preserved"
        );

        let expected_max_stop =
            ts_from_ms(STOP_BASE_MS + (fragments.len() as i64 - 1) * 1000);
        assert_eq!(
            q[0].stop_timestamp,
            Some(expected_max_stop),
            "Query stop_timestamp should be MAX of fragment stop timestamps"
        );
    }

    async fn prop_get_fragments_returns_created(
        req: CreateQuery,
        setup: model::testing::FragmentSetup,
    ) {
        let catalog = Catalog::for_test().await;
        for w in &setup.workers {
            catalog.worker.create_worker(w.clone()).await.unwrap();
        }
        let query = catalog.query.create_query(req).await.unwrap();
        let fragment_reqs = setup.create_fragments(query.id);
        let (_, created) = catalog
            .query
            .create_fragments(&query, fragment_reqs)
            .await
            .unwrap();

        let fetched = catalog.query.get_fragments(query.id).await.unwrap();

        assert_eq!(fetched.len(), created.len());

        let created_ids: std::collections::HashSet<_> = created.iter().map(|f| f.id).collect();
        for fragment in &fetched {
            assert!(
                created_ids.contains(&fragment.id),
                "Unexpected fragment {} in get_fragments result",
                fragment.id
            );
            assert_eq!(fragment.query_id, query.id);
        }
    }

    proptest! {
        #[test]
        fn create_and_get_query(req in arb_create_query()) {
            test_prop(|| async move {
                prop_create_and_get_query(req).await;
            });
        }

        #[test]
        fn drop_query(req in arb_create_query(), stop_mode in any::<StopMode>()) {
            test_prop(|| async move {
                prop_drop_query(req, stop_mode).await;
            });
        }

        #[test]
        fn get_fragments_missing_query_errors(req in arb_create_query()) {
            test_prop(|| async move {
                prop_get_fragments_missing_query_errors(req).await;
            });
        }

        #[test]
        fn update_fragment_states_empty_noop(req in arb_create_query()) {
            test_prop(|| async move {
                prop_update_fragment_states_empty_noop(req).await;
            });
        }

        #[test]
        fn fragment_negative_capacity_rejected(
            worker in arb_create_worker(),
            req in arb_create_query(),
        ) {
            test_prop(|| async move {
                prop_fragment_negative_capacity_rejected(worker, req).await;
            });
        }

        #[test]
        fn fragment_exceeding_capacity_rejected(
            worker in arb_create_worker(),
            req in arb_create_query(),
        ) {
            test_prop(|| async move {
                prop_fragment_exceeding_capacity_rejected(worker, req).await;
            });
        }

        #[test]
        fn fragment_exactly_exhausts_capacity(
            worker in arb_create_worker(),
            req in arb_create_query(),
        ) {
            test_prop(|| async move {
                prop_fragment_exactly_exhausts_capacity(worker, req).await;
            });
        }

        #[test]
        fn fragment_zero_capacity(
            worker in arb_create_worker(),
            req in arb_create_query(),
        ) {
            test_prop(|| async move {
                prop_fragment_zero_capacity(worker, req).await;
            });
        }

        #[test]
        fn fragment_creation_reserves_capacity(
            req in arb_create_query(),
            setup in arb_fragment_setup(5),
        ) {
            test_prop(|| async move {
                prop_fragment_creation_reserves_capacity(req, setup).await;
            });
        }

        #[test]
        fn capacity_released_on_fragment_terminal(
            req in arb_create_query(),
            setup in arb_fragment_setup(5),
            terminal_state in prop_oneof![
                Just(FragmentState::Completed),
                Just(FragmentState::Stopped),
                Just(FragmentState::Failed),
            ],
        ) {
            test_prop(|| async move {
                prop_capacity_released_on_fragment_terminal(req, setup, terminal_state).await;
            });
        }

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
        fn capacity_conserved_on_fragment_terminal(
            req in arb_create_query(),
            setup in arb_fragment_setup(5),
            terminal_state in prop_oneof![
                Just(FragmentState::Completed),
                Just(FragmentState::Stopped),
                Just(FragmentState::Failed),
            ],
        ) {
            test_prop(|| async move {
                prop_capacity_conserved_on_fragment_terminal(req, setup, terminal_state).await;
            });
        }

        #[test]
        fn fail_query_from_non_terminal(
            req in arb_create_query(),
            path in arb_valid_state_path(),
            error_msg in "[a-z ]{1,50}",
        ) {
            test_prop(|| async move {
                prop_fail_query_from_non_terminal(req, path, error_msg).await;
            });
        }

        #[test]
        fn fail_query_from_terminal_rejected(
            req in arb_create_query(),
            path in arb_valid_state_path().prop_filter(
                "need Completed or Stopped terminal",
                |p| matches!(p.last(), Some(QueryState::Completed) | Some(QueryState::Stopped)),
            ),
        ) {
            test_prop(|| async move {
                prop_fail_query_from_terminal_rejected(req, path).await;
            });
        }

        #[test]
        fn update_fragment_states_applied(
            req in arb_create_query(),
            setup in arb_fragment_setup(5),
            target_state in arb_fragment_state(),
        ) {
            test_prop(|| async move {
                prop_update_fragment_states_applied(req, setup, target_state).await;
            });
        }

        #[test]
        fn update_fragment_states_partial(
            req in arb_create_query(),
            setup in arb_fragment_setup(5),
            target_state in arb_forward_fragment_state(),
        ) {
            test_prop(|| async move {
                prop_update_fragment_states_partial(req, setup, target_state).await;
            });
        }

        #[test]
        fn get_mismatch_query_correctness(
            queries in prop::collection::vec(arb_create_query(), 1..=5usize),
        ) {
            test_prop(|| async move {
                prop_get_mismatch_query_correctness(queries).await;
            });
        }

        #[test]
        fn get_fragments_returns_created(
            req in arb_create_query(),
            setup in arb_fragment_setup(5),
        ) {
            test_prop(|| async move {
                prop_get_fragments_returns_created(req, setup).await;
            });
        }

        #[test]
        fn create_fragments_transitions_to_planned(
            req in arb_create_query(),
            setup in arb_fragment_setup(5),
        ) {
            test_prop(|| async move {
                prop_create_fragments_transitions_to_planned(req, setup).await;
            });
        }

        #[test]
        fn fragment_state_derives_query_state(
            req in arb_create_query(),
            setup in arb_fragment_setup(5),
        ) {
            test_prop(|| async move {
                prop_fragment_state_derives_query_state(req, setup).await;
            });
        }

        #[test]
        fn one_failed_fragment_fails_query(
            req in arb_create_query(),
            setup in arb_fragment_setup(5),
        ) {
            test_prop(|| async move {
                prop_one_failed_fragment_fails_query(req, setup).await;
            });
        }

        #[test]
        fn error_variant_matches_pre_state(
            req in arb_create_query(),
            setup in arb_fragment_setup(5),
            pre_state in prop_oneof![
                Just(QueryState::Planned),
                Just(QueryState::Registered),
                Just(QueryState::Running),
            ],
        ) {
            test_prop(|| async move {
                prop_error_variant_matches_pre_state(req, setup, pre_state).await;
            });
        }

        #[test]
        fn timestamps_propagated(
            req in arb_create_query(),
            setup in arb_fragment_setup(5),
        ) {
            test_prop(|| async move {
                prop_timestamps_propagated(req, setup).await;
            });
        }
    }
}
