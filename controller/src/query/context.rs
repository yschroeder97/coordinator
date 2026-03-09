use crate::worker::worker_client::{
    RegisterFragmentRequest, Rpc, StartFragmentRequest, StopFragmentRequest,
    UnregisterFragmentRequest, WorkerClientErr,
};
use crate::worker::worker_registry::{WorkerError, WorkerRegistryHandle};
use crate::query::retry::RetryPolicy;
use catalog::Catalog;
use futures_util::future;
use madsim::buggify::buggify;
use model::Set;
use model::query;
use model::query::StopMode;
use model::query::fragment::{self, FragmentError, FragmentId, FragmentState};
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::{error, info, warn};

pub(crate) struct QueryContext {
    pub(crate) query: query::Model,
    pub(crate) catalog: Arc<Catalog>,
    pub(crate) worker_registry: WorkerRegistryHandle,
}

impl QueryContext {
    pub(crate) async fn broadcast<F, Rsp>(
        &self,
        fragments: &[fragment::Model],
        mk_rpc: F,
        retry: &RetryPolicy,
    ) -> Vec<Result<Rsp, WorkerError>>
    where
        F: Fn(FragmentId) -> (oneshot::Receiver<Result<Rsp, WorkerClientErr>>, Rpc),
        Rsp: Send + 'static,
    {
        let mk_rpc = &mk_rpc;
        let futures = fragments
            .iter()
            .map(|fragment| retry.execute(&self.worker_registry, mk_rpc, fragment));
        let mut results = future::join_all(futures).await;
        if buggify() && !results.is_empty() {
            let idx = results.len() / 2;
            results[idx] = Err(WorkerError::ClientUnavailable(fragments[idx].grpc_addr.clone()));
        }
        results
    }

    pub(crate) async fn register_fragments(
        &self,
        fragments: &[fragment::Model],
    ) -> Vec<Result<(), WorkerError>> {
        self.broadcast(
            fragments,
            |id| {
                let (rx, req) = RegisterFragmentRequest::new(id);
                (rx, Rpc::RegisterFragment(req))
            },
            &RetryPolicy::Transition,
        )
        .await
        .into_iter()
        .map(|r| r.map(|_| ()))
        .collect()
    }

    pub(crate) async fn start_fragments(
        &self,
        fragments: &[fragment::Model],
    ) -> Vec<Result<(), WorkerError>> {
        self.broadcast(
            fragments,
            |id| {
                let (rx, req) = StartFragmentRequest::new(id);
                (rx, Rpc::StartFragment(req))
            },
            &RetryPolicy::Transition,
        )
        .await
    }

    fn rollback_retry(&self) -> RetryPolicy {
        RetryPolicy::Rollback {
            catalog: self.catalog.clone(),
        }
    }

    pub(crate) async fn stop_fragments(
        &self,
        stop_mode: StopMode,
        fragments: &[fragment::Model],
    ) -> Vec<Result<(), WorkerError>> {
        self.broadcast(
            fragments,
            |id| {
                let (rx, req) = StopFragmentRequest::new((id, stop_mode));
                (rx, Rpc::StopFragment(req))
            },
            &self.rollback_retry(),
        )
        .await
    }

    pub(crate) async fn unregister_fragments(
        &self,
        fragments: &[fragment::Model],
    ) -> Vec<Result<(), WorkerError>> {
        self.broadcast(
            fragments,
            |id| {
                let (rx, req) = UnregisterFragmentRequest::new(id);
                (rx, Rpc::UnregisterFragment(req))
            },
            &self.rollback_retry(),
        )
        .await
    }

    pub(crate) async fn apply_rpc_results(
        &mut self,
        fragments: &[fragment::Model],
        results: Vec<Result<(), WorkerError>>,
    ) -> anyhow::Result<Vec<fragment::Model>> {
        debug_assert!(!fragments.is_empty(), "At least than one fragment expected");
        debug_assert!(
            fragments
                .iter()
                .all(|f| f.current_state == fragments.last().unwrap().current_state),
            "All fragments should be in the same state"
        );
        let current_state = fragments.last().unwrap().current_state;
        let target_state = current_state
            .next()
            .expect("Cannot advance a terminal fragment state");

        if buggify() {
            panic!("buggify: apply_rpc_skip_persist");
        }

        let updates = fragments
            .iter()
            .zip(results)
            .map(|(fragment, rpc_result)| {
                let mut am: fragment::ActiveModel = fragment.clone().into();
                match rpc_result {
                    Ok(_) => am.current_state = Set(target_state),
                    Err(e) => {
                        let error = FragmentError::from(e);
                        warn!(fragment_id = fragment.id, %error, "Fragment transition failed");
                        am.current_state = Set(FragmentState::Failed);
                        am.error = Set(Some(error));
                    }
                }
                am
            })
            .collect();

        let (query, fragments) = self
            .catalog
            .query
            .update_fragment_states(self.query.id, updates)
            .await?;
        self.query = query;

        if self.query.current_state.is_terminal() {
            anyhow::bail!("Transition to {target_state} failed");
        }
        Ok(fragments)
    }

    pub(crate) async fn rollback_stop(&self, mode: StopMode, fragments: &[fragment::Model]) {
        let errors: Vec<_> = self
            .stop_fragments(mode, fragments)
            .await
            .into_iter()
            .filter_map(Result::err)
            .collect();
        if errors.is_empty() {
            info!("All fragments stopped");
        } else {
            for e in errors {
                error!("Failed to stop fragment: {e}");
            }
        }
    }

    pub(crate) async fn rollback_unregister(&self, fragments: &[fragment::Model]) {
        let errors: Vec<_> = self
            .unregister_fragments(fragments)
            .await
            .into_iter()
            .filter_map(Result::err)
            .collect();
        if errors.is_empty() {
            info!("All fragments unregistered");
        } else {
            for e in errors {
                error!("Failed to unregister fragment: {e}");
            }
        }
    }

    pub(crate) async fn persist_failed(&mut self, error: String) {
        if buggify() {
            panic!("buggify: persist_failed_panic");
        }
        match self
            .catalog
            .query
            .fail_query(self.query.clone(), error)
            .await
        {
            Ok(query) => self.query = query,
            Err(e) => error!("Failed to set query to Failed: {e}"),
        }
    }

    pub(crate) async fn persist_stopped(&mut self) {
        match self.catalog.query.stop_query(&self.query).await {
            Ok(query) => self.query = query,
            Err(e) => error!("Failed to stop query: {e}"),
        }
    }

}
