// madsim intercepts panics at the task level, so tokio's JoinError::is_panic()
// is never triggered. We must catch_unwind ourselves to detect panics.
// Outside madsim, panics propagate normally through JoinError.
#[cfg(madsim)]
pub async fn supervised<F: Future<Output = ()>>(fut: F) -> bool {
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;
    AssertUnwindSafe(fut).catch_unwind().await.is_err()
}

#[cfg(not(madsim))]
pub async fn supervised<F: Future<Output = ()>>(fut: F) -> bool {
    fut.await;
    false
}
