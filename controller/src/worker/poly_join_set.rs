#[cfg(madsim)]
use tokio::task::JoinHandle;

#[cfg(not(madsim))]
pub use tokio::task::JoinSet;

#[cfg(madsim)]
pub struct JoinSet<T> {
    tasks: futures_util::stream::FuturesUnordered<JoinHandle<T>>,
    abort_handles: Vec<tokio::task::AbortHandle>,
}

#[cfg(madsim)]
impl<T> JoinSet<T> {
    pub fn new() -> Self {
        Self {
            tasks: futures_util::stream::FuturesUnordered::new(),
            abort_handles: Vec::new(),
        }
    }

    pub fn spawn<F>(&mut self, task: F) -> AbortHandle
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let handle = tokio::spawn(task);
        let abort_handle = handle.abort_handle();
        self.abort_handles.push(abort_handle.clone());
        self.tasks.push(handle);
        AbortHandle(abort_handle)
    }

    pub async fn join_next(&mut self) -> Option<Result<T, tokio::task::JoinError>> {
        use futures_util::StreamExt;
        self.tasks.next().await
    }

    pub async fn join_all(mut self) -> Vec<T> {
        use futures_util::StreamExt;
        let mut results = Vec::new();
        while let Some(result) = self.tasks.next().await {
            if let Ok(value) = result {
                results.push(value);
            }
        }
        results
    }
}

// Dropping a madsim JoinHandle detaches the task instead of aborting it.
// We must abort explicitly to match tokio::task::JoinSet's drop semantics.
#[cfg(madsim)]
impl<T> Drop for JoinSet<T> {
    fn drop(&mut self) {
        for handle in &self.abort_handles {
            handle.abort();
        }
    }
}

#[cfg(madsim)]
pub struct AbortHandle(tokio::task::AbortHandle);

#[cfg(madsim)]
impl AbortHandle {
    pub fn abort(&self) {
        self.0.abort()
    }

    pub fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}

#[cfg(not(madsim))]
pub use tokio::task::AbortHandle;