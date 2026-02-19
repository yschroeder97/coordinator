#[cfg(madsim)]
use tokio::task::JoinHandle;

#[cfg(not(madsim))]
pub use tokio::task::JoinSet;

/// A polyfill for JoinSet that works with madsim (using FuturesUnordered)
#[cfg(madsim)]
pub struct JoinSet<T> {
    tasks: futures_util::stream::FuturesUnordered<JoinHandle<T>>,
}

#[cfg(madsim)]
impl<T> JoinSet<T> {
    pub fn new() -> Self {
        Self {
            tasks: futures_util::stream::FuturesUnordered::new(),
        }
    }

    pub fn spawn<F>(&mut self, task: F) -> AbortHandle
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let handle = tokio::spawn(task);
        let abort_handle = AbortHandle(handle.abort_handle());
        self.tasks.push(handle);
        abort_handle
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

#[cfg(madsim)]
#[derive(Debug)]
pub struct AbortHandle(tokio::task::AbortHandle);

#[cfg(madsim)]
impl AbortHandle {
    pub fn abort(&self) {
        self.0.abort()
    }
}

#[cfg(not(madsim))]
pub use tokio::task::AbortHandle;