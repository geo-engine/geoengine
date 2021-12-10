use error::Result;
use futures::{Future, Stream, StreamExt};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

pub mod error;

pub struct Executor<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Sync + Send + 'static,
{
    _pk: PhantomData<Key>,
    _pv: PhantomData<T>,
}

impl<Key, T> Executor<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Sync + Send + 'static,
{
    /// Creates a new `Executor` instance, ready to serve computations. The buffer
    /// size determines how much elements are at most kept  in memory per computation.
    pub fn new() -> Executor<Key, T> {
        Executor {
            _pk: Default::default(),
            _pv: Default::default(),
        }
    }

    /// Submits a streaming computation to this executor. In contrast
    /// to `Executor.submit_stream`, this method returns a Stream of
    /// `Arc<T>` that allows to use the executor with non-cloneable
    /// results.
    ///
    /// #Errors
    /// This call fails, if the `Executor` was already closed.
    pub async fn submit_stream_ref<F>(
        &self,
        _key: &Key,
        stream: F,
    ) -> Result<impl Stream<Item = Arc<T>>>
    where
        F: Stream<Item = T> + Send + 'static,
    {
        Ok(stream.map(|x| Arc::new(x)))
    }

    /// Submits a single-result computation to this executor. In contrast
    /// to `Executor.submit`, this method returns an
    /// `Arc<T>` that allows to use the executor with non-cloneable
    /// results.
    ///
    /// #Errors
    /// This call fails, if the `Executor` was already closed.
    pub async fn submit_ref<F>(&self, _key: &Key, f: F) -> Result<Arc<T>>
    where
        F: Future<Output = T> + Send + 'static,
    {
        Ok(Arc::new(f.await))
    }

    pub async fn close(self) -> Result<()> {
        Ok(())
    }
}

impl<Key, T> Executor<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Clone + Sync + Send + 'static,
{
    /// Submits a streaming computation to this executor. This method
    /// returns a stream providing the results of the original (given) stream.
    ///
    /// #Errors
    /// This call fails, if the `Executor` was already closed.
    pub async fn submit_stream<F>(&self, _key: &Key, stream: F) -> Result<impl Stream<Item = T>>
    where
        F: Stream<Item = T> + Send + 'static,
    {
        Ok(stream)
    }

    /// Submits a single-result computation to this executor.
    ///
    /// #Errors
    /// This call fails, if the `Executor` was already closed.
    pub async fn submit<F>(&self, _key: &Key, f: F) -> Result<T>
    where
        F: Future<Output = T> + Send + 'static,
    {
        Ok(f.await)
    }
}

impl<Key, T> Default for Executor<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Sync + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
