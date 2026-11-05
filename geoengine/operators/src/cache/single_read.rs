use crate::cache::error::CacheError;
use futures::FutureExt;
use futures::future::{BoxFuture, WeakShared};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use tokio_util::task::AbortOnDropHandle;

/// Runs a read once for all concurrent callers with the same key and gives each the result, e.g.
/// for callers that miss the same cache entry.
///
/// The read runs in its own task, so a cancelled caller does not fail the others, and is aborted
/// once all callers are gone, so a read nobody waits for does not hold up the visible ones.
pub struct SingleRead<K, V> {
    running: RunningReads<K, V>,
    joined: AtomicU64,
}

/// Only the callers hold a read strongly, so that it ends with the last of them. `None` once a
/// read's task panicked.
type RunningReads<K, V> = Arc<Mutex<HashMap<K, WeakShared<BoxFuture<'static, Option<V>>>>>>;

impl<K, V> Default for SingleRead<K, V> {
    fn default() -> Self {
        Self {
            running: Arc::default(),
            joined: AtomicU64::new(0),
        }
    }
}

impl<K, V> SingleRead<K, V>
where
    K: Clone + Eq + Hash + Send + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// The value of `read`, or of the read another caller of `key` is running.
    ///
    /// # Errors
    /// Returns an error if the read panicked.
    pub async fn read<F>(&self, key: K, read: F) -> Result<V, CacheError>
    where
        F: Future<Output = V> + Send + 'static,
    {
        let shared_read = {
            let mut running = self.running.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(running_read) = running.get(&key).and_then(WeakShared::upgrade) {
                self.joined.fetch_add(1, Ordering::Relaxed);
                running_read
            } else {
                // shortcut: the guard of an aborted read may remove a newer read of its key, which
                // then runs once more for later callers; harmless, as they read the same data
                let done = RunningReadGuard {
                    running: self.running.clone(),
                    key: key.clone(),
                };
                let task = AbortOnDropHandle::new(tokio::spawn(async move {
                    let _done = done;
                    read.await
                }));
                let running_read = task.map(Result::ok).boxed().shared();
                if let Some(weak) = running_read.downgrade() {
                    running.insert(key, weak);
                }
                running_read
            }
        };

        shared_read.await.ok_or(CacheError::SharedReadFailed)
    }

    /// Callers that waited for a read another caller had started.
    pub fn joined(&self) -> u64 {
        self.joined.load(Ordering::Relaxed)
    }
}

/// Ends a read when its task ends, also if it panics.
struct RunningReadGuard<K: Eq + Hash, V> {
    running: RunningReads<K, V>,
    key: K,
}

impl<K: Eq + Hash, V> Drop for RunningReadGuard<K, V> {
    fn drop(&mut self) {
        self.running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn it_runs_concurrent_reads_once_and_survives_a_cancelled_caller() {
        let single_read = Arc::new(SingleRead::<u8, Result<u8, String>>::default());
        let runs = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Notify::new());

        let read = |value: Result<u8, String>| {
            let (runs, release) = (runs.clone(), release.clone());
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                release.notified().await;
                value
            }
        };

        let cancelled = {
            let single_read = single_read.clone();
            let read = read(Ok(1));
            tokio::spawn(async move { single_read.read(0, read).await })
        };
        tokio::task::yield_now().await;
        while runs.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let waiters: Vec<_> = (0..4)
            .map(|_| {
                let single_read = single_read.clone();
                let read = read(Ok(2));
                tokio::spawn(async move { single_read.read(0, read).await })
            })
            .collect();
        while single_read.joined() < 4 {
            tokio::task::yield_now().await;
        }
        cancelled.abort();
        release.notify_one();

        for waiter in waiters {
            assert_eq!(
                waiter.await.unwrap().unwrap(),
                Ok(1),
                "the waiter gets the first read's value"
            );
        }
        assert_eq!(runs.load(Ordering::SeqCst), 1);

        // a read that fails reaches its callers and the next read runs again
        release.notify_one();
        assert_eq!(
            single_read
                .read(0, read(Err("failed".to_string())))
                .await
                .unwrap(),
            Err("failed".to_string())
        );
        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn it_fails_the_callers_of_a_read_that_panics_and_reads_again() {
        let single_read = SingleRead::<u8, u8>::default();

        let panicked = single_read
            .read(0, async {
                panic!("read panics");
                #[allow(unreachable_code)]
                1_u8
            })
            .await;
        assert!(panicked.is_err());

        assert_eq!(single_read.read(0, async { 1_u8 }).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn it_aborts_a_read_that_no_caller_waits_for() {
        let single_read = SingleRead::<u8, u8>::default();
        let (read_dropped, is_dropped) = tokio::sync::oneshot::channel::<()>();

        let abandoned = tokio::time::timeout(
            std::time::Duration::from_millis(10),
            single_read.read(0, async move {
                let _read_dropped = read_dropped;
                futures::future::pending::<u8>().await
            }),
        )
        .await;
        assert!(abandoned.is_err(), "the read never finishes");

        tokio::time::timeout(std::time::Duration::from_secs(5), is_dropped)
            .await
            .expect("the read ends with its last caller")
            .expect_err("the read never sends");

        assert_eq!(single_read.read(0, async { 1_u8 }).await.unwrap(), 1);
    }
}
