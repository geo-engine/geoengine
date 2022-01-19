use std::collections::HashMap;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{Future, Stream, StreamExt};
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle};

use error::{ExecutorError, Result};
use replay::SendError;

pub mod error;
pub mod replay;

/// Encapsulates a requested stream computation to send
/// it to the executor task.
struct KeyedComputation<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
{
    key: Key,
    response: tokio::sync::oneshot::Sender<replay::Receiver<Result<Arc<T>>>>,
    stream: BoxStream<'static, T>,
}

/// A helper to retrieve a computation's key even if
/// the executing task failed.
#[pin_project::pin_project]
struct KeyedJoinHandle<K, T>
where
    K: Clone,
{
    // The computation's key
    key: K,
    // A unique sequence number of the computation. See `ComputationEntry`
    // for a detailed description.
    id: usize,
    // The sender side of the replay channel.
    sender: replay::Sender<Result<Arc<T>>>,
    // The join handle of the underlying task
    #[pin]
    handle: JoinHandle<()>,
}

/// The result when waiting on a `KeyedJoinHandle`
struct KeyedJoinResult<K, T>
where
    K: Clone,
{
    // The computation's key
    key: K,
    // A unique sequence number of the computation. See `ComputationEntry`
    // for a detailed description.
    id: usize,
    // The sender side of the replay channel.
    sender: replay::Sender<Result<Arc<T>>>,
    // The result returned by the task.
    result: std::result::Result<(), JoinError>,
}

impl<K, T> Future for KeyedJoinHandle<K, T>
where
    K: Clone,
{
    type Output = KeyedJoinResult<K, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.handle.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => Poll::Ready(KeyedJoinResult {
                key: this.key.clone(),
                id: *this.id,
                sender: this.sender.clone(),
                result,
            }),
        }
    }
}

/// An entry for the map of currently running computations.
/// It contains a sender from the associated replay channel
/// in order to register new consumers.
/// See `replay::Sender::subscribe`.
struct ComputationEntry<T> {
    // A unique sequence number of the computation.
    // This is used to decide whether or not to drop computation
    // infos after completion. If a computation task for a given key
    // is finished, it might be that another one took its place
    // in the list of running computations. This happens, of a computation
    // progressed too far already and a new consumer cannot join it anymore.
    // In those cases a new task is started and takes the slot in the list
    // of active computation. Now, if the old computation terminates, the newly
    // started computation must *NOT* be removed from this list.
    // We use this ID to handle such cases.
    id: usize,
    sender: replay::Sender<Result<Arc<T>>>,
}

type TerminationMessage<T> =
    std::result::Result<(), SendError<std::result::Result<Arc<T>, ExecutorError>>>;

/// This encapsulates the main loop of the executor task.
/// It manages all new, running and finished computations.
struct ExecutorLooper<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Sync + Send + 'static,
{
    buffer_size: usize,
    receiver: mpsc::Receiver<KeyedComputation<Key, T>>,
    computations: HashMap<Key, ComputationEntry<T>>,
    tasks: FuturesUnordered<KeyedJoinHandle<Key, T>>,
    termination_msgs: FuturesUnordered<BoxFuture<'static, TerminationMessage<T>>>,
}

impl<Key, T> ExecutorLooper<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Sync + Send + 'static,
{
    /// Creates a new Looper.
    fn new(
        buffer_size: usize,
        receiver: mpsc::Receiver<KeyedComputation<Key, T>>,
    ) -> ExecutorLooper<Key, T> {
        ExecutorLooper {
            buffer_size,
            receiver,
            computations: HashMap::new(),
            tasks: FuturesUnordered::new(),
            termination_msgs: FuturesUnordered::new(),
        }
    }

    /// Handles a new computation request. It first tries to join
    /// a running computation. If this is not possible, a new
    /// computation is started.
    fn handle_new_task(&mut self, kc: KeyedComputation<Key, T>) {
        log::debug!("Received new stream request.");
        let key = kc.key;

        let entry = self.computations.entry(key.clone());
        let receiver = match entry {
            // There is a computation running
            std::collections::hash_map::Entry::Occupied(mut oe) => {
                match oe.get().sender.subscribe() {
                    Ok(rx) => {
                        log::debug!("Attaching request to existing stream.");
                        rx
                    }
                    // Stream progressed too far, start a new one
                    Err(_) => {
                        log::debug!(
                            "Stream progressed too far. Starting new computation for request."
                        );
                        let new_id = oe.get().id + 1;
                        let (entry, rx) = Self::start_computation(
                            self.buffer_size,
                            new_id,
                            key,
                            kc.stream,
                            &mut self.tasks,
                        );
                        oe.insert(entry);
                        rx
                    }
                }
            }
            // Start a new computation
            std::collections::hash_map::Entry::Vacant(ve) => {
                log::debug!("Starting new computation for request.");
                let (entry, rx) =
                    Self::start_computation(self.buffer_size, 0, key, kc.stream, &mut self.tasks);
                ve.insert(entry);
                rx
            }
        };

        // This can only fail, if the receiver side is dropped
        if kc.response.send(receiver).is_err() {
            log::warn!("Result consumer dropped unexpectedly.");
        }
    }

    /// Starts a new computation. It spawns a separate task
    /// in which the computation is executed. Therefore it establishes
    /// a new `replay::channel` and returns infos about the computation
    /// and the receiving side of the replay channel.
    fn start_computation(
        buffer_size: usize,
        id: usize,
        key: Key,
        mut stream: BoxStream<'static, T>,
        tasks: &mut FuturesUnordered<KeyedJoinHandle<Key, T>>,
    ) -> (ComputationEntry<T>, replay::Receiver<Result<Arc<T>>>) {
        let (tx, rx) = replay::channel(buffer_size);

        let entry = ComputationEntry {
            id,
            sender: tx.clone(),
        };

        let jh = {
            let tx = tx.clone();
            tokio::spawn(async move {
                while let Some(v) = stream.next().await {
                    if let Err(replay::SendError::Closed(_)) = tx.send(Ok(Arc::new(v))).await {
                        log::debug!("All consumers left. Cancelling task.");
                        break;
                    }
                }
            })
        };

        tasks.push(KeyedJoinHandle {
            key,
            id,
            sender: tx,
            handle: jh,
        });
        (entry, rx)
    }

    /// Handles computations that ran to completion. It removes
    /// them from the map of running computations and notifies
    /// consumers. Furthermore, if the computation task was cancelled
    /// or panicked, this is also propagated.
    ///
    fn handle_completed_task(&mut self, completed_task: KeyedJoinResult<Key, T>) {
        let id = completed_task.id;

        // Remove the map entry only, if the completed task's id matches the stored task's id
        // There may be older tasks around (with smaller ids) that should not trigger a removal from the map.
        if let std::collections::hash_map::Entry::Occupied(e) =
            self.computations.entry(completed_task.key)
        {
            if e.get().id == id {
                e.remove();
            }
        }

        match completed_task.result {
            Err(e) => {
                self.termination_msgs.push(Box::pin(async move {
                    if e.try_into_panic().is_ok() {
                        log::warn!("Stream task panicked. Notifying consumer streams.");
                        completed_task.sender.send(Err(ExecutorError::Panic)).await
                    } else {
                        log::warn!("Stream task was cancelled. Notifying consumer streams.");
                        completed_task
                            .sender
                            .send(Err(ExecutorError::Cancelled))
                            .await
                    }
                }));
            }
            Ok(_) => {
                log::debug!("Computation finished. Notifying consumer streams.");
                // After destroying the sender all remaining receivers will receive end-of-stream
            }
        }
    }

    /// This is the main loop. Here we check for new and completed tasks,
    /// and also drive termination messages forward.
    pub async fn main_loop(&mut self) {
        log::info!("Starting executor loop.");
        loop {
            tokio::select! {
                new_task = self.receiver.recv() => {
                    if let Some(kc) = new_task {
                        self.handle_new_task(kc);
                    }
                    else {
                        log::info!("Executor terminated.");
                        break;
                    }
                },
                Some(completed_task) = self.tasks.next() => {
                    self.handle_completed_task(completed_task);
                },
                Some(_) = self.termination_msgs.next() => {
                    log::debug!("Successfully delivered termination message.");
                }
            }
        }
        log::info!("Finished executor loop.");
    }
}

/// The `Executor` runs async (streaming) computations. It allows multiple consumers
/// per stream so that results are computed only once.
/// A pre-defined buffer size determines how many elements of a stream are kept. This
/// size can be seen as a window that slides forward, if the slowest consumer consumes
/// the oldest element.
/// New consumers join the same computation if the window did not slide forward at
/// the time of task submission. Otherwise, a new computation task is started.
pub struct Executor<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Sync + Send + 'static,
{
    sender: mpsc::Sender<KeyedComputation<Key, T>>,
    driver: JoinHandle<()>,
}

impl<Key, T> Executor<Key, T>
where
    Key: Hash + Clone + Eq + Send + 'static,
    T: Sync + Send + 'static,
{
    /// Creates a new `Executor` instance, ready to serve computations. The buffer
    /// size determines how much elements are at most kept  in memory per computation.
    pub fn new(buffer_size: usize) -> Executor<Key, T> {
        let (sender, receiver) = tokio::sync::mpsc::channel::<KeyedComputation<Key, T>>(128);

        let mut looper = ExecutorLooper::new(buffer_size, receiver);

        // This is the task that is responsible for driving the async computations and
        // notifying consumers about success and failure.
        let driver = tokio::spawn(async move { looper.main_loop().await });

        Executor { sender, driver }
    }

    /// Submits a streaming computation to this executor. In contrast
    /// to `Executor.submit_stream`, this method returns a Stream of
    /// `Arc<T>` that allows to use the executor with non-cloneable
    /// results.
    ///
    /// #Errors
    /// This call fails, if the `Executor` was already closed.
    pub async fn submit_stream_ref<F>(&self, key: &Key, stream: F) -> Result<StreamReceiver<T>>
    where
        F: Stream<Item = T> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel::<replay::Receiver<Result<Arc<T>>>>();

        let kc = KeyedComputation {
            key: key.clone(),
            stream: Box::pin(stream),
            response: tx,
        };

        self.sender.send(kc).await?;
        let res = rx.await?;

        Ok(StreamReceiver::new(res.into()))
    }

    /// Submits a single-result computation to this executor. In contrast
    /// to `Executor.submit`, this method returns an
    /// `Arc<T>` that allows to use the executor with non-cloneable
    /// results.
    ///
    /// #Errors
    /// This call fails, if the `Executor` was already closed.
    #[allow(clippy::missing_panics_doc)]
    pub async fn submit_ref<F>(&self, key: &Key, f: F) -> Result<Arc<T>>
    where
        F: Future<Output = T> + Send + 'static,
    {
        let mut stream = self
            .submit_stream_ref(key, futures::stream::once(f))
            .await?;

        Ok(stream.next().await.unwrap())
    }

    pub async fn close(self) -> Result<()> {
        drop(self.sender);
        Ok(self.driver.await?)
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
    pub async fn submit_stream<F>(&self, key: &Key, stream: F) -> Result<CloningStreamReceiver<T>>
    where
        F: Stream<Item = T> + Send + 'static,
    {
        let consumer = self.submit_stream_ref(key, stream).await?;
        Ok(CloningStreamReceiver { consumer })
    }

    /// Submits a single-result computation to this executor.
    ///
    /// #Errors
    /// This call fails, if the `Executor` was already closed.
    #[allow(clippy::missing_panics_doc)]
    pub async fn submit<F>(&self, key: &Key, f: F) -> Result<T>
    where
        F: Future<Output = T> + Send + 'static,
    {
        let mut stream = self.submit_stream(key, futures::stream::once(f)).await?;
        Ok(stream.next().await.unwrap())
    }
}

#[pin_project::pin_project]
pub struct StreamReceiver<T>
where
    T: Sync + Send + 'static,
{
    #[pin]
    input: replay::ReceiverStream<Result<Arc<T>>>,
}

impl<T> StreamReceiver<T>
where
    T: Sync + Send + 'static,
{
    fn new(input: replay::ReceiverStream<Result<Arc<T>>>) -> StreamReceiver<T> {
        StreamReceiver { input }
    }
}

impl<T> Stream for StreamReceiver<T>
where
    T: Sync + Send + 'static,
{
    type Item = Arc<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.input.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(v))) => Poll::Ready(Some(v)),
            Poll::Ready(Some(Err(ExecutorError::Panic))) => panic!("Executor task panicked!"),
            Poll::Ready(Some(Err(ExecutorError::Cancelled))) => {
                panic!("Executor task cancelled!")
            }
            // Submission already succeeded -> Unreachable.
            Poll::Ready(Some(Err(ExecutorError::Submission { .. }))) => unreachable!(),
        }
    }
}

//
#[pin_project::pin_project]
pub struct CloningStreamReceiver<T>
where
    T: Clone + Sync + Send + 'static,
{
    #[pin]
    consumer: StreamReceiver<T>,
}

impl<T> Stream for CloningStreamReceiver<T>
where
    T: Clone + Sync + Send + 'static,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.consumer.poll_next(cx) {
            Poll::Ready(Some(v)) => Poll::Ready(Some(v.as_ref().clone())),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use futures::{Stream, StreamExt};

    use crate::pro::executor::error::ExecutorError;

    use super::Executor;

    #[tokio::test]
    async fn test_stream_empty_stream() -> Result<(), ExecutorError> {
        let e = Executor::<i32, i32>::new(5);

        let sf1 = e
            .submit_stream_ref(&1, futures::stream::iter(Vec::<i32>::new()))
            .await
            .unwrap();

        let results: Vec<Arc<i32>> = sf1.collect().await;

        assert!(results.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_stream_single_consumer() -> Result<(), ExecutorError> {
        let e = Executor::<i32, i32>::new(5);

        let sf1 = e
            .submit_stream_ref(&1, futures::stream::iter(vec![1, 2, 3]))
            .await
            .unwrap();

        let results: Vec<Arc<i32>> = sf1.collect().await;

        assert_eq!(vec![Arc::new(1), Arc::new(2), Arc::new(3)], results);
        Ok(())
    }

    #[tokio::test]
    async fn test_stream_two_consumers() -> Result<(), ExecutorError> {
        let e = Executor::new(5);

        let sf1 = e.submit_stream_ref(&1, futures::stream::iter(vec![1, 2, 3]));
        let sf2 = e.submit_stream_ref(&1, futures::stream::iter(vec![1, 2, 3]));

        let (sf1, sf2) = tokio::join!(sf1, sf2);

        let (mut sf1, mut sf2) = (sf1?, sf2?);

        let mut res1 = vec![];
        let mut res2 = vec![];

        loop {
            tokio::select! {
                Some(v) = sf1.next() => {
                    res1.push(v);
                },
                Some(v) = sf2.next() => {
                    res2.push(v);
                },
                else => {
                    break;
                }
            }
        }

        assert_eq!(vec![Arc::new(1), Arc::new(2), Arc::new(3)], res1);
        assert_eq!(vec![Arc::new(1), Arc::new(2), Arc::new(3)], res2);

        Ok(())
    }

    struct PanicStream {}

    impl Stream for PanicStream {
        type Item = i32;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            panic!("Expected panic!");
        }
    }

    #[tokio::test]
    #[should_panic(expected = "Executor task panicked!")]
    async fn test_stream_propagate_panic() {
        let e = Executor::new(5);
        let sf = e.submit_stream_ref(&1, PanicStream {});
        let mut sf = sf.await.unwrap();
        sf.next().await;
    }

    #[tokio::test]
    async fn test_stream_consumer_drop() {
        let e = Executor::new(2);
        let chk;
        {
            let mut sf = e
                .submit_stream_ref(&1, futures::stream::iter(vec![1, 2, 3]))
                .await
                .unwrap();
            // Consume a single element
            chk = sf.next().await;
            assert_eq!(Some(Arc::new(1)), chk);
        }
        // Assert that the task is dropped if all consumers are dropped.
        // Therefore, resubmit another one and ensure we produce new results.
        let mut sf = e
            .submit_stream_ref(&1, futures::stream::iter(vec![1, 2, 3]))
            .await
            .unwrap();
        let chk2 = sf.next().await;
        assert_eq!(Some(Arc::new(1)), chk);

        assert_eq!(chk, chk2);

        assert!(!Arc::ptr_eq(&chk.unwrap(), &chk2.unwrap()));
    }

    #[tokio::test]
    async fn test_stream_clone() -> Result<(), ExecutorError> {
        let e = Executor::<i32, i32>::new(5);

        let sf1 = e
            .submit_stream(&1, futures::stream::iter(vec![1, 2, 3]))
            .await
            .unwrap();

        let results: Vec<i32> = sf1.collect().await;

        assert_eq!(vec![1, 2, 3], results);
        Ok(())
    }

    #[tokio::test]
    async fn test_simple() -> Result<(), ExecutorError> {
        let e = Executor::new(5);
        let f = e.submit_ref(&1, async { 2_u64 });

        assert_eq!(Arc::new(2_u64), f.await?);

        let f = e.submit_ref(&1, async { 42_u64 });
        assert_eq!(Arc::new(42_u64), f.await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_multi_consumers() -> Result<(), ExecutorError> {
        let e = Executor::new(5);
        // We use arc here to ensure both actually return the same result
        let f = e.submit_ref(&1, async { 2_u64 });
        let f2 = e.submit_ref(&1, async { 2_u64 });

        let (r1, r2) = tokio::join!(f, f2);
        let (r1, r2) = (r1?, r2?);

        assert!(Arc::ptr_eq(&r1, &r2));

        let f = e.submit_ref(&1, async { 2_u64 });
        let f2 = e.submit_ref(&1, async { 2_u64 });

        let r1 = f.await?;
        let r2 = f2.await?;
        assert!(!Arc::ptr_eq(&r1, &r2));

        Ok(())
    }

    #[tokio::test]
    #[should_panic]
    async fn test_panic() {
        let e = Executor::new(5);
        let f = e.submit_ref(&1, async { panic!("booom") });

        f.await.unwrap();
    }

    #[tokio::test]
    async fn test_close() -> Result<(), ExecutorError> {
        let e = Executor::new(5);
        let f = e.submit_ref(&1, async { 2_u64 });
        assert_eq!(Arc::new(2_u64), f.await?);
        let c = e.close();
        c.await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_clone() -> Result<(), ExecutorError> {
        let e = Executor::new(5);
        let f = e.submit(&1, async { 2_u64 });
        assert_eq!(2_u64, f.await?);
        Ok(())
    }
}
