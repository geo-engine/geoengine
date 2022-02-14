use std::collections::HashMap;
use std::fmt::Debug;
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
pub mod operators;
pub mod replay;

/// Description of an [Executor] task. The description consists provides a primary
/// key to identify identical computations (`KeyType`).
pub trait ExecutorTaskDescription: Clone + Send + Debug + 'static {
    /// A unique identifier for the actual computation of a task
    type KeyType: Clone + Send + PartialEq + Eq + Hash;
    /// The result type of the computation
    type ResultType: Sync + Send;
    /// Returns the unique identifies
    fn primary_key(&self) -> &Self::KeyType;
    /// Determines if this computation can be satisfied by using the result
    /// of `other`. E.g., if the bounding box of `other` contains this tasks bounding box.
    ///
    /// Note: It is not required to check the `primary_key`s in this method.
    fn can_join(&self, other: &Self) -> bool;
    /// Extracts the result for this task from the result of a possibly 'greater'
    /// result.
    ///
    /// If the computation returns a stream of `ResultType` irrelevant elements may by filtered
    /// out by returning `None`. Moreover, it is also possible to extract a subset of elements
    /// (e.g., features or pixels) from the given result. In those cases this method simply
    /// returns a new instance of `ResultType`.
    ///
    fn slice_result(&self, result: &Self::ResultType) -> Option<Self::ResultType>;
}

type TerminationMessage<T> =
    std::result::Result<(), SendError<std::result::Result<Arc<T>, ExecutorError>>>;

type ReplayReceiver<T> = replay::Receiver<Result<Arc<T>>>;

/// Encapsulates a requested stream computation to send
/// it to the executor task.
struct KeyedComputation<Desc>
where
    Desc: ExecutorTaskDescription,
{
    key: Desc,
    response: tokio::sync::oneshot::Sender<ReplayReceiver<Desc::ResultType>>,
    stream: BoxStream<'static, Desc::ResultType>,
}

/// A helper to retrieve a computation's key even if
/// the executing task failed.
#[pin_project::pin_project]
struct KeyedJoinHandle<Desc>
where
    Desc: ExecutorTaskDescription,
{
    // The computation's key
    key: Desc,
    // A unique sequence number of the computation. See `ComputationEntry`
    // for a detailed description.
    id: usize,
    // The sender side of the replay channel.
    sender: replay::Sender<Result<Arc<Desc::ResultType>>>,
    // The join handle of the underlying task
    #[pin]
    handle: JoinHandle<()>,
}

/// The result when waiting on a `KeyedJoinHandle`
struct KeyedJoinResult<Desc>
where
    Desc: ExecutorTaskDescription,
{
    // The computation's key
    key: Desc,
    // A unique sequence number of the computation. See `ComputationEntry`
    // for a detailed description.
    id: usize,
    // The sender side of the replay channel.
    sender: replay::Sender<Result<Arc<Desc::ResultType>>>,
    // The result returned by the task.
    result: std::result::Result<(), JoinError>,
}

impl<Desc> Future for KeyedJoinHandle<Desc>
where
    Desc: ExecutorTaskDescription,
{
    type Output = KeyedJoinResult<Desc>;

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
///
/// See [`replay::Sender::subscribe`].
struct ComputationEntry<Desc>
where
    Desc: ExecutorTaskDescription,
{
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
    key: Desc,
    sender: replay::Sender<Result<Arc<Desc::ResultType>>>,
}

/// This encapsulates the main loop of the executor task.
/// It manages all new, running and finished computations.
struct ExecutorLooper<Desc>
where
    Desc: ExecutorTaskDescription,
{
    buffer_size: usize,
    receiver: mpsc::Receiver<KeyedComputation<Desc>>,
    computations: HashMap<Desc::KeyType, Vec<ComputationEntry<Desc>>>,
    tasks: FuturesUnordered<KeyedJoinHandle<Desc>>,
    termination_msgs: FuturesUnordered<BoxFuture<'static, TerminationMessage<Desc::ResultType>>>,
    id_seq: usize,
}

impl<Desc> ExecutorLooper<Desc>
where
    Desc: ExecutorTaskDescription,
{
    /// Creates a new Looper.
    fn new(
        buffer_size: usize,
        receiver: mpsc::Receiver<KeyedComputation<Desc>>,
    ) -> ExecutorLooper<Desc> {
        ExecutorLooper {
            buffer_size,
            receiver,
            computations: HashMap::new(),
            tasks: FuturesUnordered::new(),
            termination_msgs: FuturesUnordered::new(),
            id_seq: 0,
        }
    }

    fn next_id(current: &mut usize) -> usize {
        *current += 1;
        *current
    }

    /// Handles a new computation request. It first tries to join
    /// a running computation. If this is not possible, a new
    /// computation is started.
    fn handle_new_task(&mut self, kc: KeyedComputation<Desc>) {
        let key = kc.key;

        let entry = self.computations.entry(key.primary_key().clone());
        let receiver = match entry {
            // There is a computation running
            std::collections::hash_map::Entry::Occupied(mut oe) => {
                let entries = oe.get_mut();
                let append = entries
                    .iter_mut()
                    .filter(|ce| key.can_join(&ce.key))
                    .find_map(|ce| match ce.sender.subscribe() {
                        Ok(rx) => Some((&ce.key, rx)),
                        Err(_) => None,
                    });

                match append {
                    Some((desc, rx)) => {
                        log::debug!(
                            "Joining running computation for request. New: {:?}, Running: {:?}",
                            &key,
                            desc
                        );
                        rx
                    }
                    None => {
                        log::debug!("Stream progressed too far or results do not cover requested result. Starting new computation for request: {:?}", &key);
                        let (entry, rx) = Self::start_computation(
                            self.buffer_size,
                            Self::next_id(&mut self.id_seq),
                            key,
                            kc.stream,
                            &mut self.tasks,
                        );
                        entries.push(entry);
                        rx
                    }
                }
            }
            // Start a new computation
            std::collections::hash_map::Entry::Vacant(ve) => {
                log::debug!("Starting new computation for request: {:?}", &key);
                let (entry, rx) = Self::start_computation(
                    self.buffer_size,
                    Self::next_id(&mut self.id_seq),
                    key,
                    kc.stream,
                    &mut self.tasks,
                );
                ve.insert(vec![entry]);
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
    /// a new [`replay::channel`] and returns infos about the computation
    /// and the receiving side of the replay channel.
    fn start_computation(
        buffer_size: usize,
        id: usize,
        key: Desc,
        mut stream: BoxStream<'static, Desc::ResultType>,
        tasks: &mut FuturesUnordered<KeyedJoinHandle<Desc>>,
    ) -> (ComputationEntry<Desc>, ReplayReceiver<Desc::ResultType>) {
        let (tx, rx) = replay::channel(buffer_size);

        let entry = ComputationEntry {
            id,
            key: key.clone(),
            sender: tx.clone(),
        };

        let jh = {
            let tx = tx.clone();
            let key = key.clone();
            tokio::spawn(async move {
                while let Some(v) = stream.next().await {
                    if let Err(replay::SendError::Closed(_)) = tx.send(Ok(Arc::new(v))).await {
                        log::debug!("All consumers left. Cancelling task: {:?}", &key);
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
    fn handle_completed_task(&mut self, completed_task: KeyedJoinResult<Desc>) {
        let id = completed_task.id;

        // Remove the map entry only, if the completed task's id matches the stored task's id
        // There may be older tasks around (with smaller ids) that should not trigger a removal from the map.
        if let std::collections::hash_map::Entry::Occupied(mut oe) = self
            .computations
            .entry(completed_task.key.primary_key().clone())
        {
            if let Some(idx) = oe.get().iter().position(|x| x.id == id) {
                oe.get_mut().swap_remove(idx);
            }
            if oe.get().is_empty() {
                oe.remove();
            }
        }

        match completed_task.result {
            Err(e) => {
                self.termination_msgs.push(Box::pin(async move {
                    if e.try_into_panic().is_ok() {
                        log::warn!(
                            "Stream task panicked. Notifying consumer streams. Request: {:?}",
                            &completed_task.key
                        );
                        completed_task.sender.send(Err(ExecutorError::Panic)).await
                    } else {
                        log::warn!(
                            "Stream task was cancelled. Notifying consumer streams. Request: {:?}",
                            &completed_task.key
                        );
                        completed_task
                            .sender
                            .send(Err(ExecutorError::Cancelled))
                            .await
                    }
                }));
            }
            Ok(_) => {
                log::debug!(
                    "Computation finished. Notifying consumer streams. Request: {:?}",
                    &completed_task.key
                );
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
pub struct Executor<Desc>
where
    Desc: ExecutorTaskDescription,
{
    sender: mpsc::Sender<KeyedComputation<Desc>>,
    driver: JoinHandle<()>,
}

impl<Desc> Executor<Desc>
where
    Desc: ExecutorTaskDescription,
{
    /// Creates a new `Executor` instance, ready to serve computations. The buffer
    /// size determines how much elements are at most kept  in memory per computation.
    pub fn new(buffer_size: usize) -> Executor<Desc> {
        let (sender, receiver) = tokio::sync::mpsc::channel::<KeyedComputation<Desc>>(128);

        let mut looper = ExecutorLooper::new(buffer_size, receiver);

        // This is the task that is responsible for driving the async computations and
        // notifying consumers about success and failure.
        let driver = tokio::spawn(async move { looper.main_loop().await });

        Executor { sender, driver }
    }

    /// Submits a streaming computation to this executor.
    ///
    /// #Errors
    /// This call fails, if the `Executor` was already closed.
    pub async fn submit_stream<F>(&self, key: &Desc, stream: F) -> Result<StreamReceiver<Desc>>
    where
        F: Stream<Item = Desc::ResultType> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel::<ReplayReceiver<Desc::ResultType>>();

        let kc = KeyedComputation {
            key: key.clone(),
            stream: Box::pin(stream),
            response: tx,
        };

        self.sender.send(kc).await?;
        let res = rx.await?;

        Ok(StreamReceiver::new(key.clone(), res.into()))
    }

    /// Submits a single-result computation to this executor.
    ///
    /// #Errors
    /// This call fails, if the `Executor` was already closed.
    #[allow(clippy::missing_panics_doc)]
    pub async fn submit<F>(&self, key: &Desc, f: F) -> Result<Desc::ResultType>
    where
        F: Future<Output = Desc::ResultType> + Send + 'static,
    {
        let mut stream = self.submit_stream(key, futures::stream::once(f)).await?;

        Ok(stream.next().await.unwrap())
    }

    pub async fn close(self) -> Result<()> {
        drop(self.sender);
        Ok(self.driver.await?)
    }
}

#[pin_project::pin_project]
pub struct StreamReceiver<Desc>
where
    Desc: ExecutorTaskDescription,
{
    key: Desc,
    #[pin]
    input: replay::ReceiverStream<Result<Arc<Desc::ResultType>>>,
}

impl<Desc> StreamReceiver<Desc>
where
    Desc: ExecutorTaskDescription,
{
    fn new(
        key: Desc,
        input: replay::ReceiverStream<Result<Arc<Desc::ResultType>>>,
    ) -> StreamReceiver<Desc> {
        StreamReceiver { key, input }
    }
}

impl<Desc> Stream for StreamReceiver<Desc>
where
    Desc: ExecutorTaskDescription,
{
    type Item = Desc::ResultType;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.input.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Ok(res))) => {
                    if let Some(sliced) = this.key.slice_result(res.as_ref()) {
                        // Slicing produced a result -> Otherwise loop again
                        return Poll::Ready(Some(sliced));
                    }
                }
                Poll::Ready(Some(Err(ExecutorError::Panic))) => panic!("Executor task panicked!"),
                Poll::Ready(Some(Err(ExecutorError::Cancelled))) => {
                    panic!("Executor task cancelled!")
                }
                // Submission already succeeded -> Unreachable.
                Poll::Ready(Some(Err(ExecutorError::Submission { .. }))) => unreachable!(),
            }
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
    use crate::pro::executor::ExecutorTaskDescription;

    use super::Executor;

    impl ExecutorTaskDescription for i32 {
        type KeyType = i32;
        type ResultType = i32;

        fn primary_key(&self) -> &Self::KeyType {
            self
        }

        fn can_join(&self, other: &Self) -> bool {
            self == other
        }

        fn slice_result(&self, result: &Self::ResultType) -> Option<Self::ResultType> {
            Some(*result)
        }
    }

    #[tokio::test]
    async fn test_stream_empty_stream() -> Result<(), ExecutorError> {
        let e = Executor::<i32>::new(5);

        let sf1 = e
            .submit_stream(&1, futures::stream::iter(Vec::<i32>::new()))
            .await
            .unwrap();

        let results = sf1.collect::<Vec<_>>().await;

        assert!(results.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_stream_single_consumer() -> Result<(), ExecutorError> {
        let e = Executor::<i32>::new(5);

        let sf1 = e
            .submit_stream(&1, futures::stream::iter(vec![1, 2, 3]))
            .await
            .unwrap();

        let results = sf1.collect::<Vec<_>>().await;

        assert_eq!(vec![1, 2, 3], results);
        Ok(())
    }

    #[tokio::test]
    async fn test_stream_two_consumers() -> Result<(), ExecutorError> {
        let e = Executor::new(5);

        let sf1 = e.submit_stream(&1, futures::stream::iter(vec![1, 2, 3]));
        let sf2 = e.submit_stream(&1, futures::stream::iter(vec![1, 2, 3]));

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

        assert_eq!(vec![1, 2, 3], res1);
        assert_eq!(vec![1, 2, 3], res2);

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
        let sf = e.submit_stream(&1, PanicStream {});
        let mut sf = sf.await.unwrap();
        sf.next().await;
    }

    #[tokio::test]
    async fn test_stream_consumer_drop() {
        let e = Executor::new(2);
        let chk = {
            e.submit_stream(&1, futures::stream::iter(vec![1, 2, 3]))
                .await
                .unwrap()
                .next()
                .await
        };
        // Assert that the task is dropped if all consumers are dropped.
        // Therefore, resubmit another one and ensure we produce new results.
        let chk2 = {
            e.submit_stream(&1, futures::stream::iter(vec![2, 2, 3]))
                .await
                .unwrap()
                .next()
                .await
        };

        assert_eq!(Some(1), chk);
        assert_eq!(Some(2), chk2);
    }

    #[tokio::test]
    async fn test_simple() -> Result<(), ExecutorError> {
        let e = Executor::new(5);
        let f = e.submit(&1, async { 2 });

        assert_eq!(2, f.await?);

        let f = e.submit(&1, async { 42 });
        assert_eq!(42, f.await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_multi_consumers() -> Result<(), ExecutorError> {
        let e = Executor::new(5);
        let f = e.submit(&1, async { 2 });
        let f2 = e.submit(&1, async { 2 });

        let (r1, r2) = tokio::join!(f, f2);
        let (r1, r2) = (r1?, r2?);

        assert_eq!(r1, r2);

        let f = e.submit(&1, async { 2 });
        let f2 = e.submit(&1, async { 2 });

        let r1 = f.await?;
        let r2 = f2.await?;
        assert_eq!(r1, r2);

        Ok(())
    }

    #[tokio::test]
    #[should_panic]
    async fn test_panic() {
        let e = Executor::new(5);
        let f = e.submit(&1, async { panic!("booom") });

        f.await.unwrap();
    }

    #[tokio::test]
    async fn test_close() -> Result<(), ExecutorError> {
        let e = Executor::new(5);
        let f = e.submit(&1, async { 2 });
        assert_eq!(2, f.await?);
        let c = e.close();
        c.await?;

        Ok(())
    }

    #[derive(Clone, Debug)]
    struct IntDesc {
        key: i32,
        do_slice: bool,
    }

    impl ExecutorTaskDescription for IntDesc {
        type KeyType = i32;
        type ResultType = Arc<i32>;

        fn primary_key(&self) -> &Self::KeyType {
            &self.key
        }

        fn can_join(&self, other: &Self) -> bool {
            self.key == other.key
        }

        fn slice_result(&self, result: &Self::ResultType) -> Option<Self::ResultType> {
            if !self.do_slice || *result.as_ref() % 2 == 0 {
                Some(result.clone())
            } else {
                None
            }
        }
    }

    #[tokio::test]
    async fn test_slicing() {
        let d1 = IntDesc {
            key: 1,
            do_slice: false,
        };

        let d2 = IntDesc {
            key: 1,
            do_slice: true,
        };

        let e = Executor::new(1);

        let mut s1 = e
            .submit_stream(
                &d1,
                futures::stream::iter(vec![Arc::new(1), Arc::new(2), Arc::new(3), Arc::new(4)]),
            )
            .await
            .unwrap();

        let mut s2 = e
            .submit_stream(
                &d2,
                futures::stream::iter(vec![Arc::new(1), Arc::new(2), Arc::new(3), Arc::new(4)]),
            )
            .await
            .unwrap();

        let mut res1: Vec<Arc<i32>> = vec![];
        let mut res2: Vec<Arc<i32>> = vec![];

        loop {
            tokio::select! {
                Some(v) = s1.next() => {
                    res1.push(v);
                },
                Some(v) = s2.next() => {
                    res2.push(v);
                },
                else => {
                    break;
                }
            }
        }

        assert_eq!(4, res1.len());
        assert_eq!(2, res2.len());
        assert!(Arc::ptr_eq(res1.get(1).unwrap(), res2.get(0).unwrap()));
        assert!(Arc::ptr_eq(res1.get(3).unwrap(), res2.get(1).unwrap()));
    }
}
