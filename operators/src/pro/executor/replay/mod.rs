use futures::{Future, Stream};
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// Possible errors when sending data to the channel
pub enum SendError<T> {
    /// The channel is closed (i.e., all receivers were dropped)
    Closed(T),
    /// The replay buffer is full - try again later
    Full(T),
}

/// Possible errors when sending data to the channel
#[derive(Clone, Debug)]
pub enum ReceiveError {
    /// The channel is closed - no more data will be delivered
    Closed,
    /// Currently no message present - try again later
    NoMessage,
}

/// Possible errors when subscribing to the channel
#[derive(Clone, Debug)]
pub enum SubscribeError {
    /// The channel progressed too far, joining not possible anymore
    WouldLag,
    /// The channel is closed already, joining not possible anymore
    Closed,
}

/// Creates a new replay channel with the specified queue size. This means
/// that the channel holds at most `queue_size` results.
/// In contrast to, e.g., tokio's broadcast channel, lagging receivers
/// are dropped or do not miss any result.
/// However, this means that results can only be processed at the pace
/// of the slowest receiver.
pub fn channel<T: Clone + Unpin>(queue_size: usize) -> (Sender<T>, Receiver<T>) {
    let mut inner = Inner::new(queue_size);

    let (tx_id, rx_id) = {
        (
            inner.create_sender(),
            inner
                .create_receiver(true)
                .expect("Initial receiver creation must succeed."),
        )
    };

    let inner = Arc::new(Mutex::new(inner));

    let tx = Sender {
        id: tx_id,
        pending_futures: Default::default(),
        inner: inner.clone(),
    };
    let rx = Receiver {
        id: rx_id,
        pending_futures: Default::default(),
        inner,
    };
    (tx, rx)
}

/// An entry in the replay queue. Every entry
/// consists of the actual result and the expected number
/// of receivers.
/// This count is decremented every time a receivers consumes
/// the result. When this count reaches `0`, the entry can
/// safely be discarded from the queue.
struct QueueEntry<T> {
    expected_receivers: usize,
    value: T,
}

/// The actual channel construct.
struct Inner<T> {
    receiver_id_seq: u32,
    sender_id_seq: u32,
    queue: VecDeque<QueueEntry<T>>,
    queue_size: usize,
    offset: usize,
    receivers: HashMap<u32, InnerReceiver>,
    senders: HashMap<u32, InnerSender>,
}

/// A registered receiver that carries
/// its next read index.
struct InnerReceiver {
    idx: usize,
    waker: Option<Waker>,
}

/// A registered sender
struct InnerSender {
    waker: Option<Waker>,
}

impl InnerSender {
    fn update_waker(&mut self, waker: &Waker) {
        update_waker(&mut self.waker, waker);
    }

    fn remove_waker(&mut self) {
        self.waker = None;
    }
}

impl InnerReceiver {
    fn update_waker(&mut self, waker: &Waker) {
        update_waker(&mut self.waker, waker);
    }

    fn remove_waker(&mut self) {
        self.waker = None;
    }
}

fn update_waker(opt: &mut Option<Waker>, waker: &Waker) {
    match opt {
        Some(w) if w.will_wake(waker) => {}
        _ => {
            let _ignore = opt.replace(waker.clone());
        }
    };
}

impl<T> Inner<T>
where
    T: Clone,
{
    /// Creates a new Innter instance with the given `queue_size`.
    fn new(queue_size: usize) -> Inner<T> {
        Inner {
            receiver_id_seq: 0,
            sender_id_seq: 0,
            queue: VecDeque::with_capacity(queue_size),
            queue_size,
            offset: 0,
            receivers: HashMap::new(),
            senders: HashMap::new(),
        }
    }

    /// Creates a new sender and returns its `id`.
    fn create_sender(&mut self) -> u32 {
        let id = self.sender_id_seq;
        self.sender_id_seq += 1;

        assert!(self
            .senders
            .insert(id, InnerSender { waker: None },)
            .is_none());
        id
    }

    /// Removes the sender with the given `id`.
    fn remove_sender(&mut self, id: u32) {
        if self.senders.remove(&id).is_some() && self.senders.is_empty() {
            self.notify_receivers();
        }
    }

    /// Tries make space for a new result in the queue.
    /// This attempt may fail, if the queue is full and
    /// at least one consumer did not receive the eldest element yet.
    fn clean_up_queue(&mut self) -> bool {
        while self.queue.len() >= self.queue_size
            && self
                .queue
                .front()
                .map_or(false, |e| e.expected_receivers == 0)
        {
            self.queue.pop_front().expect("");
            self.offset += 1;
        }
        self.queue.len() < self.queue_size
    }

    /// Tries to send the given value into the channel.
    ///
    /// #Errors
    /// This method fails with [`Closed`](SendError::Closed) if no receivers are left.
    /// Moreover, [`Full`](SendError::Full) is returned if there is no more space
    /// within the queue.
    fn try_send(&mut self, value: T) -> Result<(), SendError<T>> {
        if self.receivers.is_empty() {
            Err(SendError::Closed(value))
        } else if self.clean_up_queue() {
            self.queue.push_back(QueueEntry {
                expected_receivers: self.receivers.len(),
                value,
            });
            self.notify_receivers();
            Ok(())
        } else {
            Err(SendError::Full(value))
        }
    }

    /// Notifies all pending senders.
    fn notify_senders(&mut self) {
        for v in self.senders.values_mut() {
            if let Some(w) = v.waker.take() {
                w.wake();
            }
        }
    }

    /// Updates the waker for the sender with the given `sender_id`.
    fn update_send_waker(&mut self, sender_id: u32, waker: &Waker) {
        if let Some(s) = self.senders.get_mut(&sender_id) {
            s.update_waker(waker);
        }
    }

    /// Removes the waker for the sender with the given `sender_id`.
    fn remove_send_waker(&mut self, sender_id: u32) {
        if let Some(s) = self.senders.get_mut(&sender_id) {
            s.remove_waker();
        }
    }

    /// Creates a new receivers and returns its id.
    ///
    /// # Errors
    /// This methods fails with [`WouldLag`](SubscribeError::WouldLag), if the first element of
    /// the result stream was evicted already. Moreover, it returns [`Closed`](SubscribeError::Closed),
    /// if the channel was closed already (i.e., all senders or receivers were dropped).
    ///
    fn create_receiver(&mut self, first: bool) -> Result<u32, SubscribeError> {
        if self.offset > 0 {
            return Err(SubscribeError::WouldLag);
        } else if !first && self.receivers.is_empty() {
            return Err(SubscribeError::Closed);
        }

        // Increment expected read count
        for v in &mut self.queue {
            v.expected_receivers += 1;
        }

        let id = self.receiver_id_seq;
        self.receiver_id_seq += 1;

        assert!(self
            .receivers
            .insert(
                id,
                InnerReceiver {
                    idx: 0,
                    waker: None,
                },
            )
            .is_none());
        Ok(id)
    }

    /// Removes the receiver with the given `id`.
    fn remove_receiver(&mut self, id: u32) {
        if let Some(r) = self.receivers.remove(&id) {
            let idx = r.idx - self.offset;
            let mut notify = false;
            for i in idx..self.queue.len() {
                let e = &mut self.queue[i];
                e.expected_receivers -= 1;
                notify |= e.expected_receivers == 0;
            }
            if notify {
                self.notify_senders();
            }
        }
    }

    /// Tries to receive the next message for the receiver with the given `receiver_id`.
    ///
    /// # Errors
    /// Returns [`NoMessage`](ReceiveError::NoMessage) if there is currently no new message available
    /// for the given receiver - please try again later.  
    ///
    /// Moreover, it returns [`Closed`](ReceiveError::Closed) if the channel is closed. No more data
    /// will be delivered in this case.
    fn try_recv(&mut self, receiver_id: u32) -> Result<T, ReceiveError> {
        let recv = self
            .receivers
            .get_mut(&receiver_id)
            .expect("Unknown receiver id.");

        let q_idx = recv.idx - self.offset;

        let res = match self.queue.get_mut(q_idx) {
            Some(e) => {
                e.expected_receivers -= 1;
                Ok(e.value.clone())
            }
            None if self.senders.is_empty() => Err(ReceiveError::Closed),
            None => Err(ReceiveError::NoMessage),
        }?;

        recv.idx += 1;
        self.notify_senders();
        Ok(res)
    }

    /// Notifies all waiting receivers.
    fn notify_receivers(&mut self) {
        for v in self.receivers.values_mut() {
            if let Some(w) = v.waker.take() {
                w.wake();
            }
        }
    }

    /// Updates the waker for the receiver with the given `receiver_id`.
    fn update_receiver_waker(&mut self, receiver_id: u32, waker: &Waker) {
        self.receivers
            .get_mut(&receiver_id)
            .unwrap()
            .update_waker(waker);
    }

    /// Removes the waker for the receiver with the given `receiver_id`.
    fn remove_receiver_waker(&mut self, receiver_id: u32) {
        self.receivers.get_mut(&receiver_id).unwrap().remove_waker();
    }
}

// /////////////////////////////////////////////////////////////////////////////
//
// SENDER SIDE
//
// /////////////////////////////////////////////////////////////////////////////

/// The sender side of a replay channel
pub struct Sender<T>
where
    T: Clone,
{
    id: u32,
    pending_futures: AtomicUsize,
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Sender<T>
where
    T: Clone,
{
    /// Registers a new receiver to the channel and returns it.
    ///
    /// # Errors
    /// This methods fails with [`WouldLag`](SubscribeError::WouldLag), if the first element of
    /// the result stream was evicted already. Moreover, it returns [`Closed`](SubscribeError::Closed),
    /// if the channel was closed already.
    pub fn subscribe(&self) -> Result<Receiver<T>, SubscribeError> {
        let id = {
            let mut lock = crate::util::safe_lock_mutex(&self.inner);
            lock.create_receiver(false)
        }?;

        Ok(Receiver {
            id,
            pending_futures: Default::default(),
            inner: self.inner.clone(),
        })
    }

    /// Tries to send the given value immediately. If this is not possible, the
    /// value is returned with the error.
    ///
    /// # Errors
    /// This method fails with [`Closed`](SendError::Closed) if no receivers are left.
    /// Moreover, [`Full`](SendError::Full) is returned if there is no more space
    /// within the queue.
    pub fn try_send(&self, v: T) -> Result<(), SendError<T>> {
        let mut lock = crate::util::safe_lock_mutex(&self.inner);
        lock.try_send(v)
    }
}

impl<T> Sender<T>
where
    T: Clone + Unpin,
{
    /// Sends a value, waiting until there is capacity.
    pub fn send(&self, v: T) -> Send<T> {
        let _ignore = self.pending_futures.fetch_add(1, Ordering::Relaxed);
        Send {
            sender: self,
            value: Some(v),
        }
    }

    fn poll_send(
        &self,
        send: &mut Send<'_, T>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), SendError<T>>> {
        let value = send.value.take().expect("Send without message.");
        let mut lock = crate::util::safe_lock_mutex(&self.inner);
        match lock.try_send(value) {
            Ok(_) => Poll::Ready(Ok(())),
            Err(SendError::Full(v)) => {
                lock.update_send_waker(self.id, cx.waker());
                let _ignore = send.value.insert(v);
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn send_dropped(&self) {
        if self.pending_futures.fetch_sub(1, Ordering::Relaxed) == 1 {
            let mut lock = crate::util::safe_lock_mutex(&self.inner);
            lock.remove_send_waker(self.id);
        }
    }
}

impl<T> Clone for Sender<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        let id = {
            let mut lock = crate::util::safe_lock_mutex(&self.inner);
            lock.create_sender()
        };
        Sender {
            id,
            pending_futures: Default::default(),
            inner: self.inner.clone(),
        }
    }
}

impl<T> Drop for Sender<T>
where
    T: Clone,
{
    fn drop(&mut self) {
        let mut lock = crate::util::safe_lock_mutex(&self.inner);
        lock.remove_sender(self.id);
    }
}

/// A message waiting to be send.
pub struct Send<'sender, T>
where
    T: Clone + Unpin,
{
    sender: &'sender Sender<T>,
    value: Option<T>,
}

impl<T> Drop for Send<'_, T>
where
    T: Clone + Unpin,
{
    fn drop(&mut self) {
        self.sender.send_dropped();
    }
}

impl<T> Future for Send<'_, T>
where
    T: Clone + Unpin,
{
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.sender.poll_send(self.get_mut(), cx)
    }
}

// /////////////////////////////////////////////////////////////////////////////
//
// RECEIVER SIDE
//
// /////////////////////////////////////////////////////////////////////////////

/// The receiver side of a replay channel
pub struct Receiver<T>
where
    T: Clone,
{
    id: u32,
    pending_futures: AtomicUsize,
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Receiver<T>
where
    T: Clone,
{
    /// Tries to receive the next message immediately.
    ///
    /// # Errors
    /// Returns [`NoMessage`](ReceiveError::NoMessage) if there is currently no new message available
    /// - please try again later.  
    ///
    /// Moreover, it returns [`Closed`](ReceiveError::Closed) if the channel is closed. No more data
    /// will be delivered in this case.
    pub fn try_recv(&self) -> Result<T, ReceiveError> {
        let mut lock = crate::util::safe_lock_mutex(&self.inner);
        lock.try_recv(self.id)
    }

    /// Receives the next value from the channel, possibly waiting for it to arrive.
    pub fn recv(&self) -> Recv<T> {
        let _ignore = self.pending_futures.fetch_add(1, Ordering::Relaxed);
        Recv { receiver: self }
    }

    fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut lock = crate::util::safe_lock_mutex(&self.inner);
        match lock.try_recv(self.id) {
            Ok(v) => Poll::Ready(Some(v)),
            Err(ReceiveError::Closed) => Poll::Ready(None),
            Err(ReceiveError::NoMessage) => {
                lock.update_receiver_waker(self.id, cx.waker());
                Poll::Pending
            }
        }
    }

    pub fn recv_dropped(&self) {
        if self.pending_futures.fetch_sub(1, Ordering::Relaxed) == 1 {
            let mut lock = crate::util::safe_lock_mutex(&self.inner);
            lock.remove_receiver_waker(self.id);
        }
    }
}

impl<T> Drop for Receiver<T>
where
    T: Clone,
{
    fn drop(&mut self) {
        let mut lock = crate::util::safe_lock_mutex(&self.inner);
        lock.remove_receiver(self.id);
    }
}

/// A message waiting to be received.
pub struct Recv<'receiver, T>
where
    T: Clone,
{
    receiver: &'receiver Receiver<T>,
}

impl<T> Drop for Recv<'_, T>
where
    T: Clone,
{
    fn drop(&mut self) {
        self.receiver.recv_dropped();
    }
}

impl<T> Future for Recv<'_, T>
where
    T: Clone,
{
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.receiver.poll_recv(cx)
    }
}

/// A stream representation of the `Receiver`.
pub struct ReceiverStream<T>
where
    T: Clone,
{
    inner: Receiver<T>,
}

impl<T> From<Receiver<T>> for ReceiverStream<T>
where
    T: Clone,
{
    fn from(r: Receiver<T>) -> Self {
        ReceiverStream { inner: r }
    }
}

impl<T> Stream for ReceiverStream<T>
where
    T: Clone,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::matches;
    use tokio::time::Duration;

    #[test]
    fn test_send() {
        let (tx, rx) = channel(2);
        assert!(tx.try_send(1).is_ok());
        assert!(tx.try_send(2).is_ok());
        assert!(matches!(tx.try_send(3), Err(SendError::Full(3))));
        assert!(matches!(rx.try_recv(), Ok(1)));
        assert!(tx.try_send(3).is_ok());
    }

    #[test]
    fn test_all_receivers_gone() {
        let (tx, rx) = channel(2);
        assert!(tx.try_send(1).is_ok());
        drop(rx);
        assert!(matches!(tx.try_send(2), Err(SendError::Closed(2))));
    }

    #[test]
    fn test_send_drop_lagging_receiver() {
        let (tx, rx) = channel(2);
        let rx2 = tx.subscribe().unwrap();
        assert!(tx.try_send(1).is_ok());
        assert!(tx.try_send(2).is_ok());
        assert!(matches!(tx.try_send(3), Err(SendError::Full(3))));
        assert!(matches!(rx.try_recv(), Ok(1)));
        drop(rx2);
        assert!(tx.try_send(3).is_ok());
    }

    #[test]
    fn test_receive() {
        let (tx, rx) = channel(2);
        let rx2 = tx.subscribe().unwrap();
        assert!(tx.try_send(1).is_ok());
        assert!(tx.try_send(2).is_ok());
        assert!(matches!(tx.try_send(3), Err(SendError::Full(3))));
        assert!(matches!(rx.try_recv(), Ok(1)));
        assert!(matches!(rx.try_recv(), Ok(2)));
        assert!(matches!(rx.try_recv(), Err(ReceiveError::NoMessage)));
        assert!(matches!(rx2.try_recv(), Ok(1)));
        assert!(tx.try_send(3).is_ok());
        assert!(matches!(rx.try_recv(), Ok(3)));
        assert!(matches!(rx2.try_recv(), Ok(2)));
        assert!(matches!(rx2.try_recv(), Ok(3)));
        assert!(matches!(rx.try_recv(), Err(ReceiveError::NoMessage)));
        assert!(matches!(rx2.try_recv(), Err(ReceiveError::NoMessage)));
    }

    #[test]
    fn test_receive_after_tx_close() {
        let (tx, rx) = channel(2);
        let rx2 = tx.subscribe().unwrap();
        assert!(tx.try_send(1).is_ok());
        assert!(tx.try_send(2).is_ok());
        assert!(matches!(tx.try_send(3), Err(SendError::Full(3))));
        assert!(matches!(rx.try_recv(), Ok(1)));
        assert!(matches!(rx.try_recv(), Ok(2)));
        assert!(matches!(rx.try_recv(), Err(ReceiveError::NoMessage)));
        assert!(matches!(rx2.try_recv(), Ok(1)));
        assert!(tx.try_send(3).is_ok());
        drop(tx);
        assert!(matches!(rx.try_recv(), Ok(3)));
        assert!(matches!(rx2.try_recv(), Ok(2)));
        assert!(matches!(rx2.try_recv(), Ok(3)));
        assert!(matches!(rx.try_recv(), Err(ReceiveError::Closed)));
        assert!(matches!(rx2.try_recv(), Err(ReceiveError::Closed)));
    }

    #[tokio::test]
    async fn test_send_async() {
        let (tx, rx) = channel(2);

        let t1 = tokio::spawn(async move {
            for i in 1..=3 {
                assert!(tx.send(i).await.is_ok());
            }
        });

        let mut result = Vec::new();
        while let Some(v) = rx.recv().await {
            result.push(v);
        }

        assert_eq!(vec![1, 2, 3], result);
        assert!(t1.await.is_ok());
    }

    #[tokio::test]
    async fn test_all_receivers_gone_async() {
        let (tx, rx) = channel(2);
        drop(rx);
        let t1 = tokio::spawn(async move {
            let res = tx.send(1).await;
            assert!(matches!(res, Err(SendError::Closed(1))));
        });
        assert!(t1.await.is_ok());
    }

    #[tokio::test]
    async fn test_send_drop_lagging_receiver_async() {
        let (tx, rx) = channel(2);
        let rx2 = tx.subscribe().unwrap();

        let ct = tokio_util::sync::CancellationToken::new();
        let cloned_token = ct.clone();

        let t1 = tokio::spawn(async move {
            for i in 1..=3 {
                tokio::select! {
                    res = tx.send(i) => {
                        assert!(res.is_ok());
                    },
                    _ = cloned_token.cancelled() => {
                        return false;
                    }
                }
            }
            true
        });

        assert!(matches!(rx.recv().await, Some(1)));
        assert!(matches!(rx.recv().await, Some(2)));
        assert!(matches!(rx.try_recv(), Err(ReceiveError::NoMessage)));
        drop(rx2);
        // Wait until we cancel
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        ct.cancel();
        assert!(matches!(t1.await, Ok(true)));
        assert!(matches!(rx.try_recv(), Ok(3)));
    }

    #[tokio::test]
    async fn test_receive_async() {
        let (tx, rx) = channel(2);
        let rx2 = tx.subscribe().unwrap();

        let f1 = async move {
            for i in 1..=3 {
                assert!(tx.send(i).await.is_ok());
            }
        };

        let f2 = async move {
            let mut res = Vec::new();
            while let Some(v) = rx.recv().await {
                res.push(v);
            }
            res
        };

        let f3 = async move {
            let mut res = Vec::new();
            while let Some(v) = rx2.recv().await {
                res.push(v);
            }
            res
        };

        let (_, r1, r2) = tokio::join!(f1, f2, f3);

        assert_eq!(vec![1, 2, 3], r1);
        assert_eq!(vec![1, 2, 3], r2);
    }

    #[tokio::test]
    async fn test_receive_after_tx_close_async() {
        let (tx, rx) = channel(2);

        assert!(tx.send(1).await.is_ok());
        assert!(tx.send(2).await.is_ok());
        assert!(matches!(rx.recv().await, Some(1)));
        assert!(tx.send(3).await.is_ok());
        drop(tx);
        assert!(matches!(rx.recv().await, Some(2)));
        assert!(matches!(rx.recv().await, Some(3)));
        assert!(matches!(rx.recv().await, None));
    }

    #[tokio::test]
    async fn test_multiple_waiting() {
        let (tx, rx) = channel(2);

        let f1 = tx.send(1);
        let f2 = tx.send(2);
        let f3 = tx.send(3);
        let f4 = tx.send(4);

        let (_r1, _r2) = tokio::join!(f1, f2);
        let (_tx1, _tx2, r1, r2, r3, r4) =
            tokio::join!(f3, f4, rx.recv(), rx.recv(), rx.recv(), rx.recv());

        assert_eq!(Some(1), r1);
        assert_eq!(Some(2), r2);
        assert_eq!(Some(3), r3);
        assert_eq!(Some(4), r4);
    }

    #[tokio::test]
    async fn test_send_multiple_tasks() {
        let (tx, rx) = channel(2);
        let rx2 = tx.subscribe().unwrap();

        let t = tokio::spawn(async move {
            let f1 = tx.send(1);
            let f2 = tx.send(2);
            let f3 = tx.send(3);
            let (_, _, _) = tokio::join!(f1, f2, f3);
        });

        let rt = {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let (r1, r2, r3, r4) = tokio::join!(rx2.recv(), rx2.recv(), rx2.recv(), rx2.recv());
                assert_eq!(Some(1), r1);
                assert_eq!(Some(2), r2);
                assert_eq!(Some(3), r3);
                assert_eq!(None, r4);
            })
        };

        let (r1, r2, r3, r4) = tokio::join!(rx.recv(), rx.recv(), rx.recv(), rx.recv());

        assert_eq!(Some(1), r1);
        assert_eq!(Some(2), r2);
        assert_eq!(Some(3), r3);
        assert_eq!(None, r4);

        let (r1, r2) = tokio::join!(t, rt);
        assert!(r1.is_ok());
        assert!(r2.is_ok());
    }

    #[tokio::test]
    async fn test_receive_stream() {
        let (tx, rx) = channel(2);
        let rx2 = tx.subscribe().unwrap();

        let f1 = async move {
            for i in 1..=3 {
                assert!(tx.send(i).await.is_ok());
            }
        };

        let f2 = async move {
            let s: ReceiverStream<i32> = rx.into();
            s.collect::<Vec<i32>>().await
        };

        let f3 = async move {
            let s: ReceiverStream<i32> = rx2.into();
            s.collect::<Vec<i32>>().await
        };

        let (_, results1, results2) = tokio::join!(f1, f2, f3);

        assert_eq!(vec![1, 2, 3], results1);
        assert_eq!(vec![1, 2, 3], results2);
    }
}
