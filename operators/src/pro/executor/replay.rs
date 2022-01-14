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

#[allow(clippy::missing_panics_doc)]
pub fn channel<T: Clone + Unpin>(queue_size: usize) -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Mutex::new(Inner::new(queue_size)));

    let (tx_id, rx_id) = {
        let mut lock = inner.lock().unwrap();
        (lock.create_sender(), lock.create_receiver(true))
    };

    let tx = Sender {
        id: tx_id,
        pending: Default::default(),
        inner: inner.clone(),
    };
    let rx = Receiver {
        id: rx_id.unwrap(),
        pending: Default::default(),
        inner,
    };
    (tx, rx)
}

struct BufferEntry<T> {
    expected_receivers: usize,
    value: T,
}

struct Inner<T> {
    consumer_id_seq: u32,
    sender_id_seq: u32,
    buffer: VecDeque<BufferEntry<T>>,
    queue_size: usize,
    offset: usize,
    receivers: HashMap<u32, InnerReceiver>,
    senders: HashMap<u32, InnerSender>,
}

struct InnerReceiver {
    idx: usize,
    waker: Option<Waker>,
}

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
    fn new(queue_size: usize) -> Inner<T> {
        Inner {
            consumer_id_seq: 0,
            sender_id_seq: 0,
            buffer: VecDeque::with_capacity(queue_size),
            queue_size,
            offset: 0,
            receivers: HashMap::new(),
            senders: HashMap::new(),
        }
    }

    fn create_sender(&mut self) -> u32 {
        let id = self.sender_id_seq;
        self.sender_id_seq += 1;

        assert!(self
            .senders
            .insert(id, InnerSender { waker: None },)
            .is_none());
        id
    }

    fn remove_sender(&mut self, id: u32) {
        if self.senders.remove(&id).is_some() && self.senders.is_empty() {
            self.notify_receivers();
        }
    }

    fn clean_up_buffer(&mut self) -> bool {
        while self.buffer.len() >= self.queue_size
            && self
                .buffer
                .front()
                .map_or(false, |e| e.expected_receivers == 0)
        {
            self.buffer.pop_front().expect("");
            self.offset += 1;
        }
        self.buffer.len() < self.queue_size
    }

    fn try_send(&mut self, value: T) -> Result<(), SendError<T>> {
        if self.receivers.is_empty() {
            Err(SendError::Closed(value))
        } else if self.clean_up_buffer() {
            self.buffer.push_back(BufferEntry {
                expected_receivers: self.receivers.len(),
                value,
            });
            self.notify_receivers();
            Ok(())
        } else {
            Err(SendError::Full(value))
        }
    }

    fn notify_senders(&mut self) {
        for v in self.senders.values_mut() {
            if let Some(w) = v.waker.take() {
                w.wake();
            }
        }
    }

    fn update_send_waker(&mut self, sender_id: u32, waker: &Waker) {
        self.senders
            .get_mut(&sender_id)
            .unwrap()
            .update_waker(waker);
    }

    fn remove_send_waker(&mut self, sender_id: u32) {
        self.senders.get_mut(&sender_id).unwrap().remove_waker();
    }

    fn create_receiver(&mut self, first: bool) -> Result<u32, SubscribeError> {
        if self.offset > 0 {
            return Err(SubscribeError::WouldLag);
        } else if !first && self.receivers.is_empty() {
            return Err(SubscribeError::Closed);
        }

        // Increment expected read count
        for v in &mut self.buffer {
            v.expected_receivers += 1;
        }

        let id = self.consumer_id_seq;
        self.consumer_id_seq += 1;

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

    fn remove_receiver(&mut self, id: u32) {
        if let Some(r) = self.receivers.remove(&id) {
            let idx = r.idx - self.offset;
            let mut notify = false;
            for i in idx..self.buffer.len() {
                let e = &mut self.buffer[i];
                e.expected_receivers -= 1;
                notify |= e.expected_receivers == 0;
            }
            if notify {
                self.notify_senders();
            }
        }
    }

    fn try_recv(&mut self, receiver_id: u32) -> Result<T, ReceiveError> {
        let c = self.receivers.get_mut(&receiver_id).unwrap();
        let q_idx = c.idx - self.offset;

        let res = match self.buffer.get_mut(q_idx) {
            Some(e) => {
                e.expected_receivers -= 1;
                Ok(e.value.clone())
            }
            None if self.senders.is_empty() => Err(ReceiveError::Closed),
            None => Err(ReceiveError::NoMessage),
        };

        match res {
            Ok(v) => {
                c.idx += 1;
                self.notify_senders();
                Ok(v)
            }
            Err(e) => Err(e),
        }
    }

    fn notify_receivers(&mut self) {
        for v in self.receivers.values_mut() {
            if let Some(w) = v.waker.take() {
                w.wake();
            }
        }
    }

    fn remove_receiver_waker(&mut self, receiver_id: u32) {
        self.receivers.get_mut(&receiver_id).unwrap().remove_waker();
    }

    fn update_receiver_waker(&mut self, receiver_id: u32, waker: &Waker) {
        self.receivers
            .get_mut(&receiver_id)
            .unwrap()
            .update_waker(waker);
    }
}

// /////////////////////////////////////////////////////////////////////////////
//
// SENDER SIDE
//
// /////////////////////////////////////////////////////////////////////////////

pub struct Sender<T>
where
    T: Clone,
{
    id: u32,
    pending: AtomicUsize,
    inner: Arc<Mutex<Inner<T>>>,
}

#[allow(clippy::missing_panics_doc)]
impl<T> Sender<T>
where
    T: Clone,
{
    pub fn subscribe(&self) -> Result<Receiver<T>, SubscribeError> {
        let id = {
            let mut lock = self.inner.lock().unwrap();
            lock.create_receiver(false)
        }?;

        Ok(Receiver {
            id,
            pending: Default::default(),
            inner: self.inner.clone(),
        })
    }

    pub fn try_send(&self, v: T) -> Result<(), SendError<T>> {
        let mut lock = self.inner.lock().unwrap();
        lock.try_send(v)
    }
}

#[allow(clippy::missing_panics_doc)]
impl<T> Sender<T>
where
    T: Clone + Unpin,
{
    pub fn send(&self, v: T) -> Send<T> {
        let _ignore = self.pending.fetch_add(1, Ordering::Relaxed);
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
        let value = send.value.take().unwrap();
        let mut lock = self.inner.lock().unwrap();
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
        if self.pending.fetch_sub(1, Ordering::Relaxed) == 1 {
            let mut lock = self.inner.lock().unwrap();
            lock.remove_send_waker(self.id);
        }
    }
}

#[allow(clippy::missing_panics_doc)]
impl<T> Clone for Sender<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        let id = {
            let mut lock = self.inner.lock().unwrap();
            lock.create_sender()
        };
        Sender {
            id,
            pending: Default::default(),
            inner: self.inner.clone(),
        }
    }
}

#[allow(clippy::missing_panics_doc)]
impl<T> Drop for Sender<T>
where
    T: Clone,
{
    fn drop(&mut self) {
        let mut lock = self.inner.lock().unwrap();
        lock.remove_sender(self.id);
    }
}

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

pub struct Receiver<T>
where
    T: Clone,
{
    id: u32,
    pending: AtomicUsize,
    inner: Arc<Mutex<Inner<T>>>,
}

#[allow(clippy::missing_panics_doc)]
impl<T> Receiver<T>
where
    T: Clone,
{
    pub fn try_recv(&self) -> Result<T, ReceiveError> {
        let mut lock = self.inner.lock().unwrap();
        lock.try_recv(self.id)
    }

    pub fn recv(&self) -> Recv<T> {
        let _ignore = self.pending.fetch_add(1, Ordering::Relaxed);
        Recv { receiver: self }
    }

    fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut lock = self.inner.lock().unwrap();
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
        if self.pending.fetch_sub(1, Ordering::Relaxed) == 1 {
            let mut lock = self.inner.lock().unwrap();
            lock.remove_receiver_waker(self.id);
        }
    }
}

#[allow(clippy::missing_panics_doc)]
impl<T> Drop for Receiver<T>
where
    T: Clone,
{
    fn drop(&mut self) {
        let mut lock = self.inner.lock().unwrap();
        lock.remove_receiver(self.id);
    }
}

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
}
