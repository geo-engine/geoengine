use futures::executor::block_on_stream;
use futures::stream;
use futures::task::Poll;
use futures::Stream;

pub fn fn_stream() -> impl Stream<Item = usize> {
    let mut counter: usize = 2;

    stream::poll_fn(move |_| -> Poll<Option<usize>> {
        if counter == 0 {
            return Poll::Ready(None);
        }
        counter -= 1;
        Poll::Ready(Some(counter))
    })
}

#[test]
fn fn_test() {
    let mut stream = block_on_stream(fn_stream());

    assert_eq!(stream.next(), Some(1));
    assert_eq!(stream.next(), Some(0));
    assert_eq!(stream.next(), None);
}
