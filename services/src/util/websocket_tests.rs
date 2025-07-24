use actix_http::{
    BoxedPayloadStream, header,
    ws::{OpCode, Parser},
};
use actix_web::{
    FromRequest, HttpRequest, HttpResponse,
    body::MessageBody,
    error::PayloadError,
    test::TestRequest,
    web::{Bytes, BytesMut, Payload},
};
use futures::{
    SinkExt, Stream, StreamExt,
    channel::mpsc,
    stream::{poll_fn, unfold},
};

/// Sends a text message to the websocket session.
///
/// # Panics
///
/// - if sending message to a channel fails
///
pub async fn send_text(sender: &mut mpsc::Sender<Result<Bytes, PayloadError>>, text: &str) {
    let mut buf = BytesMut::new();
    Parser::write_message(&mut buf, text, OpCode::Text, true, true);
    sender.send(Ok(buf.freeze())).await.unwrap();
}

/// Sends a close message to the websocket session.
///
/// # Panics
///
/// - if sending message to a channel fails
///
pub async fn send_close(sender: &mut mpsc::Sender<Result<Bytes, PayloadError>>) {
    let mut buf = BytesMut::new();
    Parser::write_close(&mut buf, None, true);
    sender.send(Ok(buf.freeze())).await.unwrap();
}

/// Creates a test client for websocket requests.
///
/// # Panics
///
/// - if the request cannot be converted to a `Payload`.
/// - if sending message to a channel fails.
///
pub async fn test_client() -> (
    HttpRequest,
    Payload,
    mpsc::Sender<Result<Bytes, PayloadError>>,
    mpsc::Sender<()>,
) {
    let req: HttpRequest = TestRequest::get()
        .insert_header((header::UPGRADE, "websocket"))
        .insert_header((header::CONNECTION, "upgrade"))
        .insert_header((header::SEC_WEBSOCKET_VERSION, "13"))
        .insert_header((header::SEC_WEBSOCKET_KEY, "dGhpcyBpcyBhIHRlc3Q="))
        .to_http_request();

    let (input_tx, input_rx) = mpsc::channel(1);
    let (send_next_msg_trigger, send_next_msg) = mpsc::channel(1);

    let payload_stream: BoxedPayloadStream = unfold(
        (input_rx, send_next_msg),
        |(mut input_rx, mut send_next_msg)| async move {
            send_next_msg.next().await.unwrap();
            let value = input_rx.next().await?;
            Some((value, (input_rx, send_next_msg)))
        },
    )
    .boxed();
    let payload = Payload::from_request(&req, &mut payload_stream.into())
        .await
        .unwrap();

    (req, payload, input_tx, send_next_msg_trigger)
}

/// Creates a stream of message bytes from the response body.
///
/// # Panics
///
/// - if the response body cannot be converted to a stream of bytes
/// - if the response body is not a valid websocket message
///
pub fn response_messages(
    response: HttpResponse,
    send_next_msg_trigger: mpsc::Sender<()>,
) -> impl Stream<Item = Bytes> {
    let mut response_body = response.into_body();

    let response_stream = poll_fn(move |cx| response_body.as_pin_mut().poll_next(cx));
    unfold(
        (response_stream, send_next_msg_trigger),
        move |(mut response_stream, mut send_next_msg_trigger)| async move {
            let _ = send_next_msg_trigger.send(()).await;
            let bytes = response_stream.next().await?;
            let bytes = bytes.unwrap();

            let (_b, opcode, bytes) = Parser::parse(&mut bytes.into(), false, 1_000_000)
                .unwrap()
                .unwrap();

            if opcode == OpCode::Close {
                return None; // close the session
            }
            let Some(bytes) = bytes else {
                return None; // no more bytes
            };

            Some((bytes.freeze(), (response_stream, send_next_msg_trigger)))
        },
    )
}
