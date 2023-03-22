use crate::{contexts::SessionContext, error::Result};
use actix::{
    fut::wrap_future, Actor, ActorContext, ActorFutureExt, AsyncContext, SpawnHandle, StreamHandler,
};
use actix_http::ws::{CloseCode, CloseReason, Item};
use actix_web_actors::ws;
use futures::{stream::BoxStream, FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use geoengine_datatypes::{collections::FeatureCollectionIpc, primitives::VectorQueryRectangle};
use geoengine_operators::{
    call_on_generic_vector_processor,
    engine::{QueryAbortTrigger, QueryContext, QueryProcessorExt, VectorOperator},
};

pub struct VectorWebsocketStreamHandler {
    state: VectorWebsocketStreamHandlerState,
    abort_handle: Option<QueryAbortTrigger>,
}

type ByteStream = BoxStream<'static, StreamResult>;
type StreamResult = Result<Vec<u8>>;

enum VectorWebsocketStreamHandlerState {
    Closed,
    Idle { stream: ByteStream },
    Processing { _fut: SpawnHandle },
}

impl Default for VectorWebsocketStreamHandlerState {
    fn default() -> Self {
        Self::Closed
    }
}

impl Actor for VectorWebsocketStreamHandler {
    type Context = ws::WebsocketContext<Self>;
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for VectorWebsocketStreamHandler {
    fn started(&mut self, _ctx: &mut Self::Context) {}

    fn finished(&mut self, ctx: &mut Self::Context) {
        ctx.stop();

        self.abort_processing();
    }

    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Text(text)) if &text == "NEXT" => self.next_chunk(ctx),
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);

                self.finished(ctx);
            }
            // for now, we ignore all other messages
            _ => (),
        }
    }
}

impl VectorWebsocketStreamHandler {
    pub async fn new<C: SessionContext>(
        vector_operator: Box<dyn VectorOperator>,
        query_rectangle: VectorQueryRectangle,
        execution_ctx: C::ExecutionContext,
        mut query_ctx: C::QueryContext,
    ) -> Result<Self> {
        let initialized_operator = vector_operator.initialize(&execution_ctx).await?;

        let spatial_reference = initialized_operator.result_descriptor().spatial_reference;

        let query_processor = initialized_operator.query_processor()?;

        let abort_handle = query_ctx.abort_trigger().ok();

        let byte_stream = call_on_generic_vector_processor!(query_processor, p => {
            let batch_stream = p
                .query_into_owned_stream(query_rectangle, Box::new(query_ctx))
                .await?;

            batch_stream.and_then(
                move |batch| crate::util::spawn_blocking(
                    move || batch.to_arrow_ipc_file_bytes(spatial_reference).map_err(Into::into)
                ).err_into().map(| r | r.and_then(std::convert::identity))
            ).boxed()
        });

        // TODO: think about buffering the stream?

        Ok(Self {
            state: VectorWebsocketStreamHandlerState::Idle {
                stream: byte_stream.map_err(Into::into).boxed(),
            },
            abort_handle,
        })
    }

    pub fn next_chunk(&mut self, ctx: &mut <Self as Actor>::Context) {
        let state = std::mem::take(&mut self.state);

        self.state = match state {
            VectorWebsocketStreamHandlerState::Closed => {
                self.finished(ctx);
                return;
            }
            VectorWebsocketStreamHandlerState::Idle { mut stream } => {
                VectorWebsocketStreamHandlerState::Processing {
                    _fut: ctx.spawn(
                        wrap_future(async move {
                            let tile = stream.next().await;

                            (tile, stream)
                        })
                        .then(send_result),
                    ),
                }
            }
            VectorWebsocketStreamHandlerState::Processing { _fut: _ } => state,
        };
    }

    pub fn abort_processing(&mut self) {
        if let Some(abort_handle) = self.abort_handle.take() {
            abort_handle.abort();
        }
    }
}

fn send_result(
    (tile, stream): (Option<StreamResult>, ByteStream),
    actor: &mut VectorWebsocketStreamHandler,
    ctx: &mut <VectorWebsocketStreamHandler as Actor>::Context,
) -> futures::future::Ready<()> {
    // TODO: spawn thread instead of blocking and returning an ok future

    match tile {
        Some(Ok(tile)) => {
            const MESSAGE_MAX_SIZE: usize = 16 * 1024 * 1024; // 16 MB

            actor.state = VectorWebsocketStreamHandlerState::Idle { stream };

            // we can send the whole message at once if it is small enough, i.e. <= `MESSAGE_MAX_SIZE`
            if tile.len() <= MESSAGE_MAX_SIZE {
                ctx.binary(tile);
                return futures::future::ready(());
            }

            // limit message chunks to `MESSAGE_MAX_SIZE`
            let mut chunks = tile.chunks(MESSAGE_MAX_SIZE).enumerate();

            while let Some((i, chunk)) = chunks.next() {
                let chunk_bytes = chunk.to_vec().into();
                if i == 0 {
                    // first chunk
                    ctx.write_raw(ws::Message::Continuation(Item::FirstBinary(chunk_bytes)));
                } else if chunks.len() == 0 {
                    // last chunk
                    ctx.write_raw(ws::Message::Continuation(Item::Last(chunk_bytes)));
                } else {
                    ctx.write_raw(ws::Message::Continuation(Item::Continue(chunk_bytes)));
                }
            }
        }
        Some(Err(e)) => {
            // on error, send the error and close the connection
            actor.state = VectorWebsocketStreamHandlerState::Closed;
            ctx.close(Some(CloseReason {
                code: CloseCode::Error,
                description: Some(e.to_string()),
            }));
            actor.finished(ctx);
        }
        None => {
            // stream ended
            actor.state = VectorWebsocketStreamHandlerState::Closed;
            ctx.close(Some(CloseReason {
                code: CloseCode::Normal,
                description: None,
            }));
            actor.finished(ctx);
        }
    }

    futures::future::ready(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        contexts::{InMemoryContext, InMemorySessionContext, SimpleApplicationContext},
        workflows::workflow::Workflow,
    };
    use actix_http::error::PayloadError;
    use actix_web_actors::ws::WebsocketContext;
    use bytes::{Bytes, BytesMut};
    use futures::channel::mpsc::UnboundedSender;
    use geoengine_datatypes::{
        collections::MultiPointCollection,
        primitives::{
            BoundingBox2D, DateTime, FeatureData, MultiPoint, SpatialResolution, TimeInterval,
        },
        util::{arrow::arrow_ipc_file_to_record_batches, test::TestDefault},
    };
    use geoengine_operators::{engine::TypedOperator, mock::MockFeatureCollectionSource};

    #[tokio::test]
    async fn test_websocket_stream() {
        fn send_next(input_sender: &UnboundedSender<Result<Bytes, PayloadError>>) {
            let mut buf = BytesMut::new();
            actix_http::ws::Parser::write_message(
                &mut buf,
                "NEXT",
                actix_http::ws::OpCode::Text,
                true,
                true,
            );

            input_sender.unbounded_send(Ok(buf.into())).unwrap();
        }

        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 3.1)]).unwrap(),
            vec![
                TimeInterval::new(
                    DateTime::new_utc(2014, 1, 1, 0, 0, 0),
                    DateTime::new_utc(2015, 1, 1, 0, 0, 0)
                )
                .unwrap();
                3
            ],
            [
                (
                    "foobar".to_string(),
                    FeatureData::NullableInt(vec![Some(0), None, Some(2)]),
                ),
                (
                    "strings".to_string(),
                    FeatureData::Text(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                ),
            ]
            .iter()
            .cloned()
            .collect(),
        )
        .unwrap();

        let app_ctx = InMemoryContext::new_with_context_spec(
            TestDefault::test_default(),
            usize::MAX.into(), // ensure that we get one chunk per input
        );
        let ctx = app_ctx.default_session_context().await;

        let workflow = Workflow {
            operator: TypedOperator::Vector(
                MockFeatureCollectionSource::multiple(vec![
                    collection.clone(),
                    collection.clone(),
                    collection.clone(),
                ])
                .boxed(),
            ),
        };

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new_upper_left_lower_right(
                (-180., 90.).into(),
                (180., -90.).into(),
            )
            .unwrap(),
            time_interval: TimeInterval::new_instant(DateTime::new_utc(2014, 3, 1, 0, 0, 0))
                .unwrap(),
            spatial_resolution: SpatialResolution::one(),
        };

        let handler = VectorWebsocketStreamHandler::new::<InMemorySessionContext>(
            workflow.operator.get_vector().unwrap(),
            query_rectangle,
            ctx.execution_context().unwrap(),
            ctx.query_context().unwrap(),
        )
        .await
        .unwrap();

        let (input_sender, input_receiver) = futures::channel::mpsc::unbounded();

        let mut websocket_context = WebsocketContext::create(handler, input_receiver);

        // 3 batches
        for _ in 0..3 {
            send_next(&input_sender);

            let bytes = websocket_context.next().await.unwrap().unwrap();

            let bytes = &bytes[4..]; // the first four bytes are WS op bytes

            let record_batches = arrow_ipc_file_to_record_batches(bytes).unwrap();
            assert_eq!(record_batches.len(), 1);
            let record_batch = record_batches.first().unwrap();
            let schema = record_batch.schema();

            assert_eq!(schema.metadata()["spatialReference"], "EPSG:4326");

            assert_eq!(record_batch.column_by_name("foobar").unwrap().len(), 3);
            assert_eq!(record_batch.column_by_name("strings").unwrap().len(), 3);
        }

        send_next(&input_sender);
        assert_eq!(websocket_context.next().await.unwrap().unwrap().len(), 4); // close frame

        send_next(&input_sender);
        assert!(websocket_context.next().await.is_none());
    }
}
