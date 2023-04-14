use crate::{contexts::SessionContext, error::Result};
use actix::{
    fut::wrap_future, Actor, ActorContext, ActorFutureExt, AsyncContext, SpawnHandle, StreamHandler,
};
use actix_http::ws::{CloseCode, CloseReason};
use actix_web_actors::ws;
use futures::{stream::BoxStream, FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use geoengine_datatypes::{
    primitives::RasterQueryRectangle, raster::raster_tile_2d_to_arrow_ipc_file,
};
use geoengine_operators::{
    call_on_generic_raster_processor,
    engine::{QueryAbortTrigger, QueryContext, QueryProcessorExt, RasterOperator, WorkflowOperatorPath},
};

pub struct RasterWebsocketStreamHandler {
    state: RasterWebsocketStreamHandlerState,
    abort_handle: Option<QueryAbortTrigger>,
}

type ByteStream = BoxStream<'static, StreamResult>;
type StreamResult = Result<Vec<u8>>;

enum RasterWebsocketStreamHandlerState {
    Closed,
    Idle { stream: ByteStream },
    Processing { _fut: SpawnHandle },
}

impl Default for RasterWebsocketStreamHandlerState {
    fn default() -> Self {
        Self::Closed
    }
}

impl Actor for RasterWebsocketStreamHandler {
    type Context = ws::WebsocketContext<Self>;
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for RasterWebsocketStreamHandler {
    fn started(&mut self, _ctx: &mut Self::Context) {}

    fn finished(&mut self, ctx: &mut Self::Context) {
        ctx.stop();

        self.abort_processing();
    }

    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Text(text)) if &text == "NEXT" => self.next_tile(ctx),
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);

                self.finished(ctx);
            }
            // for now, we ignore all other messages
            _ => (),
        }
    }
}

impl RasterWebsocketStreamHandler {
    pub async fn new<C: SessionContext>(
        raster_operator: Box<dyn RasterOperator>,
        query_rectangle: RasterQueryRectangle,
        execution_ctx: C::ExecutionContext,
        mut query_ctx: C::QueryContext,
    ) -> Result<Self> {
        let workflow_operator_path_root = WorkflowOperatorPath::default();

        let initialized_operator = raster_operator.initialize(workflow_operator_path_root, &execution_ctx).await?;

        let spatial_reference = initialized_operator.result_descriptor().spatial_reference;

        let query_processor = initialized_operator.query_processor()?;

        let abort_handle = query_ctx.abort_trigger().ok();

        let byte_stream = call_on_generic_raster_processor!(query_processor, p => {
            let tile_stream = p
                .query_into_owned_stream(query_rectangle, Box::new(query_ctx))
                .await?;

            tile_stream.and_then(
                move |tile| crate::util::spawn_blocking(
                    move || raster_tile_2d_to_arrow_ipc_file(tile, spatial_reference).map_err(Into::into)
                ).err_into().map(| r | r.and_then(std::convert::identity))
            ).boxed()
        });

        // TODO: think about buffering the stream?

        Ok(Self {
            state: RasterWebsocketStreamHandlerState::Idle {
                stream: byte_stream.map_err(Into::into).boxed(),
            },
            abort_handle,
        })
    }

    pub fn next_tile(&mut self, ctx: &mut <Self as Actor>::Context) {
        let state = std::mem::take(&mut self.state);

        self.state = match state {
            RasterWebsocketStreamHandlerState::Closed => {
                self.finished(ctx);
                return;
            }
            RasterWebsocketStreamHandlerState::Idle { mut stream } => {
                RasterWebsocketStreamHandlerState::Processing {
                    _fut: ctx.spawn(
                        wrap_future(async move {
                            let tile = stream.next().await;

                            (tile, stream)
                        })
                        .then(send_result),
                    ),
                }
            }
            RasterWebsocketStreamHandlerState::Processing { _fut: _ } => state,
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
    actor: &mut RasterWebsocketStreamHandler,
    ctx: &mut <RasterWebsocketStreamHandler as Actor>::Context,
) -> futures::future::Ready<()> {
    match tile {
        Some(Ok(tile)) => {
            actor.state = RasterWebsocketStreamHandlerState::Idle { stream };
            ctx.binary(tile);
        }
        Some(Err(e)) => {
            // on error, send the error and close the connection
            actor.state = RasterWebsocketStreamHandlerState::Closed;
            ctx.close(Some(CloseReason {
                code: CloseCode::Error,
                description: Some(e.to_string()),
            }));
            actor.finished(ctx);
        }
        None => {
            // stream ended
            actor.state = RasterWebsocketStreamHandlerState::Closed;
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
        util::tests::register_ndvi_workflow_helper,
    };
    use actix_http::error::PayloadError;
    use actix_web_actors::ws::WebsocketContext;
    use bytes::{Bytes, BytesMut};
    use futures::channel::mpsc::UnboundedSender;
    use geoengine_datatypes::{
        primitives::{DateTime, SpatialPartition2D, SpatialResolution, TimeInterval},
        util::{arrow::arrow_ipc_file_to_record_batches, test::TestDefault},
    };

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

        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

        let (workflow, _workflow_id) = register_ndvi_workflow_helper(&app_ctx).await;

        let query_rectangle = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into())
                .unwrap(),
            time_interval: TimeInterval::new_instant(DateTime::new_utc(2014, 3, 1, 0, 0, 0))
                .unwrap(),
            spatial_resolution: SpatialResolution::one(),
        };

        let handler = RasterWebsocketStreamHandler::new::<InMemorySessionContext>(
            workflow.operator.get_raster().unwrap(),
            query_rectangle,
            ctx.execution_context().unwrap(),
            ctx.query_context().unwrap(),
        )
        .await
        .unwrap();

        let (input_sender, input_receiver) = futures::channel::mpsc::unbounded();

        let mut websocket_context = WebsocketContext::create(handler, input_receiver);

        // 4 tiles
        for _ in 0..4 {
            send_next(&input_sender);

            let bytes = websocket_context.next().await.unwrap().unwrap();

            let pos = bytes.windows(6).position(|w| w == b"ARROW1").unwrap();
            let bytes = &bytes[pos..]; // the first "pos" bytes are WS op bytes

            let record_batches = arrow_ipc_file_to_record_batches(bytes).unwrap();
            assert_eq!(record_batches.len(), 1);
            let record_batch = record_batches.first().unwrap();
            let schema = record_batch.schema();

            assert_eq!(schema.metadata()["spatialReference"], "EPSG:4326");
        }

        send_next(&input_sender);
        assert_eq!(websocket_context.next().await.unwrap().unwrap().len(), 4); // close frame

        send_next(&input_sender);
        assert!(websocket_context.next().await.is_none());
    }
}
