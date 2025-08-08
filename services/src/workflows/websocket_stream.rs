use crate::{contexts::SessionContext, error::Result};
use actix_http::ws::{CloseCode, CloseReason};
use actix_ws::Item;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt, stream::BoxStream};
use geoengine_datatypes::{
    collections::FeatureCollectionIpc,
    primitives::{RasterQueryRectangle, VectorQueryRectangle},
    raster::raster_tile_2d_to_arrow_ipc_file,
};
use geoengine_operators::{
    call_on_generic_raster_processor, call_on_generic_vector_processor,
    engine::{
        InitializedRasterOperator, QueryAbortTrigger, QueryContext, QueryProcessorExt,
        RasterOperator, VectorOperator, WorkflowOperatorPath,
    },
};
use tokio::{
    sync::mpsc::{Receiver, Sender, error::TrySendError},
    task::JoinHandle,
};

type ByteStream = BoxStream<'static, StreamResult>;
type StreamResult = Result<Vec<u8>>;

pub struct WebsocketStreamTask {
    _handle: JoinHandle<()>,
    compute_sender: Sender<()>,
    tile_receiver: Receiver<Option<Result<Vec<u8>>>>,
    abort_handle: QueryAbortTrigger,
}

impl WebsocketStreamTask {
    pub async fn new_raster_initialized<C: QueryContext + 'static>(
        initialized_operator: Box<dyn InitializedRasterOperator>,
        query_rectangle: RasterQueryRectangle,
        mut query_ctx: C,
    ) -> Result<Self> {
        let spatial_reference = initialized_operator.result_descriptor().spatial_reference;

        let query_processor = initialized_operator.query_processor()?;

        let abort_handle = query_ctx.abort_trigger()?;

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

        Ok(Self::new_from_byte_stream(
            byte_stream.map_err(Into::into).boxed(),
            abort_handle,
        ))
    }

    pub async fn new_raster<C: SessionContext>(
        raster_operator: Box<dyn RasterOperator>,
        query_rectangle: RasterQueryRectangle,
        execution_ctx: C::ExecutionContext,
        query_ctx: C::QueryContext,
    ) -> Result<Self> {
        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized_operator = raster_operator
            .initialize(workflow_operator_path_root, &execution_ctx)
            .await?;

        Self::new_raster_initialized(initialized_operator, query_rectangle, query_ctx).await
    }

    pub async fn new_vector<C: SessionContext>(
        vector_operator: Box<dyn VectorOperator>,
        query_rectangle: VectorQueryRectangle,
        execution_ctx: C::ExecutionContext,
        mut query_ctx: C::QueryContext,
    ) -> Result<Self> {
        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized_operator = vector_operator
            .initialize(workflow_operator_path_root, &execution_ctx)
            .await?;

        let spatial_reference = initialized_operator.result_descriptor().spatial_reference;

        let query_processor = initialized_operator.query_processor()?;

        let abort_handle = query_ctx.abort_trigger()?;

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

        Ok(Self::new_from_byte_stream(
            byte_stream.map_err(Into::into).boxed(),
            abort_handle,
        ))
    }

    fn new_from_byte_stream(mut byte_stream: ByteStream, abort_handle: QueryAbortTrigger) -> Self {
        let (compute_sender, mut compute_msgs) = tokio::sync::mpsc::channel(1);
        let (tile_sender, tile_msgs) = tokio::sync::mpsc::channel(1);

        let handle = crate::util::spawn(async move {
            while compute_msgs.recv().await.is_some() {
                let tile = byte_stream.next().await;
                tile_sender.send(tile).await.unwrap_or_else(|e| {
                    tracing::error!("Failed to send tile: {}", e);
                });
            }
        });

        Self {
            _handle: handle,
            compute_sender,
            tile_receiver: tile_msgs,
            abort_handle,
        }
    }

    pub fn abort_processing(self) {
        self.abort_handle.abort();
    }

    pub fn compute_next_tile(&mut self) {
        match self.compute_sender.try_send(()) {
            Ok(()) | Err(TrySendError::Full(())) => { /* ok or drop message */ }
            Err(TrySendError::Closed(())) => {
                debug_assert!(
                    false, // must not happen
                    "Compute sender is closed, cannot send next tile request"
                );
            }
        }
    }

    pub async fn receive_tile(&mut self) -> Option<Result<Vec<u8>>> {
        self.tile_receiver.recv().await?
    }
}

/// Sends the result of a stream to the websocket session.
///
/// Return `Some` if the tile was sent successfully, `None` if the stream ended,
///
pub async fn send_websocket_message(
    tile: Option<StreamResult>,
    mut ctx: actix_ws::Session,
) -> Option<()> {
    match tile {
        Some(Ok(bytes)) => {
            const MESSAGE_MAX_SIZE: usize = 16 * 1024 * 1024; // 16 MB

            // we can send the whole message at once if it is small enough, i.e. <= `MESSAGE_MAX_SIZE`
            if bytes.len() <= MESSAGE_MAX_SIZE {
                ctx.binary(bytes).await.ok()?;
                return Some(());
            }

            // limit message chunks to `MESSAGE_MAX_SIZE`
            let mut chunks = bytes.chunks(MESSAGE_MAX_SIZE).enumerate();

            while let Some((i, chunk)) = chunks.next() {
                let chunk_bytes = chunk.to_vec().into();
                if i == 0 {
                    // first chunk
                    ctx.continuation(Item::FirstBinary(chunk_bytes))
                        .await
                        .ok()?;
                } else if chunks.len() == 0 {
                    // last chunk
                    ctx.continuation(Item::Last(chunk_bytes)).await.ok()?;
                } else {
                    ctx.continuation(Item::Continue(chunk_bytes)).await.ok()?;
                }
            }
        }
        Some(Err(e)) => {
            // on error, send the error and close the connection
            ctx.close(Some(CloseReason {
                code: CloseCode::Error,
                description: Some(e.to_string()),
            }))
            .await
            .ok()?;
            return None;
        }
        None => {
            // stream ended
            ctx.close(Some(CloseReason {
                code: CloseCode::Normal,
                description: None,
            }))
            .await
            .ok()?;
            return None;
        }
    }

    Some(())
}

pub async fn handle_websocket_message(
    msg: actix_ws::Message,
    stream_task: &mut WebsocketStreamTask,
    session: &mut actix_ws::Session,
) -> Option<()> {
    match msg {
        actix_ws::Message::Ping(bytes) => {
            session.pong(&bytes).await.ok()?;
        }
        actix_ws::Message::Text(msg) if &msg == "NEXT" => {
            stream_task.compute_next_tile();
        }
        actix_ws::Message::Close(_) => {
            return None; // close the session
        }
        _ => { /* ignore other messages */ }
    }

    Some(())
}
