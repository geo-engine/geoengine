use crate::engine::{QueryContext, RasterQueryProcessor};
use crate::error;
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::{BoxStream, TryStreamExt};
use futures::{Future, Stream, StreamExt};
use geoengine_datatypes::primitives::{
    BandSelectionIter, CacheHint, RasterQueryRectangle, TimeInterval,
};
use geoengine_datatypes::raster::{
    GridOrEmpty, Pixel, RasterTile2D, TileInformation, TileInformationBandCrossProductIter,
    TilingStrategy,
};
use rayon::ThreadPool;
use std::sync::Arc;

#[async_trait]
pub trait FoldTileAccu {
    type RasterType: Pixel;
    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>>;
    fn thread_pool(&self) -> &Arc<ThreadPool>;
}

pub trait FoldTileAccuMut: FoldTileAccu {
    fn set_time(&mut self, new_time: TimeInterval);
    fn set_cache_hint(&mut self, new_cache_hint: CacheHint);
}

/// Generates tiles by running bounded, ordered sub-query futures.
pub struct RasterSubQueryAdapter<'a, PixelType, RasterProcessorType, SubQuery, TimeStream>
where
    PixelType: Pixel,
    RasterProcessorType: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<'a, PixelType>,
    TimeStream: Stream<Item = Result<TimeInterval>>,
{
    stream: BoxStream<'a, Result<RasterTile2D<PixelType>>>,
    _marker: std::marker::PhantomData<fn() -> (RasterProcessorType, SubQuery, TimeStream)>,
}

impl<'a, PixelType, RasterProcessor, SubQuery, TimeStream>
    RasterSubQueryAdapter<'a, PixelType, RasterProcessor, SubQuery, TimeStream>
where
    PixelType: Pixel,
    RasterProcessor: RasterQueryProcessor<RasterType = PixelType> + 'a,
    SubQuery: SubQueryTileAggregator<'a, PixelType> + Sync + 'a,
    TimeStream: Stream<Item = Result<TimeInterval>> + Send + 'a,
{
    pub fn new(
        source_processor: &'a RasterProcessor,
        query_rect_to_answer: RasterQueryRectangle,
        tiling_strategy: TilingStrategy,
        query_ctx: &'a dyn QueryContext,
        sub_query: SubQuery,
        time_stream: TimeStream,
    ) -> Self {
        let descriptor_query = query_rect_to_answer.clone();
        let descriptor_bands = query_rect_to_answer.attributes().clone();
        let sub_query = Arc::new(sub_query);
        let parallelism = query_ctx.tile_scheduler().parallelism();
        tracing::debug!(parallelism, "raster sub-query fanout configured");
        let descriptors = futures::stream::try_unfold(
            (
                time_stream.boxed(),
                None::<TileInformationBandCrossProductIter>,
                None::<TimeInterval>,
            ),
            move |(mut time_stream, mut band_tile_iter, mut time)| {
                let descriptor_query = descriptor_query.clone();
                let descriptor_bands = descriptor_bands.clone();
                async move {
                    loop {
                        if let Some(iter) = band_tile_iter.as_mut() {
                            if let Some((tile, band)) = iter.next() {
                                return Ok(Some((
                                    (time.expect("time is set"), tile, band),
                                    (time_stream, band_tile_iter, time),
                                )));
                            }
                        }

                        let next_time = time_stream.next().await.transpose()?;
                        let Some(next_time) = next_time else {
                            return Ok(None);
                        };
                        time = Some(next_time);
                        let tile_iter = tiling_strategy
                            .tile_information_iterator_from_pixel_bounds(
                                descriptor_query.spatial_bounds(),
                            );
                        band_tile_iter = Some(TileInformationBandCrossProductIter::new(
                            tile_iter,
                            BandSelectionIter::new(descriptor_bands.clone()),
                        ));
                    }
                }
            },
        )
        .map_ok(move |(time, tile, band)| {
            process_tile(
                source_processor,
                query_ctx,
                sub_query.clone(),
                query_rect_to_answer.clone(),
                time,
                tile,
                band,
            )
        })
        .try_buffered(parallelism)
        .boxed();

        Self {
            stream: query_ctx.abort_registration().wrap(descriptors).boxed(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn box_pin(self) -> BoxStream<'a, Result<RasterTile2D<PixelType>>>
    where
        SubQuery: Send + 'static,
        TimeStream: Send + 'a,
    {
        self.stream
    }
}

impl<'a, PixelType, RasterProcessor, SubQuery, TimeStream> Stream
    for RasterSubQueryAdapter<'a, PixelType, RasterProcessor, SubQuery, TimeStream>
where
    PixelType: Pixel,
    RasterProcessor: RasterQueryProcessor<RasterType = PixelType>,
    SubQuery: SubQueryTileAggregator<'a, PixelType> + Sync + 'a,
    TimeStream: Stream<Item = Result<TimeInterval>> + Send + 'a,
{
    type Item = Result<RasterTile2D<PixelType>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().stream.poll_next_unpin(cx)
    }
}

async fn process_tile<'a, P, R, S>(
    source_processor: &'a R,
    query_ctx: &'a dyn QueryContext,
    sub_query: Arc<S>,
    query_rect: RasterQueryRectangle,
    time: TimeInterval,
    tile: TileInformation,
    band: u32,
) -> Result<RasterTile2D<P>>
where
    P: Pixel,
    R: RasterQueryProcessor<RasterType = P>,
    S: SubQueryTileAggregator<'a, P> + Sync,
{
    let Some(raster_query_rect) = sub_query.tile_query_rectangle(tile, query_rect, time, band)?
    else {
        return Ok(RasterTile2D::new_with_tile_info(
            time,
            tile,
            band,
            GridOrEmpty::new_empty_shape(tile.tile_size_in_pixels),
            CacheHint::max_duration(),
        ));
    };

    let (query, accu) = futures::try_join!(
        source_processor.raster_query(raster_query_rect.clone(), query_ctx),
        sub_query.new_fold_accu(tile, raster_query_rect, query_ctx.thread_pool()),
    )?;
    let accu = query.try_fold(accu, sub_query.fold_method()).await?;
    let mut result = accu.into_tile().await?;
    result.band = band;
    Ok(result)
}

pub trait SubQueryTileAggregator<'a, T>: Send + 'a
where
    T: Pixel,
{
    type FoldFuture: Send + futures::TryFuture<Ok = Self::TileAccu, Error = error::Error>;
    type FoldMethod: 'a
        + Send
        + Sync
        + Clone
        + Fn(Self::TileAccu, RasterTile2D<T>) -> Self::FoldFuture;
    type TileAccu: FoldTileAccu<RasterType = T> + Clone + Send;
    type TileAccuFuture: Send + Future<Output = Result<Self::TileAccu>>;

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture;

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        _query_rect: RasterQueryRectangle,
        time: TimeInterval,
        band_idx: u32,
    ) -> Result<Option<RasterQueryRectangle>> {
        Ok(Some(RasterQueryRectangle::new(
            tile_info.global_pixel_bounds(),
            time,
            band_idx.into(),
        )))
    }

    fn fold_method(&self) -> Self::FoldMethod;

    fn into_raster_subquery_adapter<S, G>(
        self,
        source: &'a S,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
        tiling_strategy: TilingStrategy,
        time_stream: G,
    ) -> RasterSubQueryAdapter<'a, T, S, Self, G>
    where
        S: RasterQueryProcessor<RasterType = T>,
        G: Stream<Item = Result<TimeInterval>> + Send + 'a,
        Self: Sized + Sync,
    {
        RasterSubQueryAdapter::new(source, query, tiling_strategy, ctx, self, time_stream)
    }
}
