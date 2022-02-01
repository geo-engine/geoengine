use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use geoengine_datatypes::{
    primitives::{RasterQueryRectangle, SpatialPartition2D},
    raster::{ConvertDataTypeParallel, Pixel, RasterTile2D},
};
use num_traits::AsPrimitive;

use crate::engine::{BoxRasterQueryProcessor, QueryContext, QueryProcessor};
use crate::util::Result;

pub struct RasterConversionQueryProcessor<PIn: Pixel, POut: Pixel> {
    query_processor: BoxRasterQueryProcessor<PIn>,
    _p_out: std::marker::PhantomData<POut>,
}

impl<PIn: Pixel, POut: Pixel> RasterConversionQueryProcessor<PIn, POut> {
    pub fn new(query_processor: BoxRasterQueryProcessor<PIn>) -> Self {
        Self {
            query_processor,
            _p_out: std::marker::PhantomData,
        }
    }
}

async fn process_tile<PIn: Pixel + AsPrimitive<POut>, POut: Pixel>(
    tile: RasterTile2D<PIn>,
    pool: Arc<rayon::ThreadPool>,
) -> Result<RasterTile2D<POut>> {
    tokio::task::spawn_blocking(move || pool.install(|| tile.convert_data_type_parallel()))
        .await
        .map_err(|e| e.into())
}

#[async_trait]
impl<'a, PIn: Pixel, POut: Pixel> QueryProcessor for RasterConversionQueryProcessor<PIn, POut>
where
    PIn: AsPrimitive<POut>,
{
    type Output = RasterTile2D<POut>;
    type SpatialBounds = SpatialPartition2D;

    async fn query<'b>(
        &'b self,
        query: RasterQueryRectangle,
        ctx: &'b dyn QueryContext,
    ) -> Result<BoxStream<'b, Result<Self::Output>>> {
        let stream = self.query_processor.query(query, ctx).await?;
        let converted_stream =
            stream.and_then(move |tile| process_tile(tile, ctx.thread_pool().clone()));

        Ok(converted_stream.boxed())
    }
}
