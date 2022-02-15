use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt, TryFutureExt, TryStreamExt};
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
        let converted_stream = stream.and_then(move |tile| {
            crate::util::spawn_blocking_with_thread_pool(ctx.thread_pool().clone(), || {
                tile.convert_data_type_parallel()
            })
            .map_err(Into::into)
        });

        Ok(converted_stream.boxed())
    }
}
