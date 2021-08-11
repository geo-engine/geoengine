use crate::engine::{
    QueryContext, RasterQueryProcessor, RasterQueryRectangle, VectorQueryProcessor,
    VectorQueryRectangle,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use geoengine_datatypes::raster::RasterTile2D;

/// This `QueryProcessor` allows to rewrite a query. It does not change the data. Results of the children are forwarded.
pub(crate) struct MapQueryProcessor<S, Q> {
    source: S,
    query_fn: Q,
}

impl<S, Q> MapQueryProcessor<S, Q> {
    pub fn new(source: S, query_fn: Q) -> Self {
        Self { source, query_fn }
    }
}

#[async_trait]
impl<S, Q> RasterQueryProcessor for MapQueryProcessor<S, Q>
where
    S: RasterQueryProcessor,
    Q: Fn(RasterQueryRectangle) -> Result<RasterQueryRectangle> + Sync + Send,
{
    type RasterType = S::RasterType;
    async fn raster_query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<S::RasterType>>>> {
        let rewritten_query = (self.query_fn)(query)?;
        self.source.raster_query(rewritten_query, ctx).await
    }
}

#[async_trait]
impl<S, Q> VectorQueryProcessor for MapQueryProcessor<S, Q>
where
    S: VectorQueryProcessor,
    Q: Fn(VectorQueryRectangle) -> Result<VectorQueryRectangle> + Sync + Send,
{
    type VectorType = S::VectorType;
    async fn vector_query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>> {
        let rewritten_query = (self.query_fn)(query)?;
        self.source.vector_query(rewritten_query, ctx).await
    }
}
