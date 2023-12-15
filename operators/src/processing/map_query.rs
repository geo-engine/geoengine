use crate::adapters::{FillerTileCacheExpirationStrategy, SparseTilesFillAdapter};
use crate::engine::{
    QueryContext, RasterQueryProcessor, RasterResultDescriber, RasterResultDescriptor,
    VectorQueryProcessor, VectorResultDescriptor,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_datatypes::raster::{RasterTile2D, TilingSpecification};

/// This `QueryProcessor` allows to rewrite a query. It does not change the data. Results of the children are forwarded.
pub(crate) struct MapQueryProcessor<S, Q, A, R> {
    source: S,
    result_descriptor: R,
    query_fn: Q,
    additional_data: A,
}

impl<S, Q, A, R> MapQueryProcessor<S, Q, A, R> {
    pub fn new(source: S, result_descriptor: R, query_fn: Q, additional_data: A) -> Self {
        Self {
            source,
            result_descriptor,
            query_fn,
            additional_data,
        }
    }
}

impl<S, Q> RasterResultDescriber
    for MapQueryProcessor<S, Q, TilingSpecification, RasterResultDescriptor>
{
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

#[async_trait]
impl<S, Q> RasterQueryProcessor
    for MapQueryProcessor<S, Q, TilingSpecification, RasterResultDescriptor>
where
    S: RasterQueryProcessor,
    Q: Fn(RasterQueryRectangle) -> Result<Option<RasterQueryRectangle>> + Sync + Send,
{
    type RasterType = S::RasterType;
    async fn raster_query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<S::RasterType>>>> {
        let rewritten_query = (self.query_fn)(query.clone())?;

        if let Some(rewritten_query) = rewritten_query {
            self.source.raster_query(rewritten_query, ctx).await
        } else {
            log::debug!("Query was rewritten to empty query. Returning empty / filled stream.");
            let s = futures::stream::empty();

            // TODO: The input of the `SparseTilesFillAdapter` is empty here, so we can't derive the expiration, as there are no tiles to derive them from.
            //       As this is the result of the query not being rewritten, we should check if the expiration could also be `max`, because this error
            //       will be persistent and we might as well cache the empty stream.
            Ok(SparseTilesFillAdapter::new_like_subquery(
                s,
                &query,
                self.additional_data,
                FillerTileCacheExpirationStrategy::NoCache,
            )
            .boxed())
        }
    }
}

#[async_trait]
impl<S, Q> VectorQueryProcessor for MapQueryProcessor<S, Q, (), VectorResultDescriptor>
where
    S: VectorQueryProcessor,
    Q: Fn(VectorQueryRectangle) -> Result<Option<VectorQueryRectangle>> + Sync + Send,
    S::VectorType: Send,
{
    type VectorType = S::VectorType;
    async fn vector_query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>> {
        let rewritten_query = (self.query_fn)(query)?;
        if let Some(rewritten_query) = rewritten_query {
            self.source.vector_query(rewritten_query, ctx).await
        } else {
            log::debug!("Query was rewritten to empty query. Returning empty stream.");
            Ok(Box::pin(futures::stream::empty())) // TODO: should be empty collection?
        }
    }
}
