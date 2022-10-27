use crate::adapters::SparseTilesFillAdapter;
use crate::engine::{QueryContext, RasterQueryProcessor, VectorQueryProcessor};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_datatypes::raster::{RasterTile2D, TilingSpecification};

/// This `QueryProcessor` allows to rewrite a query. It does not change the data. Results of the children are forwarded.
pub(crate) struct MapQueryProcessor<S, Q, A> {
    source: S,
    query_fn: Q,
    additional_data: A,
}

impl<S, Q, A> MapQueryProcessor<S, Q, A> {
    pub fn new(source: S, query_fn: Q, additional_data: A) -> Self {
        Self {
            source,
            query_fn,
            additional_data,
        }
    }
}

#[async_trait]
impl<S, Q> RasterQueryProcessor for MapQueryProcessor<S, Q, TilingSpecification>
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
        let rewritten_query = (self.query_fn)(query)?;

        if let Some(rewritten_query) = rewritten_query {
            self.source.raster_query(rewritten_query, ctx).await
        } else {
            log::debug!("Query was rewritten to empty query. Returning empty / filled stream.");
            let s = futures::stream::empty();

            Ok(SparseTilesFillAdapter::new_like_subquery(s, query, self.additional_data).boxed())
        }
    }
}

#[async_trait]
impl<S, Q> VectorQueryProcessor for MapQueryProcessor<S, Q, ()>
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
