use crate::engine::{QueryContext, VectorQueryProcessor, VectorResultDescriptor};
use crate::util::Result;
use async_trait::async_trait;

use futures::stream::BoxStream;
use geoengine_datatypes::primitives::VectorQueryRectangle;

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
impl<S, Q> VectorQueryProcessor for MapQueryProcessor<S, Q>
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
            tracing::debug!("Query was rewritten to empty query. Returning empty stream.");
            Ok(Box::pin(futures::stream::empty())) // TODO: should be empty collection?
        }
    }

    fn vector_result_descriptor(&self) -> &VectorResultDescriptor {
        self.source.vector_result_descriptor()
    }
}
