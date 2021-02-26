use crate::{engine::QueryProcessor, util::Result};

use crate::engine::{QueryContext, QueryRectangle};

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

impl<S, Q> QueryProcessor for MapQueryProcessor<S, Q>
where
    S: QueryProcessor,
    Q: Fn(QueryRectangle) -> QueryRectangle + Sync + Send,
{
    type Output = S::Output;

    fn query<'a>(
        &'a self,
        query: QueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> futures::stream::BoxStream<'a, Result<Self::Output>> {
        let rewritten_query = (self.query_fn)(query); // TODO: error checking
        self.source.query(rewritten_query, ctx)
    }
}
