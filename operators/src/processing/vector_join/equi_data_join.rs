use std::marker::PhantomData;

use futures::stream::BoxStream;

use geoengine_datatypes::collections::{DataCollection, FeatureCollection};
use geoengine_datatypes::primitives::Geometry;
use geoengine_datatypes::util::arrow::ArrowTyped;

use crate::adapters::FeatureCollectionChunkMerger;
use crate::engine::{QueryContext, QueryProcessor, QueryRectangle, VectorQueryProcessor};
use crate::util::Result;
use futures::stream;
use futures::StreamExt;

/// Implements a left inner join between a `GeoFeatureCollection` stream and a `DataCollection` stream.
pub struct EquiLeftJoinProcessor<G> {
    vector_type: PhantomData<FeatureCollection<G>>,
    left_processor: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
    right_processor: Box<dyn VectorQueryProcessor<VectorType = DataCollection>>,
    left_column: String,
    right_column: String,
    right_column_prefix: String,
}

impl<G> EquiLeftJoinProcessor<G>
where
    G: Geometry + ArrowTyped + Sync + Send,
{
    pub fn new(
        left_processor: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
        right_processor: Box<dyn VectorQueryProcessor<VectorType = DataCollection>>,
        left_column: String,
        right_column: String,
        right_prefix: String,
    ) -> Self {
        Self {
            vector_type: PhantomData,
            left_processor,
            right_processor,
            left_column,
            right_column,
            right_column_prefix: right_prefix,
        }
    }

    fn join(
        &self,
        left: &FeatureCollection<G>,
        right: &DataCollection,
    ) -> Result<FeatureCollection<G>> {
        let _left = left;
        let _left_column = &self.left_column;
        let _right = right;
        let _right_column = &self.right_column;
        let _right_column_prefix = &self.right_column_prefix;
        todo!("implement")
    }
}

impl<G> VectorQueryProcessor for EquiLeftJoinProcessor<G>
where
    G: Geometry + ArrowTyped + Sync + Send + 'static,
{
    type VectorType = FeatureCollection<G>;

    fn vector_query(
        &self,
        query: QueryRectangle,
        ctx: QueryContext,
    ) -> BoxStream<'_, Result<Self::VectorType>> {
        let result_stream =
            self.left_processor
                .query(query, ctx)
                .flat_map(move |left_collection| {
                    // This implementation is a nested-loop join

                    let left_collection = match left_collection {
                        Ok(collection) => collection,
                        Err(e) => return stream::once(async { Err(e) }).boxed(),
                    };

                    let data_query = self.right_processor.query(query, ctx);

                    data_query
                        .map(move |right_collection| {
                            self.join(&left_collection, &right_collection?)
                        })
                        .boxed()
                });

        FeatureCollectionChunkMerger::new(result_stream.fuse(), ctx.chunk_byte_size).boxed()
    }
}
