use crate::engine::{
    ExecutionContext, InitializedOperator, InitializedOperatorImpl, InitializedVectorOperator,
    SourceOperator, TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor,
    VectorResultDescriptor,
};
use crate::engine::{QueryContext, QueryProcessor, QueryRectangle};
use crate::util::Result;
use futures::stream::{self, BoxStream, StreamExt};
use geoengine_datatypes::collections::FeatureCollection;
use geoengine_datatypes::primitives::{
    Geometry, MultiLineString, MultiPoint, MultiPolygon, NoGeometry,
};
use geoengine_datatypes::projection::Projection;
use geoengine_datatypes::util::arrow::ArrowTyped;
use serde::{Deserialize, Serialize};

pub struct MockFeatureCollectionSourceProcessor<G>
where
    G: Geometry + ArrowTyped,
{
    collection: FeatureCollection<G>,
}

impl<G> QueryProcessor for MockFeatureCollectionSourceProcessor<G>
where
    G: Geometry + ArrowTyped + Send + Sync,
{
    type Output = FeatureCollection<G>;

    fn query(&self, _query: QueryRectangle, _ctx: QueryContext) -> BoxStream<Result<Self::Output>> {
        // TODO: chunk it up
        // let chunk_size = ctx.chunk_byte_size / std::mem::size_of::<Coordinate2D>();

        let collection = self.collection.clone();
        stream::once(async move { Ok(collection) }).boxed()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MockFeatureCollectionSourceParams<G>
where
    G: Geometry + ArrowTyped,
{
    pub collection: FeatureCollection<G>,
}

pub type MockFeatureCollectionSource<G> = SourceOperator<MockFeatureCollectionSourceParams<G>>;

// TODO: use single implementation once
//      "deserialization of generic impls is not supported yet; use #[typetag::serialize] to generate serialization only"
//  is solved

macro_rules! impl_mock_feature_collection_source {
    ($geometry:ident, $output:ident) => {
        #[typetag::serde]
        impl VectorOperator for MockFeatureCollectionSource<$geometry> {
            fn initialize(
                self: Box<Self>,
                context: ExecutionContext,
            ) -> Result<Box<InitializedVectorOperator>> {
                InitializedOperatorImpl::create(
                    self.params,
                    context,
                    |_, _, _, _| Ok(()),
                    |_, _, _, _, _| {
                        Ok(VectorResultDescriptor {
                            data_type: $geometry::DATA_TYPE,
                            projection: Projection::wgs84().into(), // TODO: get from `FeatureCollection`
                        })
                    },
                    vec![],
                    vec![],
                )
                .map(InitializedOperatorImpl::boxed)
            }
        }

        impl InitializedOperator<VectorResultDescriptor, TypedVectorQueryProcessor>
            for InitializedOperatorImpl<
                MockFeatureCollectionSourceParams<$geometry>,
                VectorResultDescriptor,
                (),
            >
        {
            fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
                Ok(TypedVectorQueryProcessor::$output(
                    MockFeatureCollectionSourceProcessor {
                        collection: self.params.collection.clone(),
                    }
                    .boxed(),
                ))
            }
        }
    };
}

impl_mock_feature_collection_source!(NoGeometry, Data);
impl_mock_feature_collection_source!(MultiPoint, MultiPoint);
impl_mock_feature_collection_source!(MultiLineString, MultiLineString);
impl_mock_feature_collection_source!(MultiPolygon, MultiPolygon);

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on_stream;
    use geoengine_datatypes::collections::MultiPointCollection;
    use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D, FeatureData, TimeInterval};

    #[test]
    fn serde() {
        // TODO: implement
        //
        // let points = vec![Coordinate2D::new(1., 2.); 3];
        //
        // let mps = MockFeatureCollectionSource {
        //     params: MockFeatureCollectionSourceParams { points },
        // }
        // .boxed();
        // let serialized = serde_json::to_string(&mps).unwrap();
        // let expect = "{\"type\":\"MockPointSource\",\"params\":{\"points\":[{\"x\":1.0,\"y\":2.0},{\"x\":1.0,\"y\":2.0},{\"x\":1.0,\"y\":2.0}]}}";
        // assert_eq!(serialized, expect);
        //
        // let _: Box<dyn VectorOperator> = serde_json::from_str(&serialized).unwrap();
    }

    #[test]
    fn execute() {
        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 3.1)]).unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            [(
                "foobar".to_string(),
                FeatureData::NullableDecimal(vec![Some(0), None, Some(2)]),
            )]
            .iter()
            .cloned()
            .collect(),
        )
        .unwrap();

        let source = MockFeatureCollectionSource {
            params: MockFeatureCollectionSourceParams {
                collection: collection.clone(),
            },
        }
        .boxed();

        let source = source.initialize(ExecutionContext).unwrap();

        let processor =
            if let Ok(TypedVectorQueryProcessor::MultiPoint(p)) = source.query_processor() {
                p
            } else {
                panic!()
            };

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            time_interval: TimeInterval::default(),
        };
        let ctx = QueryContext {
            chunk_byte_size: 2 * std::mem::size_of::<Coordinate2D>(),
        };

        let stream = processor.vector_query(query_rectangle, ctx);

        let blocking_stream = block_on_stream(stream);

        let collections: Vec<MultiPointCollection> = blocking_stream.map(Result::unwrap).collect();

        assert_eq!(collections.len(), 1);

        assert_eq!(collections[0], collection);
    }
}
