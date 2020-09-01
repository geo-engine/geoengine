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
// TODO: implementation is done with `paste!`, but we can use `core::concat_idents` once its stable

macro_rules! impl_mock_feature_collection_source {
    ($geometry:ty, $output:ident) => {
        paste::paste! {
            impl_mock_feature_collection_source!(
                $geometry,
                $output,
                [<MockFeatureCollectionSource$geometry>]
            );
        }
    };

    ($geometry:ty, $output:ident, $newtype:ident) => {
        type $newtype = MockFeatureCollectionSource<$geometry>;

        #[typetag::serde]
        impl VectorOperator for $newtype {
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
                            data_type: <$geometry>::DATA_TYPE,
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
            params: MockFeatureCollectionSourceParams { collection },
        }
        .boxed();

        let serialized = serde_json::to_string(&source).unwrap();
        let collection_bytes = [
            65, 82, 82, 79, 87, 49, 0, 0, 144, 1, 0, 0, 16, 0, 0, 0, 0, 0, 10, 0, 14, 0, 12, 0, 11,
            0, 4, 0, 10, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 1, 3, 0, 10, 0, 12, 0, 0, 0, 8, 0, 4, 0,
            10, 0, 0, 0, 8, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 180, 0, 0, 0, 72, 0, 0, 0,
            20, 0, 0, 0, 16, 0, 20, 0, 16, 0, 14, 0, 15, 0, 4, 0, 0, 0, 8, 0, 16, 0, 0, 0, 16, 0,
            0, 0, 24, 0, 0, 0, 0, 0, 1, 2, 20, 0, 0, 0, 176, 255, 255, 255, 64, 0, 0, 0, 0, 0, 0,
            1, 0, 0, 0, 0, 6, 0, 0, 0, 102, 111, 111, 98, 97, 114, 0, 0, 168, 255, 255, 255, 24, 0,
            0, 0, 12, 0, 0, 0, 0, 0, 0, 16, 60, 0, 0, 0, 1, 0, 0, 0, 12, 0, 0, 0, 102, 255, 255,
            255, 2, 0, 0, 0, 152, 255, 255, 255, 0, 0, 0, 2, 16, 0, 0, 0, 24, 0, 0, 0, 8, 0, 12, 0,
            4, 0, 11, 0, 8, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 6, 0, 0, 0, 95, 95, 116,
            105, 109, 101, 0, 0, 16, 0, 20, 0, 16, 0, 0, 0, 15, 0, 4, 0, 0, 0, 8, 0, 16, 0, 0, 0,
            28, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 12, 128, 0, 0, 0, 1, 0, 0, 0, 28, 0, 0, 0, 4, 0, 4,
            0, 4, 0, 0, 0, 16, 0, 16, 0, 0, 0, 0, 0, 7, 0, 8, 0, 0, 0, 12, 0, 16, 0, 0, 0, 0, 0, 0,
            16, 24, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 36, 0, 0, 0, 0, 0, 6, 0, 8, 0, 4, 0, 6, 0, 0,
            0, 2, 0, 0, 0, 16, 0, 18, 0, 0, 0, 0, 0, 7, 0, 8, 0, 0, 0, 12, 0, 16, 0, 0, 0, 0, 0, 0,
            3, 16, 0, 0, 0, 20, 0, 0, 0, 0, 0, 6, 0, 8, 0, 6, 0, 6, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0,
            0, 10, 0, 0, 0, 95, 95, 103, 101, 111, 109, 101, 116, 114, 121, 0, 0, 16, 0, 0, 0, 12,
            0, 26, 0, 24, 0, 23, 0, 4, 0, 8, 0, 12, 0, 0, 0, 32, 0, 0, 0, 184, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 3, 3, 0, 10, 0, 24, 0, 12, 0, 8, 0, 4, 0, 10, 0, 0, 0, 124, 0, 0,
            0, 16, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0,
            0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0,
            32, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 40, 0, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0,
            0, 0, 0, 0, 88, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 96, 0, 0, 0, 0, 0, 0, 0,
            8, 0, 0, 0, 0, 0, 0, 0, 104, 0, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0, 0, 0, 0, 0, 152, 0, 0,
            0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 160, 0, 0, 0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0,
            0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 7, 0, 0, 0,
            0, 0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 154, 153, 153, 153, 153,
            153, 185, 63, 0, 0, 0, 0, 0, 0, 240, 63, 154, 153, 153, 153, 153, 153, 241, 63, 0, 0,
            0, 0, 0, 0, 0, 64, 205, 204, 204, 204, 204, 204, 8, 64, 7, 0, 0, 0, 0, 0, 0, 0, 255, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 16,
            0, 0, 0, 12, 0, 18, 0, 16, 0, 12, 0, 8, 0, 4, 0, 12, 0, 0, 0, 128, 1, 0, 0, 156, 1, 0,
            0, 16, 0, 0, 0, 3, 0, 10, 0, 12, 0, 0, 0, 8, 0, 4, 0, 10, 0, 0, 0, 8, 0, 0, 0, 8, 0, 0,
            0, 0, 0, 0, 0, 3, 0, 0, 0, 180, 0, 0, 0, 72, 0, 0, 0, 20, 0, 0, 0, 16, 0, 20, 0, 16, 0,
            14, 0, 15, 0, 4, 0, 0, 0, 8, 0, 16, 0, 0, 0, 16, 0, 0, 0, 24, 0, 0, 0, 0, 0, 1, 2, 20,
            0, 0, 0, 176, 255, 255, 255, 64, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 6, 0, 0, 0, 102, 111,
            111, 98, 97, 114, 0, 0, 168, 255, 255, 255, 24, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 16, 60,
            0, 0, 0, 1, 0, 0, 0, 12, 0, 0, 0, 102, 255, 255, 255, 2, 0, 0, 0, 152, 255, 255, 255,
            0, 0, 0, 2, 16, 0, 0, 0, 24, 0, 0, 0, 8, 0, 12, 0, 4, 0, 11, 0, 8, 0, 0, 0, 64, 0, 0,
            0, 0, 0, 0, 1, 0, 0, 0, 0, 6, 0, 0, 0, 95, 95, 116, 105, 109, 101, 0, 0, 16, 0, 20, 0,
            16, 0, 0, 0, 15, 0, 4, 0, 0, 0, 8, 0, 16, 0, 0, 0, 28, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0,
            12, 128, 0, 0, 0, 1, 0, 0, 0, 28, 0, 0, 0, 4, 0, 4, 0, 4, 0, 0, 0, 16, 0, 16, 0, 0, 0,
            0, 0, 7, 0, 8, 0, 0, 0, 12, 0, 16, 0, 0, 0, 0, 0, 0, 16, 24, 0, 0, 0, 4, 0, 0, 0, 1, 0,
            0, 0, 36, 0, 0, 0, 0, 0, 6, 0, 8, 0, 4, 0, 6, 0, 0, 0, 2, 0, 0, 0, 16, 0, 18, 0, 0, 0,
            0, 0, 7, 0, 8, 0, 0, 0, 12, 0, 16, 0, 0, 0, 0, 0, 0, 3, 16, 0, 0, 0, 20, 0, 0, 0, 0, 0,
            6, 0, 8, 0, 6, 0, 6, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 10, 0, 0, 0, 95, 95, 103, 101,
            111, 109, 101, 116, 114, 121, 0, 0, 1, 0, 0, 0, 152, 1, 0, 0, 0, 0, 0, 0, 92, 1, 0, 0,
            0, 0, 0, 0, 184, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 184, 1, 0, 0, 65, 82, 82,
            79, 87, 49,
        ]
        .to_vec();
        assert_eq!(
            serialized,
            serde_json::json!({
                "type": "MockFeatureCollectionSourceMultiPoint",
                "params": {
                    "collection": {
                        "table": collection_bytes,
                        "types": {
                            "foobar": "NullableDecimal"
                        },
                    }
                }
            })
            .to_string()
        );

        let _: Box<dyn VectorOperator> = serde_json::from_str(&serialized).unwrap();
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
