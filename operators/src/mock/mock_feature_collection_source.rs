use crate::engine::{
    ExecutionContext, InitializedOperator, InitializedVectorOperator, ResultDescriptor,
    SourceOperator, TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor,
    VectorResultDescriptor,
};
use crate::engine::{QueryContext, QueryProcessor, QueryRectangle};
use crate::util::Result;
use futures::stream::{self, BoxStream, StreamExt};
use geoengine_datatypes::collections::{FeatureCollection, FeatureCollectionInfos};
use geoengine_datatypes::primitives::{
    Geometry, MultiLineString, MultiPoint, MultiPolygon, NoGeometry,
};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_datatypes::util::arrow::ArrowTyped;
use serde::{Deserialize, Serialize};

pub struct MockFeatureCollectionSourceProcessor<G>
where
    G: Geometry + ArrowTyped,
{
    collections: Vec<FeatureCollection<G>>,
}

impl<G> QueryProcessor for MockFeatureCollectionSourceProcessor<G>
where
    G: Geometry + ArrowTyped + Send + Sync + 'static,
{
    type Output = FeatureCollection<G>;

    fn query<'a>(
        &'a self,
        _query: QueryRectangle,
        _ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        // TODO: chunk it up
        // let chunk_size = ctx.chunk_byte_size / std::mem::size_of::<Coordinate2D>();

        Ok(stream::iter(self.collections.iter().map(|c| Ok(c.clone()))).boxed())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MockFeatureCollectionSourceParams<G>
where
    G: Geometry + ArrowTyped,
{
    pub collections: Vec<FeatureCollection<G>>,
}

pub type MockFeatureCollectionSource<G> = SourceOperator<MockFeatureCollectionSourceParams<G>>;

impl<G> MockFeatureCollectionSource<G>
where
    G: Geometry + ArrowTyped,
{
    pub fn single(collection: FeatureCollection<G>) -> Self {
        Self::multiple(vec![collection])
    }

    pub fn multiple(collections: Vec<FeatureCollection<G>>) -> Self {
        Self {
            params: MockFeatureCollectionSourceParams { collections },
        }
    }
}

pub struct InitializedMockFeatureCollectionSource<R: ResultDescriptor, G: Geometry> {
    result_descriptor: R,
    collections: Vec<FeatureCollection<G>>,
}

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
                _context: &dyn ExecutionContext,
            ) -> Result<Box<InitializedVectorOperator>> {
                let result_descriptor = VectorResultDescriptor {
                    data_type: <$geometry>::DATA_TYPE,
                    spatial_reference: SpatialReference::epsg_4326().into(), // TODO: get from `FeatureCollection`
                    columns: self.params.collections[0].column_types(),
                };

                Ok(InitializedMockFeatureCollectionSource {
                    result_descriptor,
                    collections: self.params.collections,
                }
                .boxed())
            }
        }

        impl InitializedOperator<VectorResultDescriptor, TypedVectorQueryProcessor>
            for InitializedMockFeatureCollectionSource<VectorResultDescriptor, $geometry>
        {
            fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
                Ok(TypedVectorQueryProcessor::$output(
                    MockFeatureCollectionSourceProcessor {
                        collections: self.collections.clone(),
                    }
                    .boxed(),
                ))
            }
            fn result_descriptor(&self) -> &VectorResultDescriptor {
                &self.result_descriptor
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
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use futures::executor::block_on_stream;
    use geoengine_datatypes::primitives::{BoundingBox2D, Coordinate2D, FeatureData, TimeInterval};
    use geoengine_datatypes::{collections::MultiPointCollection, primitives::SpatialResolution};

    #[test]
    fn serde() {
        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 3.1)]).unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            [(
                "foobar".to_string(),
                FeatureData::NullableInt(vec![Some(0), None, Some(2)]),
            )]
            .iter()
            .cloned()
            .collect(),
        )
        .unwrap();

        let source = MockFeatureCollectionSource::single(collection).boxed();

        let serialized = serde_json::to_string(&source).unwrap();
        let collection_bytes = [
            65, 82, 82, 79, 87, 49, 0, 0, 255, 255, 255, 255, 176, 1, 0, 0, 16, 0, 0, 0, 0, 0, 10,
            0, 14, 0, 12, 0, 11, 0, 4, 0, 10, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 1, 4, 0, 10, 0, 12, 0,
            0, 0, 8, 0, 4, 0, 10, 0, 0, 0, 8, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 180, 0,
            0, 0, 56, 0, 0, 0, 4, 0, 0, 0, 52, 255, 255, 255, 16, 0, 0, 0, 24, 0, 0, 0, 0, 0, 1, 2,
            20, 0, 0, 0, 172, 255, 255, 255, 64, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 6, 0, 0, 0, 102,
            111, 111, 98, 97, 114, 0, 0, 152, 255, 255, 255, 24, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 16,
            76, 0, 0, 0, 1, 0, 0, 0, 12, 0, 0, 0, 82, 255, 255, 255, 2, 0, 0, 0, 136, 255, 255,
            255, 24, 0, 0, 0, 32, 0, 0, 0, 0, 0, 1, 2, 28, 0, 0, 0, 8, 0, 12, 0, 4, 0, 11, 0, 8, 0,
            0, 0, 64, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 4, 0, 0, 0, 105, 116, 101, 109, 0, 0, 0, 0,
            6, 0, 0, 0, 95, 95, 116, 105, 109, 101, 0, 0, 16, 0, 20, 0, 16, 0, 0, 0, 15, 0, 4, 0,
            0, 0, 8, 0, 16, 0, 0, 0, 28, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 12, 160, 0, 0, 0, 1, 0, 0,
            0, 28, 0, 0, 0, 4, 0, 4, 0, 4, 0, 0, 0, 16, 0, 20, 0, 16, 0, 14, 0, 15, 0, 4, 0, 0, 0,
            8, 0, 16, 0, 0, 0, 32, 0, 0, 0, 12, 0, 0, 0, 0, 0, 1, 16, 96, 0, 0, 0, 1, 0, 0, 0, 36,
            0, 0, 0, 0, 0, 6, 0, 8, 0, 4, 0, 6, 0, 0, 0, 2, 0, 0, 0, 16, 0, 22, 0, 16, 0, 14, 0,
            15, 0, 4, 0, 0, 0, 8, 0, 16, 0, 0, 0, 24, 0, 0, 0, 28, 0, 0, 0, 0, 0, 1, 3, 24, 0, 0,
            0, 0, 0, 6, 0, 8, 0, 6, 0, 6, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 4, 0, 0, 0, 105, 116,
            101, 109, 0, 0, 0, 0, 4, 0, 0, 0, 105, 116, 101, 109, 0, 0, 0, 0, 10, 0, 0, 0, 95, 95,
            103, 101, 111, 109, 101, 116, 114, 121, 0, 0, 255, 255, 255, 255, 80, 1, 0, 0, 16, 0,
            0, 0, 12, 0, 26, 0, 24, 0, 23, 0, 4, 0, 8, 0, 12, 0, 0, 0, 32, 0, 0, 0, 184, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 4, 0, 10, 0, 20, 0, 12, 0, 8, 0, 4, 0, 10, 0, 0, 0,
            116, 0, 0, 0, 12, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
            0, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0,
            16, 0, 0, 0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 0,
            0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 40, 0, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0, 0, 0, 0, 0,
            88, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 96, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0,
            0, 0, 0, 0, 104, 0, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0, 0, 0, 0, 0, 152, 0, 0, 0, 0, 0, 0,
            0, 8, 0, 0, 0, 0, 0, 0, 0, 160, 0, 0, 0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0,
            255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 154, 153, 153, 153, 153, 153, 185,
            63, 0, 0, 0, 0, 0, 0, 240, 63, 154, 153, 153, 153, 153, 153, 241, 63, 0, 0, 0, 0, 0, 0,
            0, 64, 205, 204, 204, 204, 204, 204, 8, 64, 7, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255,
            255, 0, 0, 0, 0, 16, 0, 0, 0, 12, 0, 18, 0, 16, 0, 12, 0, 8, 0, 4, 0, 12, 0, 0, 0, 160,
            1, 0, 0, 184, 1, 0, 0, 16, 0, 0, 0, 4, 0, 10, 0, 12, 0, 0, 0, 8, 0, 4, 0, 10, 0, 0, 0,
            8, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 180, 0, 0, 0, 56, 0, 0, 0, 4, 0, 0, 0,
            52, 255, 255, 255, 16, 0, 0, 0, 24, 0, 0, 0, 0, 0, 1, 2, 20, 0, 0, 0, 172, 255, 255,
            255, 64, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 6, 0, 0, 0, 102, 111, 111, 98, 97, 114, 0, 0,
            152, 255, 255, 255, 24, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 16, 76, 0, 0, 0, 1, 0, 0, 0, 12,
            0, 0, 0, 82, 255, 255, 255, 2, 0, 0, 0, 136, 255, 255, 255, 24, 0, 0, 0, 32, 0, 0, 0,
            0, 0, 1, 2, 28, 0, 0, 0, 8, 0, 12, 0, 4, 0, 11, 0, 8, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 1,
            0, 0, 0, 0, 4, 0, 0, 0, 105, 116, 101, 109, 0, 0, 0, 0, 6, 0, 0, 0, 95, 95, 116, 105,
            109, 101, 0, 0, 16, 0, 20, 0, 16, 0, 0, 0, 15, 0, 4, 0, 0, 0, 8, 0, 16, 0, 0, 0, 28, 0,
            0, 0, 12, 0, 0, 0, 0, 0, 0, 12, 160, 0, 0, 0, 1, 0, 0, 0, 28, 0, 0, 0, 4, 0, 4, 0, 4,
            0, 0, 0, 16, 0, 20, 0, 16, 0, 14, 0, 15, 0, 4, 0, 0, 0, 8, 0, 16, 0, 0, 0, 32, 0, 0, 0,
            12, 0, 0, 0, 0, 0, 1, 16, 96, 0, 0, 0, 1, 0, 0, 0, 36, 0, 0, 0, 0, 0, 6, 0, 8, 0, 4, 0,
            6, 0, 0, 0, 2, 0, 0, 0, 16, 0, 22, 0, 16, 0, 14, 0, 15, 0, 4, 0, 0, 0, 8, 0, 16, 0, 0,
            0, 24, 0, 0, 0, 28, 0, 0, 0, 0, 0, 1, 3, 24, 0, 0, 0, 0, 0, 6, 0, 8, 0, 6, 0, 6, 0, 0,
            0, 0, 0, 2, 0, 0, 0, 0, 0, 4, 0, 0, 0, 105, 116, 101, 109, 0, 0, 0, 0, 4, 0, 0, 0, 105,
            116, 101, 109, 0, 0, 0, 0, 10, 0, 0, 0, 95, 95, 103, 101, 111, 109, 101, 116, 114, 121,
            0, 0, 1, 0, 0, 0, 192, 1, 0, 0, 0, 0, 0, 0, 88, 1, 0, 0, 0, 0, 0, 0, 184, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 212, 1, 0, 0, 65, 82, 82, 79, 87, 49,
        ]
        .to_vec();
        assert_eq!(
            serialized,
            serde_json::json!({
                "type": "MockFeatureCollectionSourceMultiPoint",
                "params": {
                    "collections": [{
                        "table": collection_bytes,
                        "types": {
                            "foobar": "int"
                        },
                    }]
                }
            })
            .to_string()
        );

        let _operator: Box<dyn VectorOperator> = serde_json::from_str(&serialized).unwrap();
    }

    #[test]
    fn execute() {
        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 3.1)]).unwrap(),
            vec![TimeInterval::new_unchecked(0, 1); 3],
            [(
                "foobar".to_string(),
                FeatureData::NullableInt(vec![Some(0), None, Some(2)]),
            )]
            .iter()
            .cloned()
            .collect(),
        )
        .unwrap();

        let source = MockFeatureCollectionSource::single(collection.clone()).boxed();

        let source = source.initialize(&MockExecutionContext::default()).unwrap();

        let processor =
            if let Ok(TypedVectorQueryProcessor::MultiPoint(p)) = source.query_processor() {
                p
            } else {
                panic!()
            };

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
        };
        let ctx = MockQueryContext::new(2 * std::mem::size_of::<Coordinate2D>());

        let stream = processor.vector_query(query_rectangle, &ctx).unwrap();

        let blocking_stream = block_on_stream(stream);

        let collections: Vec<MultiPointCollection> = blocking_stream.map(Result::unwrap).collect();

        assert_eq!(collections.len(), 1);

        assert_eq!(collections[0], collection);
    }
}
