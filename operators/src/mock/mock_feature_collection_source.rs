use std::collections::HashMap;

use crate::engine::{CanonicOperatorName, QueryContext};
use crate::engine::{
    ExecutionContext, InitializedVectorOperator, OperatorData, OperatorName, ResultDescriptor,
    SourceOperator, TypedVectorQueryProcessor, VectorOperator, VectorQueryProcessor,
    VectorResultDescriptor, WorkflowOperatorPath,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use geoengine_datatypes::collections::{
    FeatureCollection, FeatureCollectionInfos, FeatureCollectionModifications,
};
use geoengine_datatypes::dataset::NamedData;
use geoengine_datatypes::primitives::{
    Geometry, Measurement, MultiLineString, MultiPoint, MultiPolygon, NoGeometry, TimeInterval,
    VectorQueryRectangle,
};
use geoengine_datatypes::spatial_reference::{SpatialReference, SpatialReferenceOption};
use geoengine_datatypes::util::arrow::ArrowTyped;
use serde::{Deserialize, Serialize};

pub struct MockFeatureCollectionSourceProcessor<G>
where
    G: Geometry + ArrowTyped,
{
    result_descriptor: VectorResultDescriptor,
    collections: Vec<FeatureCollection<G>>,
}

#[async_trait]
impl<G> VectorQueryProcessor for MockFeatureCollectionSourceProcessor<G>
where
    G: Geometry + ArrowTyped + Send + Sync + 'static,
{
    type VectorType = FeatureCollection<G>;

    async fn vector_query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        _ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::VectorType>>> {
        // TODO: chunk it up
        // let chunk_size = ctx.chunk_byte_size / std::mem::size_of::<Coordinate2D>();

        // TODO: filter spatially

        let stream = stream::iter(
            self.collections
                .iter()
                .map(move |c| filter_time_intervals(c, query.time_interval)),
        );

        Ok(stream.boxed())
    }

    fn vector_result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }
}

fn filter_time_intervals<G>(
    feature_collection: &FeatureCollection<G>,
    time_interval: TimeInterval,
) -> Result<FeatureCollection<G>>
where
    G: Geometry + ArrowTyped,
{
    let mask: Vec<bool> = feature_collection
        .time_intervals()
        .iter()
        .map(|ti| ti.intersects(&time_interval))
        .collect();

    feature_collection.filter(mask).map_err(Into::into)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MockFeatureCollectionSourceParams<G>
where
    G: Geometry + ArrowTyped,
{
    pub collections: Vec<FeatureCollection<G>>,
    pub spatial_reference: SpatialReferenceOption,
    measurements: Option<HashMap<String, Measurement>>,
}

pub type MockFeatureCollectionSource<G> = SourceOperator<MockFeatureCollectionSourceParams<G>>;

impl<G: Geometry + ArrowTyped> OperatorName for MockFeatureCollectionSource<G> {
    const TYPE_NAME: &'static str = "MockFeatureCollectionSource";
}

impl<G> OperatorData for MockFeatureCollectionSource<G>
where
    G: Geometry + ArrowTyped,
{
    fn data_names_collect(&self, _data_names: &mut Vec<NamedData>) {}
}

impl<G> MockFeatureCollectionSource<G>
where
    G: Geometry + ArrowTyped,
{
    pub fn single(collection: FeatureCollection<G>) -> Self {
        Self::multiple(vec![collection])
    }

    pub fn multiple(collections: Vec<FeatureCollection<G>>) -> Self {
        Self::with_collections_and_sref(collections, SpatialReference::epsg_4326())
    }

    pub fn with_collections_and_sref(
        collections: Vec<FeatureCollection<G>>,
        spatial_reference: SpatialReference,
    ) -> Self {
        Self {
            params: MockFeatureCollectionSourceParams {
                spatial_reference: spatial_reference.into(),
                measurements: None,
                collections,
            },
        }
    }

    pub fn with_collections_and_measurements(
        collections: Vec<FeatureCollection<G>>,
        measurements: HashMap<String, Measurement>,
    ) -> Self {
        Self {
            params: MockFeatureCollectionSourceParams {
                collections,
                spatial_reference: SpatialReference::epsg_4326().into(),
                measurements: Some(measurements),
            },
        }
    }
}

pub struct InitializedMockFeatureCollectionSource<R: ResultDescriptor, G: Geometry> {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
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
        #[async_trait]
        impl VectorOperator for $newtype {
            async fn _initialize(
                self: Box<Self>,
                path: WorkflowOperatorPath,
                _context: &dyn ExecutionContext,
            ) -> Result<Box<dyn InitializedVectorOperator>> {
                let columns = self.params.collections[0]
                    .column_types()
                    .into_iter()
                    .map(|(name, data_type)| {
                        let measurement = self
                            .params
                            .measurements
                            .as_ref()
                            .and_then(|m| m.get(&name).cloned())
                            .into();
                        (
                            name,
                            crate::engine::VectorColumnInfo {
                                data_type,
                                measurement,
                            },
                        )
                    })
                    .collect();

                let result_descriptor = VectorResultDescriptor {
                    data_type: <$geometry>::DATA_TYPE,
                    spatial_reference: self.params.spatial_reference,
                    columns,
                    time: None,
                    bbox: None,
                };

                Ok(InitializedMockFeatureCollectionSource {
                    name: CanonicOperatorName::from(&self),
                    path,
                    result_descriptor,
                    collections: self.params.collections,
                }
                .boxed())
            }

            span_fn!($newtype);
        }

        impl InitializedVectorOperator
            for InitializedMockFeatureCollectionSource<VectorResultDescriptor, $geometry>
        {
            fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
                Ok(TypedVectorQueryProcessor::$output(
                    MockFeatureCollectionSourceProcessor {
                        result_descriptor: self.result_descriptor.clone(),
                        collections: self.collections.clone(),
                    }
                    .boxed(),
                ))
            }
            fn result_descriptor(&self) -> &VectorResultDescriptor {
                &self.result_descriptor
            }
            fn canonic_name(&self) -> CanonicOperatorName {
                self.name.clone()
            }
            fn name(&self) -> &'static str {
                MockFeatureCollectionSource::<$geometry>::TYPE_NAME
            }
            fn path(&self) -> WorkflowOperatorPath {
                self.path.clone()
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
    use crate::engine::QueryProcessor;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use futures::executor::block_on_stream;
    use geoengine_datatypes::collections::{ChunksEqualIgnoringCacheHint, MultiPointCollection};
    use geoengine_datatypes::primitives::{
        BoundingBox2D, CacheHint, ColumnSelection, Coordinate2D, FeatureData, TimeInterval,
    };
    use geoengine_datatypes::raster::TilingSpecification;
    use geoengine_datatypes::util::test::TestDefault;

    #[test]
    #[allow(clippy::too_many_lines)]
    fn serde() {
        let cache_hint = CacheHint::default();
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
            cache_hint,
        )
        .unwrap();

        let source = MockFeatureCollectionSource::single(collection).boxed();

        let serialized = serde_json::to_value(&source).unwrap();

        // TODO: replace at some point with human-readable serialization
        // for now, you can replace the expected value with the output of the following line
        // eprintln!("{}", serialized["params"]["collections"][0]["table"]);

        let collection_bytes = [
            65, 82, 82, 79, 87, 49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 184, 1, 0, 0, 16, 0, 0, 0, 0, 0, 10, 0, 12,
            0, 10, 0, 9, 0, 4, 0, 10, 0, 0, 0, 16, 0, 0, 0, 0, 1, 4, 0, 8, 0, 8, 0, 0, 0, 4, 0, 8,
            0, 0, 0, 4, 0, 0, 0, 3, 0, 0, 0, 180, 0, 0, 0, 56, 0, 0, 0, 4, 0, 0, 0, 52, 255, 255,
            255, 16, 0, 0, 0, 24, 0, 0, 0, 0, 0, 1, 2, 20, 0, 0, 0, 172, 255, 255, 255, 64, 0, 0,
            0, 0, 0, 0, 1, 0, 0, 0, 0, 6, 0, 0, 0, 102, 111, 111, 98, 97, 114, 0, 0, 152, 255, 255,
            255, 24, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 16, 76, 0, 0, 0, 1, 0, 0, 0, 12, 0, 0, 0, 82,
            255, 255, 255, 2, 0, 0, 0, 136, 255, 255, 255, 24, 0, 0, 0, 32, 0, 0, 0, 0, 0, 1, 2,
            28, 0, 0, 0, 8, 0, 12, 0, 4, 0, 11, 0, 8, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
            4, 0, 0, 0, 105, 116, 101, 109, 0, 0, 0, 0, 6, 0, 0, 0, 95, 95, 116, 105, 109, 101, 0,
            0, 16, 0, 20, 0, 16, 0, 0, 0, 15, 0, 4, 0, 0, 0, 8, 0, 16, 0, 0, 0, 28, 0, 0, 0, 12, 0,
            0, 0, 0, 0, 0, 12, 160, 0, 0, 0, 1, 0, 0, 0, 28, 0, 0, 0, 4, 0, 4, 0, 4, 0, 0, 0, 16,
            0, 20, 0, 16, 0, 14, 0, 15, 0, 4, 0, 0, 0, 8, 0, 16, 0, 0, 0, 32, 0, 0, 0, 12, 0, 0, 0,
            0, 0, 1, 16, 96, 0, 0, 0, 1, 0, 0, 0, 36, 0, 0, 0, 0, 0, 6, 0, 8, 0, 4, 0, 6, 0, 0, 0,
            2, 0, 0, 0, 16, 0, 22, 0, 16, 0, 14, 0, 15, 0, 4, 0, 0, 0, 8, 0, 16, 0, 0, 0, 24, 0, 0,
            0, 28, 0, 0, 0, 0, 0, 1, 3, 24, 0, 0, 0, 0, 0, 6, 0, 8, 0, 6, 0, 6, 0, 0, 0, 0, 0, 2,
            0, 0, 0, 0, 0, 4, 0, 0, 0, 105, 116, 101, 109, 0, 0, 0, 0, 4, 0, 0, 0, 105, 116, 101,
            109, 0, 0, 0, 0, 10, 0, 0, 0, 95, 95, 103, 101, 111, 109, 101, 116, 114, 121, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 120, 1, 0,
            0, 16, 0, 0, 0, 12, 0, 26, 0, 24, 0, 23, 0, 4, 0, 8, 0, 12, 0, 0, 0, 32, 0, 0, 0, 128,
            2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 4, 0, 10, 0, 24, 0, 12, 0, 8, 0, 4, 0, 10,
            0, 0, 0, 124, 0, 0, 0, 16, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 3,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
            0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 1,
            0, 0, 0, 0, 0, 0, 0, 192, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
            0, 0, 0, 48, 0, 0, 0, 0, 0, 0, 0, 64, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 128,
            1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 192, 1, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0, 0,
            0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 64, 2, 0, 0, 0, 0, 0, 0, 24,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0,
            3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 154, 153, 153, 153, 153, 153, 185, 63,
            0, 0, 0, 0, 0, 0, 240, 63, 154, 153, 153, 153, 153, 153, 241, 63, 0, 0, 0, 0, 0, 0, 0,
            64, 205, 204, 204, 204, 204, 204, 8, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 0, 16, 0, 0, 0, 12, 0, 20, 0, 18, 0, 12, 0, 8,
            0, 4, 0, 12, 0, 0, 0, 152, 1, 0, 0, 180, 1, 0, 0, 16, 0, 0, 0, 0, 0, 4, 0, 8, 0, 8, 0,
            0, 0, 4, 0, 8, 0, 0, 0, 4, 0, 0, 0, 3, 0, 0, 0, 180, 0, 0, 0, 56, 0, 0, 0, 4, 0, 0, 0,
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
            0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 128, 1, 0, 0, 0, 0, 0, 0, 128, 2, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 208, 1, 0, 0, 65, 82, 82, 79, 87, 49,
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
                        "cacheHint": cache_hint
                    }],
                    "spatialReference": "EPSG:4326",
                    "measurements": null,
                },
            })
        );

        let _operator: Box<dyn VectorOperator> = serde_json::from_value(serialized).unwrap();
    }

    #[tokio::test]
    async fn execute() {
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
            CacheHint::default(),
        )
        .unwrap();

        let source = MockFeatureCollectionSource::single(collection.clone()).boxed();

        let source = source
            .initialize(
                WorkflowOperatorPath::initialize_root(),
                &MockExecutionContext::test_default(),
            )
            .await
            .unwrap();

        let Ok(TypedVectorQueryProcessor::MultiPoint(processor)) = source.query_processor() else {
            panic!()
        };

        let query_rectangle = VectorQueryRectangle::with_bounds(
            BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            TimeInterval::default(),
            ColumnSelection::all(),
        );
        let ctx = MockQueryContext::new(
            (2 * std::mem::size_of::<Coordinate2D>()).into(),
            TilingSpecification::test_default(),
        );

        let stream = processor.query(query_rectangle, &ctx).await.unwrap();

        let blocking_stream = block_on_stream(stream);

        let collections: Vec<MultiPointCollection> = blocking_stream.map(Result::unwrap).collect();

        assert_eq!(collections.len(), 1);

        assert!(collections[0].chunks_equal_ignoring_cache_hint(&collection));
    }
}
