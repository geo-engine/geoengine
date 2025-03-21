use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedSources, InitializedVectorOperator, Operator,
    OperatorName, QueryContext, QueryProcessor, TypedVectorQueryProcessor, VectorOperator,
    VectorQueryProcessor, VectorResultDescriptor, WorkflowOperatorPath,
};
use crate::error;
use crate::util::Result;
use crate::util::input::StringOrNumberRange;
use crate::{adapters::FeatureCollectionChunkMerger, engine::SingleVectorSource};
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use geoengine_datatypes::collections::{
    FeatureCollection, FeatureCollectionInfos, FeatureCollectionModifications,
};
use geoengine_datatypes::primitives::{
    BoundingBox2D, ColumnSelection, FeatureDataType, FeatureDataValue, Geometry,
    VectorQueryRectangle,
};
use geoengine_datatypes::util::arrow::ArrowTyped;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::ops::RangeInclusive;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ColumnRangeFilterParams {
    pub column: String,
    pub ranges: Vec<StringOrNumberRange>,
    pub keep_nulls: bool,
}

pub type ColumnRangeFilter = Operator<ColumnRangeFilterParams, SingleVectorSource>;

impl OperatorName for ColumnRangeFilter {
    const TYPE_NAME: &'static str = "ColumnRangeFilter";
}

#[typetag::serde]
#[async_trait]
impl VectorOperator for ColumnRangeFilter {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedVectorOperator>> {
        let name = CanonicOperatorName::from(&self);

        let initialized_sources = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?;

        let initialized_operator = InitializedColumnRangeFilter {
            name,
            path,
            result_descriptor: initialized_sources.vector.result_descriptor().clone(),
            vector_source: initialized_sources.vector,
            state: self.params,
        };

        Ok(initialized_operator.boxed())
    }

    span_fn!(ColumnRangeFilter);
}

pub struct InitializedColumnRangeFilter {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    result_descriptor: VectorResultDescriptor,
    vector_source: Box<dyn InitializedVectorOperator>,
    state: ColumnRangeFilterParams,
}

impl InitializedVectorOperator for InitializedColumnRangeFilter {
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        Ok(map_typed_query_processor!(
            self.vector_source.query_processor()?,
            source => ColumnRangeFilterProcessor::new(source, self.state.clone()).boxed()
        ))
    }

    fn result_descriptor(&self) -> &VectorResultDescriptor {
        &self.result_descriptor
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        ColumnRangeFilter::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }
}

pub struct ColumnRangeFilterProcessor<G> {
    vector_type: PhantomData<FeatureCollection<G>>,
    source: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
    column: String,
    keep_nulls: bool,
    ranges: Vec<StringOrNumberRange>,
}

impl<G> ColumnRangeFilterProcessor<G>
where
    G: Geometry + ArrowTyped + Sync + Send,
{
    pub fn new(
        source: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
        params: ColumnRangeFilterParams,
    ) -> Self {
        Self {
            vector_type: Default::default(),
            source,
            column: params.column,
            keep_nulls: params.keep_nulls,
            ranges: params.ranges,
        }
    }
}

#[async_trait]
impl<G> QueryProcessor for ColumnRangeFilterProcessor<G>
where
    G: Geometry + ArrowTyped + Sync + Send + 'static,
{
    type Output = FeatureCollection<G>;
    type SpatialBounds = BoundingBox2D;
    type Selection = ColumnSelection;
    type ResultDescription = VectorResultDescriptor;

    async fn _query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let column_name = self.column.clone();
        let ranges = self.ranges.clone();
        let keep_nulls = self.keep_nulls;

        let filter_stream = self.source.query(query, ctx).await?.map(move |collection| {
            let collection = collection?;

            // TODO: do transformation work only once
            let ranges: Result<Vec<RangeInclusive<FeatureDataValue>>> =
                match collection.column_type(&column_name)? {
                    FeatureDataType::Text => ranges
                        .iter()
                        .cloned()
                        .map(|range| range.into_string_range().map(Into::into))
                        .collect(),
                    FeatureDataType::Float => ranges
                        .iter()
                        .cloned()
                        .map(|range| range.into_float_range().map(Into::into))
                        .collect(),
                    FeatureDataType::Int => ranges
                        .iter()
                        .cloned()
                        .map(|range| range.into_int_range().map(Into::into))
                        .collect(),
                    FeatureDataType::Bool => ranges
                        .iter()
                        .cloned()
                        .map(|range| range.into_int_range().map(Into::into))
                        .collect(),
                    FeatureDataType::DateTime => ranges
                        .iter()
                        .cloned()
                        .map(|range| range.into_int_range().map(Into::into))
                        .collect(),
                    FeatureDataType::Category => Err(error::Error::InvalidType {
                        expected: "text, float, int, bool or datetime".to_string(),
                        found: "category".to_string(),
                    }),
                };

            collection
                .column_range_filter(&column_name, &ranges?, keep_nulls)
                .map_err(Into::into)
        });

        let merged_chunks_stream =
            FeatureCollectionChunkMerger::new(filter_stream.fuse(), ctx.chunk_byte_size().into());

        Ok(merged_chunks_stream.boxed())
    }

    fn result_descriptor(&self) -> &VectorResultDescriptor {
        self.source.result_descriptor()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use crate::mock::MockFeatureCollectionSource;
    use geoengine_datatypes::collections::{
        ChunksEqualIgnoringCacheHint, FeatureCollectionModifications, MultiPointCollection,
    };
    use geoengine_datatypes::primitives::CacheHint;
    use geoengine_datatypes::primitives::{
        BoundingBox2D, Coordinate2D, FeatureData, MultiPoint, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::util::test::TestDefault;

    #[test]
    fn serde() {
        let filter = ColumnRangeFilter {
            params: ColumnRangeFilterParams {
                column: "foobar".to_string(),
                ranges: vec![(1..=2).into()],
                keep_nulls: false,
            },
            sources: MockFeatureCollectionSource::<MultiPoint>::multiple(vec![])
                .boxed()
                .into(),
        }
        .boxed();

        let serialized = serde_json::to_value(&filter).unwrap();

        assert_eq!(
            serialized,
            serde_json::json!({
                "type": "ColumnRangeFilter",
                "params": {
                    "column": "foobar",
                    "ranges": [
                        [1, 2]
                    ],
                    "keepNulls": false
                },
                "sources": {
                    "vector": {
                        "type": "MockFeatureCollectionSourceMultiPoint",
                        "params": {
                            "collections": [],
                            "spatialReference": "EPSG:4326",
                            "measurements": null,
                        }
                    }
                },
            })
        );

        let _operator: Box<dyn VectorOperator> = serde_json::from_value(serialized).unwrap();
    }

    #[tokio::test]
    async fn execute() {
        let column_name = "foo";

        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 2.1), (3.0, 3.1)]).unwrap(),
            vec![TimeInterval::new(0, 1).unwrap(); 4],
            [(
                column_name.to_string(),
                FeatureData::Float(vec![0., 1., 2., 3.]),
            )]
            .iter()
            .cloned()
            .collect(),
            CacheHint::default(),
        )
        .unwrap();

        let source = MockFeatureCollectionSource::single(collection.clone()).boxed();

        let filter = ColumnRangeFilter {
            params: ColumnRangeFilterParams {
                column: column_name.to_string(),
                ranges: vec![(1..=2).into()],
                keep_nulls: false,
            },
            sources: source.into(),
        }
        .boxed();

        let initialized = filter
            .initialize(
                WorkflowOperatorPath::initialize_root(),
                &MockExecutionContext::test_default(),
            )
            .await
            .unwrap();

        let Ok(TypedVectorQueryProcessor::MultiPoint(point_processor)) =
            initialized.query_processor()
        else {
            panic!();
        };

        let query_rectangle = VectorQueryRectangle {
            spatial_bounds: BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            time_interval: TimeInterval::default(),
            spatial_resolution: SpatialResolution::zero_point_one(),
            attributes: ColumnSelection::all(),
        };

        let ctx = MockQueryContext::new((2 * std::mem::size_of::<Coordinate2D>()).into());

        let stream = point_processor.query(query_rectangle, &ctx).await.unwrap();

        let collections: Vec<MultiPointCollection> = stream.map(Result::unwrap).collect().await;

        assert_eq!(collections.len(), 1);

        assert!(collections[0].chunks_equal_ignoring_cache_hint(
            &collection.filter(vec![false, true, true, false]).unwrap()
        ));
    }
}
