use crate::adapters::FeatureCollectionChunkMerger;
use crate::engine::{
    ExecutionContext, InitializedOperator, InitializedOperatorImpl, InitializedVectorOperator,
    Operator, QueryContext, QueryProcessor, QueryRectangle, TypedVectorQueryProcessor,
    VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
};
use crate::error;
use crate::util::input::StringOrNumberRange;
use crate::util::Result;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::collections::FeatureCollection;
use geoengine_datatypes::primitives::{FeatureDataType, FeatureDataValue, Geometry};
use geoengine_datatypes::util::arrow::ArrowTyped;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::marker::PhantomData;
use std::ops::RangeInclusive;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnRangeFilterParams {
    pub column: String,
    pub ranges: Vec<StringOrNumberRange>,
    pub keep_nulls: bool,
}

pub type ColumnRangeFilter = Operator<ColumnRangeFilterParams>;

#[typetag::serde]
impl VectorOperator for ColumnRangeFilter {
    fn initialize(
        self: Box<Self>,
        context: ExecutionContext,
    ) -> Result<Box<InitializedVectorOperator>> {
        // TODO: create generic validate util
        ensure!(
            self.vector_sources.len() == 1,
            error::InvalidNumberOfVectorInputs {
                expected: 1..2,
                found: self.vector_sources.len()
            }
        );
        ensure!(
            self.raster_sources.is_empty(),
            error::InvalidNumberOfRasterInputs {
                expected: 0..1,
                found: self.raster_sources.len()
            }
        );

        InitializedColumnRangeFilter::create(
            self.params,
            context,
            |_, _, _, _| Ok(()),
            |_, _, _, _, vector_sources| Ok(vector_sources[0].result_descriptor()),
            self.raster_sources,
            self.vector_sources,
        )
        .map(InitializedColumnRangeFilter::boxed)
    }
}

pub type InitializedColumnRangeFilter =
    InitializedOperatorImpl<ColumnRangeFilterParams, VectorResultDescriptor, ()>;

impl InitializedOperator<VectorResultDescriptor, TypedVectorQueryProcessor>
    for InitializedColumnRangeFilter
{
    fn query_processor(&self) -> Result<TypedVectorQueryProcessor> {
        match self.vector_sources[0].query_processor()? {
            // TODO: use macro for that
            TypedVectorQueryProcessor::Data(source) => Ok(TypedVectorQueryProcessor::Data(
                ColumnRangeFilterProcessor::new(source, self.params.clone()).boxed(),
            )),
            TypedVectorQueryProcessor::MultiPoint(source) => {
                Ok(TypedVectorQueryProcessor::MultiPoint(
                    ColumnRangeFilterProcessor::new(source, self.params.clone()).boxed(),
                ))
            }
            TypedVectorQueryProcessor::MultiLineString(source) => {
                Ok(TypedVectorQueryProcessor::MultiLineString(
                    ColumnRangeFilterProcessor::new(source, self.params.clone()).boxed(),
                ))
            }
            TypedVectorQueryProcessor::MultiPolygon(source) => {
                Ok(TypedVectorQueryProcessor::MultiPolygon(
                    ColumnRangeFilterProcessor::new(source, self.params.clone()).boxed(),
                ))
            }
        }
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

impl<G> VectorQueryProcessor for ColumnRangeFilterProcessor<G>
where
    G: Geometry + ArrowTyped + Sync + Send + 'static,
{
    type VectorType = FeatureCollection<G>;

    fn vector_query(
        &self,
        query: QueryRectangle,
        ctx: QueryContext,
    ) -> BoxStream<Result<Self::VectorType>> {
        let column_name = self.column.clone();
        let ranges = self.ranges.clone();
        let keep_nulls = self.keep_nulls;

        let filter_stream = self.source.query(query, ctx).map(move |collection| {
            let collection = collection?;

            // TODO: do transformation work only once
            let ranges: Result<Vec<RangeInclusive<FeatureDataValue>>> =
                match collection.column_type(&column_name)? {
                    FeatureDataType::Text | FeatureDataType::NullableText => ranges
                        .iter()
                        .cloned()
                        .map(|range| range.into_string_range().map(Into::into))
                        .collect(),
                    FeatureDataType::Number | FeatureDataType::NullableNumber => ranges
                        .iter()
                        .cloned()
                        .map(|range| range.into_number_range().map(Into::into))
                        .collect(),
                    FeatureDataType::Decimal | FeatureDataType::NullableDecimal => ranges
                        .iter()
                        .cloned()
                        .map(|range| range.into_decimal_range().map(Into::into))
                        .collect(),
                    FeatureDataType::Categorical | FeatureDataType::NullableCategorical => {
                        Err(error::Error::InvalidType {
                            expected: "text, number, or decimal".to_string(),
                            found: "categorical".to_string(),
                        })
                    }
                };

            collection
                .column_range_filter(&column_name, &ranges?, keep_nulls)
                .map_err(Into::into)
        });

        let merged_chunks_stream =
            FeatureCollectionChunkMerger::new(filter_stream.fuse(), ctx.chunk_byte_size);

        merged_chunks_stream.boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::{MockFeatureCollectionSource, MockFeatureCollectionSourceParams};
    use geoengine_datatypes::collections::MultiPointCollection;
    use geoengine_datatypes::primitives::{
        BoundingBox2D, Coordinate2D, FeatureData, MultiPoint, TimeInterval,
    };

    #[test]
    fn serde() {
        let filter = ColumnRangeFilter {
            params: ColumnRangeFilterParams {
                column: "foobar".to_string(),
                ranges: vec![(1..=2).into()],
                keep_nulls: false,
            },
            vector_sources: vec![],
            raster_sources: vec![],
        }
        .boxed();

        let serialized = serde_json::to_string(&filter).unwrap();

        assert_eq!(
            serialized,
            serde_json::json!({
                "type": "ColumnRangeFilter",
                "params": {
                    "column": "foobar",
                    "ranges": [
                        [1, 2]
                    ],
                    "keep_nulls": false
                },
                "raster_sources": [],
                "vector_sources": []
            })
            .to_string()
        );

        let _: Box<dyn VectorOperator> = serde_json::from_str(&serialized).unwrap();
    }

    #[tokio::test]
    async fn execute() {
        let column_name = "foo";

        let collection = MultiPointCollection::from_data(
            MultiPoint::many(vec![(0.0, 0.1), (1.0, 1.1), (2.0, 2.1), (3.0, 3.1)]).unwrap(),
            vec![TimeInterval::new(0, 1).unwrap(); 4],
            [(
                column_name.to_string(),
                FeatureData::Number(vec![0., 1., 2., 3.]),
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

        let filter = ColumnRangeFilter {
            params: ColumnRangeFilterParams {
                column: column_name.to_string(),
                ranges: vec![(1..=2).into()],
                keep_nulls: false,
            },
            vector_sources: vec![source],
            raster_sources: vec![],
        }
        .boxed();

        let initialized = filter.initialize(ExecutionContext).unwrap();

        let point_processor = match initialized.query_processor() {
            Ok(TypedVectorQueryProcessor::MultiPoint(processor)) => processor,
            _ => panic!(),
        };

        let query_rectangle = QueryRectangle {
            bbox: BoundingBox2D::new((0., 0.).into(), (4., 4.).into()).unwrap(),
            time_interval: TimeInterval::default(),
        };
        let ctx = QueryContext {
            chunk_byte_size: 2 * std::mem::size_of::<Coordinate2D>(),
        };
        let stream = point_processor.vector_query(query_rectangle, ctx);

        let collections: Vec<MultiPointCollection> = stream.map(Result::unwrap).collect().await;

        assert_eq!(collections.len(), 1);

        assert_eq!(
            collections[0],
            collection.filter(vec![false, true, true, false]).unwrap()
        );
    }
}
