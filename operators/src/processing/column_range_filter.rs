use crate::engine::{
    ExecutionContext, InitializedOperator, InitializedOperatorImpl, InitializedVectorOperator,
    Operator, QueryContext, QueryProcessor, QueryRectangle, TypedVectorQueryProcessor,
    VectorOperator, VectorQueryProcessor, VectorResultDescriptor,
};
use crate::error;
use crate::util::input::StringOrNumber;
use crate::util::Result;
use failure::_core::marker::PhantomData;
use failure::_core::ops::RangeInclusive;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::collections::FeatureCollection;
use geoengine_datatypes::primitives::{FeatureDataType, FeatureDataValue, Geometry};
use geoengine_datatypes::util::arrow::ArrowTyped;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::convert::TryInto;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnRangeFilterParams {
    pub column: String,
    pub ranges: Vec<(StringOrNumber, StringOrNumber)>,
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
    params: ColumnRangeFilterParams,
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
            params,
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
        let column_name = self.params.column.clone();
        let ranges = self.params.ranges.clone();
        let keep_nulls = self.params.keep_nulls;

        // TODO: create stream adapter that munches collections together to adhere to chunk size
        self.source
            .query(query, ctx)
            .map(move |collection| {
                let collection = collection?;

                let filter_ranges: Result<Vec<RangeInclusive<FeatureDataValue>>> =
                    // TODO: do transformation work only once
                    match collection.column_type(&column_name)? {
                        FeatureDataType::Text | FeatureDataType::NullableText => ranges
                            .iter()
                            .map(|(range_start, range_end)| {
                                Ok(FeatureDataValue::Text(range_start.try_into()?)
                                    ..=FeatureDataValue::Text(range_end.try_into()?))
                            })
                            .collect(),
                        FeatureDataType::Number | FeatureDataType::NullableNumber => ranges
                            .iter()
                            .map(|(range_start, range_end)| {
                                Ok(FeatureDataValue::Number(range_start.try_into()?)
                                    ..=FeatureDataValue::Number(range_end.try_into()?))
                            })
                            .collect(),
                        FeatureDataType::Decimal | FeatureDataType::NullableDecimal => ranges
                            .iter()
                            .map(|(range_start, range_end)| {
                                Ok(FeatureDataValue::Decimal(range_start.try_into()?)
                                    ..=FeatureDataValue::Decimal(range_end.try_into()?))
                            })
                            .collect(),
                        FeatureDataType::Categorical | FeatureDataType::NullableCategorical => {
                            Err(error::Error::InvalidType {
                                expected: "text, number, or decimal".to_string(),
                                found: "categorical".to_string(),
                            })
                        }
                    };

                collection
                    .column_range_filter(&column_name, &filter_ranges?, keep_nulls)
                    .map_err(Into::into)
            })
            .boxed()
    }
}
