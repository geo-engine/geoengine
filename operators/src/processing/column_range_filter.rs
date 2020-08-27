use crate::engine::{
    ExecutionContext, InitializedOperator, InitializedOperatorImpl, InitializedVectorOperator,
    Operator, QueryContext, QueryRectangle, TypedVectorQueryProcessor, VectorOperator,
    VectorQueryProcessor, VectorResultDescriptor,
};
use crate::error;
use crate::util::input::StringOrNumber;
use crate::util::Result;
use failure::_core::marker::PhantomData;
use futures::stream::BoxStream;
use geoengine_datatypes::collections::VectorDataType;
use serde::{Deserialize, Serialize};
use snafu::ensure;

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
        match self.result_descriptor.data_type {
            // TODO: use macro for that
            VectorDataType::Data => Ok(TypedVectorQueryProcessor::Data(
                ColumnRangeFilterProcessor::new().boxed(),
            )),
            VectorDataType::MultiPoint => Ok(TypedVectorQueryProcessor::MultiPoint(
                ColumnRangeFilterProcessor::new().boxed(),
            )),
            VectorDataType::MultiLineString => Ok(TypedVectorQueryProcessor::MultiLineString(
                ColumnRangeFilterProcessor::new().boxed(),
            )),
            VectorDataType::MultiPolygon => Ok(TypedVectorQueryProcessor::MultiPolygon(
                ColumnRangeFilterProcessor::new().boxed(),
            )),
        }
    }
}

pub struct ColumnRangeFilterProcessor<V> {
    vector_type: PhantomData<V>,
}

impl<V> ColumnRangeFilterProcessor<V>
where
    V: Sync + Send,
{
    pub fn new() -> Self {
        Self {
            vector_type: Default::default(),
        }
    }
}

impl<V> VectorQueryProcessor for ColumnRangeFilterProcessor<V>
where
    V: Sync + Send,
{
    type VectorType = V;

    fn vector_query(
        &self,
        _query: QueryRectangle,
        _ctx: QueryContext,
    ) -> BoxStream<Result<Self::VectorType>> {
        unimplemented!()
    }
}
