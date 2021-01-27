use futures::stream::BoxStream;
use futures::StreamExt;

use geoengine_datatypes::collections::{
    FeatureCollectionInfos, FeatureCollectionModifications, MultiPointCollection,
};
use geoengine_datatypes::raster::{Pixel, RasterDataType};

use crate::engine::{
    QueryContext, QueryProcessor, QueryRectangle, RasterQueryProcessor, TypedRasterQueryProcessor,
    VectorQueryProcessor,
};
use crate::processing::raster_vector_join::aggregator::{
    Aggregator, FirstValueDecimalAggregator, FirstValueNumberAggregator, MeanValueAggregator,
    TypedAggregator,
};
use crate::processing::raster_vector_join::AggregationMethod;
use crate::util::Result;

pub struct RasterPointJoinProcessor {
    points: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
    rasters: Vec<TypedRasterQueryProcessor>,
    column_names: Vec<String>,
    aggregation: AggregationMethod,
}

impl RasterPointJoinProcessor {
    pub fn new(
        points: Box<dyn VectorQueryProcessor<VectorType = MultiPointCollection>>,
        rasters: Vec<TypedRasterQueryProcessor>,
        column_names: Vec<String>,
        aggregation: AggregationMethod,
    ) -> Self {
        Self {
            points,
            rasters,
            column_names,
            aggregation,
        }
    }

    fn extract_raster_values<P: Pixel>(
        points: &MultiPointCollection,
        raster: &dyn RasterQueryProcessor<RasterType = P>,
        new_column_name: &str,
        aggregation: AggregationMethod,
    ) -> Result<MultiPointCollection> {
        let mut aggregator = Self::create_aggregator::<P>(points.len(), aggregation);

        // TODO: call rasters, etc.
        let _raster = raster;
        aggregator.add_value(0, 0, 1);

        points
            .add_column(new_column_name, aggregator.into_data())
            .map_err(Into::into)
    }

    fn create_aggregator<P: Pixel>(
        number_of_features: usize,
        aggregation: AggregationMethod,
    ) -> TypedAggregator {
        match aggregation {
            AggregationMethod::First => match P::TYPE {
                RasterDataType::U8
                | RasterDataType::U16
                | RasterDataType::U32
                | RasterDataType::U64
                | RasterDataType::I8
                | RasterDataType::I16
                | RasterDataType::I32
                | RasterDataType::I64 => {
                    FirstValueDecimalAggregator::new(number_of_features).into_typed()
                }
                RasterDataType::F32 | RasterDataType::F64 => {
                    FirstValueNumberAggregator::new(number_of_features).into_typed()
                }
            },
            AggregationMethod::Mean => MeanValueAggregator::new(number_of_features).into_typed(),
        }
    }
}

impl VectorQueryProcessor for RasterPointJoinProcessor {
    type VectorType = MultiPointCollection;

    fn vector_query(
        &self,
        query: QueryRectangle,
        ctx: QueryContext,
    ) -> BoxStream<'_, Result<Self::VectorType>> {
        self.points
            .query(query, ctx)
            .map(move |mut points| {
                for (raster, new_column_name) in self.rasters.iter().zip(&self.column_names) {
                    points = call_on_generic_raster_processor!(raster, raster => Self::extract_raster_values(&points?, raster, new_column_name, self.aggregation));
                }

                points
            })
            .boxed()
    }
}
