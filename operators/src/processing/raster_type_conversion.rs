use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt, TryFutureExt, TryStreamExt};
use geoengine_datatypes::{
    primitives::{Measurement, RasterQueryRectangle, SpatialPartition2D},
    raster::{ConvertDataTypeParallel, DefaultNoDataValue, Pixel, RasterTile2D},
};
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};

use crate::engine::{
    ExecutionContext, InitializedRasterOperator, Operator, QueryContext, QueryProcessor,
    RasterOperator, RasterQueryProcessor, RasterResultDescriptor, SingleRasterSource,
    TypedRasterQueryProcessor,
};
use crate::util::Result;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RasterTypeConversionParams {
    // output_no_data_value: Option<f64>, TODO: discuss
    output_measurement: Option<Measurement>,
}

pub type RasterTypeConversionOperator = Operator<RasterTypeConversionParams, SingleRasterSource>;

pub struct InitializedRasterTypeConversionOperator {
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for RasterTypeConversionOperator {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let input = self.sources.raster.initialize(context).await?;
        let in_desc = input.result_descriptor();

        let out_no_data_value = in_desc.no_data_value;

        let out_desc = RasterResultDescriptor {
            spatial_reference: in_desc.spatial_reference,
            data_type: in_desc.data_type,
            measurement: self
                .params
                .output_measurement
                .unwrap_or(in_desc.measurement.clone()),
            no_data_value: out_no_data_value,
        };

        let initialized_operator = InitializedRasterTypeConversionOperator {
            result_descriptor: out_desc,
            source: input,
        };

        Ok(initialized_operator.boxed())
    }
}

impl InitializedRasterOperator for InitializedRasterTypeConversionOperator {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source = self.source.query_processor()?;
        let out_data_type = self.result_descriptor.data_type;

        let res_op = call_on_generic_raster_processor!(source, source_proc => {
            call_generic_raster_processor!(out_data_type,
                RasterTypeConversionQueryProcessor::create_boxed(source_proc)
            )
        });

        Ok(res_op)
    }
}

pub struct RasterTypeConversionQueryProcessor<
    Q: RasterQueryProcessor<RasterType = PIn>,
    PIn: Pixel,
    POut: Pixel,
> {
    query_processor: Q,
    _p_out: std::marker::PhantomData<POut>,
}

impl<Q, PIn, POut> RasterTypeConversionQueryProcessor<Q, PIn, POut>
where
    Q: 'static + RasterQueryProcessor<RasterType = PIn>,
    PIn: Pixel + AsPrimitive<POut>,
    POut: Pixel,
{
    pub fn new(query_processor: Q) -> Self {
        Self {
            query_processor,
            _p_out: std::marker::PhantomData,
        }
    }

    pub fn create_boxed(source: Q) -> Box<dyn RasterQueryProcessor<RasterType = POut>> {
        RasterTypeConversionQueryProcessor::new(source).boxed()
    }
}

#[async_trait]
impl<'a, Q, PIn: Pixel, POut: Pixel> QueryProcessor
    for RasterTypeConversionQueryProcessor<Q, PIn, POut>
where
    PIn: AsPrimitive<POut>,
    POut: DefaultNoDataValue,
    Q: RasterQueryProcessor<RasterType = PIn>,
{
    type Output = RasterTile2D<POut>;
    type SpatialBounds = SpatialPartition2D;

    async fn query<'b>(
        &'b self,
        query: RasterQueryRectangle,
        ctx: &'b dyn QueryContext,
    ) -> Result<BoxStream<'b, Result<Self::Output>>> {
        let stream = self.query_processor.raster_query(query, ctx).await?;
        let converted_stream = stream.and_then(move |tile| {
            crate::util::spawn_blocking_with_thread_pool(ctx.thread_pool().clone(), || {
                tile.convert_data_type_parallel()
            })
            .map_err(Into::into)
        });

        Ok(converted_stream.boxed())
    }
}
