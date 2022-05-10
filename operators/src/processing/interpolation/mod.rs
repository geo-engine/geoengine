use std::marker::PhantomData;

use crate::engine::{
    ExecutionContext, InitializedRasterOperator, Operator, QueryContext, QueryProcessor,
    RasterOperator, RasterQueryProcessor, RasterResultDescriptor, SingleRasterSource,
    TypedRasterQueryProcessor,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, RasterQueryRectangle, SpatialPartition2D, SpatialPartitioned,
    SpatialResolution,
};
use geoengine_datatypes::raster::{
    EmptyGrid2D, Grid2D, GridIndexAccess, GridIndexAccessMut, GridOrEmpty, GridSize,
    MaterializedRasterTile2D, Pixel, RasterTile2D, TilingSpecification,
};
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InterpolationParams {
    pub interpolation: InterpolationMethod,
    pub input_resolution: SpatialResolution,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum InterpolationMethod {
    NearestNeighbor,
    BiLinear,
}

pub type Interpolation = Operator<InterpolationParams, SingleRasterSource>;

#[typetag::serde]
#[async_trait]
impl RasterOperator for Interpolation {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let raster_source = self.sources.raster.initialize(context).await?;

        let initialized_operator = InitializedInterpolation {
            result_descriptor: raster_source.result_descriptor().clone(),
            raster_source,
            params: self.params,
            tiling_specification: context.tiling_specification(),
        };

        Ok(initialized_operator.boxed())
    }
}

pub struct InitializedInterpolation {
    result_descriptor: RasterResultDescriptor,
    raster_source: Box<dyn InitializedRasterOperator>,
    params: InterpolationParams,
    tiling_specification: TilingSpecification,
}

impl InitializedRasterOperator for InitializedInterpolation {
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source_processor = self.raster_source.query_processor()?;

        let res = call_on_generic_raster_processor!(
            source_processor, p => match self.params.interpolation  {
                InterpolationMethod::NearestNeighbor => InterploationProcessor::<_,_, NearestNeighbor>::new(
                        p,
                        self.params.clone(),
                        self.tiling_specification,
                        self.result_descriptor.no_data_value.unwrap().as_(),
                    ).boxed()
                    .into(),
                InterpolationMethod::BiLinear => todo!(),
            }
        );

        Ok(res)
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

pub struct InterploationProcessor<Q, P, I>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
    I: InterpolationAlgorithm<P>,
{
    source: Q,
    params: InterpolationParams,
    tiling_specification: TilingSpecification,
    no_data_value: P,
    interpolation: PhantomData<I>,
}

impl<Q, P, I> InterploationProcessor<Q, P, I>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
    I: InterpolationAlgorithm<P>,
{
    pub fn new(
        source: Q,
        params: InterpolationParams,
        tiling_specification: TilingSpecification,
        no_data_value: P,
    ) -> Self {
        Self {
            source,
            params,
            tiling_specification,
            no_data_value,
            interpolation: PhantomData,
        }
    }
}

#[async_trait]
impl<Q, P, I> QueryProcessor for InterploationProcessor<Q, P, I>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
    I: InterpolationAlgorithm<P>,
{
    type Output = RasterTile2D<P>;
    type SpatialBounds = SpatialPartition2D;

    async fn query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        // TODO: check input resolution < output resolution

        // TODO: sparse tile fill adapter?

        // TODO: handle multiple time steps

        let tile_iter = self
            .tiling_specification
            .strategy(query.spatial_resolution.x, query.spatial_resolution.y)
            .tile_information_iterator(query.spatial_bounds);

        let stream = stream::iter(tile_iter)
            .then(move |tile| async move {
                let source_query = RasterQueryRectangle {
                    spatial_bounds: query.spatial_bounds,
                    time_interval: query.time_interval,
                    spatial_resolution: self.params.input_resolution,
                };

                // as input  resolution is smaller than output resolution we only need one tile to fill the output
                let input_tile = self
                    .source
                    .raster_query(source_query, ctx)
                    .await?
                    .next()
                    .await;

                let output_tile = match input_tile {
                    Some(Ok(input_tile)) => {
                        let mut output_tile = RasterTile2D::new_with_tile_info(
                            query.time_interval,
                            tile,
                            Grid2D::new(
                                tile.tile_size_in_pixels,
                                vec![self.no_data_value; 1],
                                Some(self.no_data_value),
                            )?
                            .into(),
                        )
                        .into_materialized_tile();

                        I::interpolate(&input_tile, &mut output_tile)?;

                        output_tile.into()
                    }
                    Some(Err(e)) => {
                        return Err(e);
                    }
                    None => RasterTile2D::new_with_tile_info(
                        query.time_interval,
                        tile,
                        GridOrEmpty::Empty(EmptyGrid2D::new(
                            tile.tile_size_in_pixels,
                            self.no_data_value,
                        )),
                    ),
                };

                Ok(output_tile)
            })
            .boxed();

        Ok(stream)
    }
}

pub trait InterpolationAlgorithm<P: Pixel>: Send + Sync {
    fn interpolate(input: &RasterTile2D<P>, output: &mut MaterializedRasterTile2D<P>)
        -> Result<()>;
}

pub struct NearestNeighbor {}

impl<P> InterpolationAlgorithm<P> for NearestNeighbor
where
    P: Pixel,
{
    fn interpolate(
        input: &RasterTile2D<P>,
        output: &mut MaterializedRasterTile2D<P>,
    ) -> Result<()> {
        let info_in = input.tile_information();
        let in_origin = info_in.spatial_partition().upper_left();
        let in_x_size = info_in.global_geo_transform.x_pixel_size();
        let in_y_size = info_in.global_geo_transform.y_pixel_size();

        let info_out = output.tile_information();
        let out_origin = info_in.spatial_partition().upper_left();
        let out_x_size = info_out.global_geo_transform.x_pixel_size();
        let out_y_size = info_out.global_geo_transform.y_pixel_size();

        let x_offset = ((out_origin.x - in_origin.x) / in_x_size).floor() as isize;
        let y_offset = ((out_origin.y - in_origin.y) / in_y_size).floor() as isize;

        for y in 0..output.grid_array.axis_size_y() {
            let out_y_coord = out_origin.y + y as f64 * out_y_size;
            let nearest_in_y_idx =
                y_offset + ((out_y_coord - in_origin.y) / in_y_size).round() as isize;

            for x in 0..output.grid_array.axis_size_x() {
                let out_x_coord = out_origin.x + x as f64 * out_x_size;
                let nearest_in_x_idx =
                    x_offset + ((out_x_coord - in_origin.x) / in_x_size).round() as isize;

                let value = input.get_at_grid_index([nearest_in_y_idx, nearest_in_x_idx])?;

                output.set_at_grid_index([y as isize, x as isize], value)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {}
