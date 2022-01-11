use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use futures::{stream::BoxStream, try_join, StreamExt, TryStreamExt};
use geoengine_datatypes::{
    primitives::{SpatialPartition2D, TimeInterval},
    raster::{
        GeoTransform, Grid2D, GridIdx2D, GridShape2D, GridShapeAccess, NoDataValue, Pixel,
        RasterTile2D,
    },
};
use num_traits::AsPrimitive;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::{
    engine::{BoxRasterQueryProcessor, QueryContext, QueryProcessor, RasterQueryRectangle},
    util::{stream_zip::StreamTupleZip, Result},
};

use super::compiled::LinkedExpression;

pub struct ExpressionQueryProcessor<TO, Sources>
where
    TO: Pixel,
{
    pub sources: Sources,
    pub phantom_data: PhantomData<TO>,
    pub program: Arc<LinkedExpression>,
    pub no_data_value: TO,
    pub map_no_data: bool,
}

impl<TO, Sources> ExpressionQueryProcessor<TO, Sources>
where
    TO: Pixel,
{
    pub fn new(
        program: LinkedExpression,
        sources: Sources,
        no_data_value: TO,
        map_no_data: bool,
    ) -> Self {
        Self {
            sources,
            program: Arc::new(program),
            phantom_data: PhantomData::default(),
            no_data_value,
            map_no_data,
        }
    }
}

#[async_trait]
impl<'a, TO, Tuple> QueryProcessor for ExpressionQueryProcessor<TO, Tuple>
where
    TO: Pixel,
    Tuple: ExpressionTupleProcessor<TO>,
{
    type Output = RasterTile2D<TO>;
    type SpatialBounds = SpatialPartition2D;

    async fn query<'b>(
        &'b self,
        query: RasterQueryRectangle,
        ctx: &'b dyn QueryContext,
    ) -> Result<BoxStream<'b, Result<Self::Output>>> {
        let stream = self
            .sources
            .queries(query, ctx)
            .await?
            .and_then(move |rasters| async move {
                if Tuple::all_empty(&rasters) {
                    return Ok(Tuple::empty_raster(&rasters));
                }

                let (out_time, out_tile_position, out_global_geo_transform, output_grid_shape) =
                    Tuple::metadata(&rasters);

                let out_no_data = self.no_data_value;

                let thread_pool = ctx.thread_pool().clone();
                let program = self.program.clone();
                let map_no_data = self.map_no_data;

                let data = tokio::task::spawn_blocking(move || {
                    thread_pool.install(move || {
                        Tuple::compute_expression(rasters, &program, map_no_data, out_no_data)
                    })
                })
                .await??;

                let out =
                    Grid2D::<TO>::new(output_grid_shape, data, Some(self.no_data_value))?.into();

                Ok(RasterTile2D::new(
                    out_time,
                    out_tile_position,
                    out_global_geo_transform,
                    out,
                ))
            });

        Ok(stream.boxed())
    }
}

#[async_trait]
trait ExpressionTupleProcessor<TO: Pixel>: Send + Sync {
    type Tuple: Send + 'static;

    async fn queries<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Tuple>>>;

    fn all_empty(tuple: &Self::Tuple) -> bool;

    fn empty_raster(tuple: &Self::Tuple) -> RasterTile2D<TO>;

    fn metadata(tuple: &Self::Tuple) -> (TimeInterval, GridIdx2D, GeoTransform, GridShape2D);

    fn compute_expression(
        tuple: Self::Tuple,
        program: &LinkedExpression,
        map_no_data: bool,
        out_no_data: TO,
    ) -> Result<Vec<TO>>;
}

#[async_trait]
impl<TO, T1> ExpressionTupleProcessor<TO> for BoxRasterQueryProcessor<T1>
where
    TO: Pixel,
    T1: Pixel + AsPrimitive<TO>,
{
    type Tuple = RasterTile2D<T1>;

    #[inline]
    async fn queries<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Tuple>>> {
        let stream = self.query(query, ctx).await?;

        Ok(stream.boxed())
    }

    #[inline]
    fn all_empty(tuple: &Self::Tuple) -> bool {
        tuple.grid_array.is_empty()
    }

    #[inline]
    fn empty_raster(tuple: &Self::Tuple) -> RasterTile2D<TO> {
        tuple.clone().convert()
    }

    #[inline]
    fn metadata(tuple: &Self::Tuple) -> (TimeInterval, GridIdx2D, GeoTransform, GridShape2D) {
        let raster = &tuple;

        (
            raster.time,
            raster.tile_position,
            raster.global_geo_transform,
            raster.grid_shape(),
        )
    }

    #[inline]
    fn compute_expression(
        raster: Self::Tuple,
        program: &LinkedExpression,
        map_no_data: bool,
        out_no_data: TO,
    ) -> Result<Vec<TO>> {
        let expression = unsafe {
            // we have to "trust" that the function has the signature we expect
            program.unary_function()?
        };

        // cannot be empty at this point
        let tile = raster.into_materialized_tile();

        let data = tile
            .grid_array
            .data
            .par_iter()
            .map(|a| {
                let is_no_data = tile.is_no_data(*a);

                if !map_no_data && is_no_data {
                    return out_no_data;
                }

                let result = expression(a.as_(), is_no_data);
                TO::from_(result)
            })
            .collect();

        Result::<Vec<TO>>::Ok(data)
    }
}

// TODO: implement this via macro for 2-8 sources
#[async_trait]
impl<TO, T1, T2> ExpressionTupleProcessor<TO>
    for (BoxRasterQueryProcessor<T1>, BoxRasterQueryProcessor<T2>)
where
    TO: Pixel,
    T1: Pixel + AsPrimitive<TO>,
    T2: Pixel,
{
    type Tuple = (RasterTile2D<T1>, RasterTile2D<T2>);

    #[inline]
    async fn queries<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Tuple>>> {
        // TODO: tile alignment

        let queries = try_join!(self.0.query(query, ctx), self.1.query(query, ctx))?;

        let stream = StreamTupleZip::new(queries).map(|rasters| Ok((rasters.0?, rasters.1?)));

        Ok(stream.boxed())
    }

    #[inline]
    fn all_empty(tuple: &Self::Tuple) -> bool {
        tuple.0.grid_array.is_empty() && tuple.1.grid_array.is_empty()
    }

    #[inline]
    fn empty_raster(tuple: &Self::Tuple) -> RasterTile2D<TO> {
        tuple.0.clone().convert()
    }

    #[inline]
    fn metadata(tuple: &Self::Tuple) -> (TimeInterval, GridIdx2D, GeoTransform, GridShape2D) {
        let raster = &tuple.0;

        (
            raster.time,
            raster.tile_position,
            raster.global_geo_transform,
            raster.grid_shape(),
        )
    }

    #[inline]
    fn compute_expression(
        rasters: Self::Tuple,
        program: &LinkedExpression,
        map_no_data: bool,
        out_no_data: TO,
    ) -> Result<Vec<TO>> {
        let expression = unsafe {
            // we have to "trust" that the function has the signature we expect
            program.binary_function()?
        };

        // TODO: allow iterating over empty rasters
        let tile_0 = rasters.0.into_materialized_tile();
        let tile_1 = rasters.1.into_materialized_tile();

        let data = (&tile_0.grid_array.data, &tile_1.grid_array.data)
            .into_par_iter()
            .map(|(a, b)| {
                let is_a_no_data = tile_0.is_no_data(*a);
                let is_b_no_data = tile_1.is_no_data(*b);

                if !map_no_data && (is_a_no_data || is_b_no_data) {
                    return out_no_data;
                }

                let result = expression(a.as_(), is_a_no_data, b.as_(), is_b_no_data);
                TO::from_(result)
            })
            .collect();

        Result::<Vec<TO>>::Ok(data)
    }
}

#[async_trait]
impl<TO, T1, T2, T3> ExpressionTupleProcessor<TO>
    for (
        BoxRasterQueryProcessor<T1>,
        BoxRasterQueryProcessor<T2>,
        BoxRasterQueryProcessor<T3>,
    )
where
    TO: Pixel,
    T1: Pixel + AsPrimitive<TO>,
    T2: Pixel,
    T3: Pixel,
{
    type Tuple = (RasterTile2D<T1>, RasterTile2D<T2>, RasterTile2D<T3>);

    #[inline]
    async fn queries<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Tuple>>> {
        // TODO: tile alignment

        let queries = try_join!(
            self.0.query(query, ctx),
            self.1.query(query, ctx),
            self.2.query(query, ctx)
        )?;

        let stream =
            StreamTupleZip::new(queries).map(|rasters| Ok((rasters.0?, rasters.1?, rasters.2?)));

        Ok(stream.boxed())
    }

    #[inline]
    fn all_empty(tuple: &Self::Tuple) -> bool {
        tuple.0.grid_array.is_empty()
            && tuple.1.grid_array.is_empty()
            && tuple.2.grid_array.is_empty()
    }

    #[inline]
    fn empty_raster(tuple: &Self::Tuple) -> RasterTile2D<TO> {
        tuple.0.clone().convert()
    }

    #[inline]
    fn metadata(tuple: &Self::Tuple) -> (TimeInterval, GridIdx2D, GeoTransform, GridShape2D) {
        let raster = &tuple.0;

        (
            raster.time,
            raster.tile_position,
            raster.global_geo_transform,
            raster.grid_shape(),
        )
    }

    fn compute_expression(
        rasters: Self::Tuple,
        program: &LinkedExpression,
        map_no_data: bool,
        out_no_data: TO,
    ) -> Result<Vec<TO>> {
        let expression = unsafe {
            // we have to "trust" that the function has the signature we expect
            program.function_3ary()?
        };

        // TODO: allow iterating over empty rasters
        let tile_0 = rasters.0.into_materialized_tile();
        let tile_1 = rasters.1.into_materialized_tile();
        let tile_2 = rasters.2.into_materialized_tile();

        let data = (
            &tile_0.grid_array.data,
            &tile_1.grid_array.data,
            &tile_2.grid_array.data,
        )
            .into_par_iter()
            .map(|(a, b, c)| {
                let is_a_no_data = tile_0.is_no_data(*a);
                let is_b_no_data = tile_1.is_no_data(*b);
                let is_c_no_data = tile_2.is_no_data(*c);

                if !map_no_data && (is_a_no_data || is_b_no_data || is_c_no_data) {
                    return out_no_data;
                }

                let result = expression(
                    a.as_(),
                    is_a_no_data,
                    b.as_(),
                    is_b_no_data,
                    c.as_(),
                    is_c_no_data,
                );
                TO::from_(result)
            })
            .collect();

        Result::<Vec<TO>>::Ok(data)
    }
}
