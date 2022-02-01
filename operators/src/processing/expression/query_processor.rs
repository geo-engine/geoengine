use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use futures::{stream::BoxStream, try_join, StreamExt, TryStreamExt};
use geoengine_datatypes::{
    primitives::{RasterQueryRectangle, SpatialPartition2D, TimeInterval},
    raster::{
        ConvertDataType, GeoTransform, Grid2D, GridIdx2D, GridShape2D, GridShapeAccess, GridSize,
        NoDataValue, Pixel, RasterTile2D,
    },
};
use libloading::Symbol;
use num_traits::AsPrimitive;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};

use crate::{
    engine::{BoxRasterQueryProcessor, QueryContext, QueryProcessor},
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
        tuple.clone().convert_data_type()
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
            program.function_3::<f64, bool, f64>()?
        };

        // cannot be empty at this point
        let tile = raster.into_materialized_tile();

        let data = tile
            .grid_array
            .data
            .par_iter()
            .with_min_len(tile.grid_array.grid_shape().axis_size_x())
            .map(|a| {
                let is_no_data = tile.is_no_data(*a);

                if !map_no_data && is_no_data {
                    return out_no_data;
                }

                let result = expression(a.as_(), is_no_data, out_no_data.as_());
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
        tuple.0.clone().convert_data_type()
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
            program.function_5::<f64, bool, f64, bool, f64>()?
        };

        // TODO: allow iterating over empty rasters
        let tile_0 = rasters.0.into_materialized_tile();
        let tile_1 = rasters.1.into_materialized_tile();

        let data = (&tile_0.grid_array.data, &tile_1.grid_array.data)
            .into_par_iter()
            .with_min_len(tile_0.grid_array.grid_shape().axis_size_x())
            .map(|(a, b)| {
                let is_a_no_data = tile_0.is_no_data(*a);
                let is_b_no_data = tile_1.is_no_data(*b);

                if !map_no_data && (is_a_no_data || is_b_no_data) {
                    return out_no_data;
                }

                let result = expression(
                    a.as_(),
                    is_a_no_data,
                    b.as_(),
                    is_b_no_data,
                    out_no_data.as_(),
                );
                TO::from_(result)
            })
            .collect();

        Result::<Vec<TO>>::Ok(data)
    }
}

type Function3 = fn(f64, bool, f64, bool, f64, bool, f64) -> f64;
type Function4 = fn(f64, bool, f64, bool, f64, bool, f64, bool, f64) -> f64;
type Function5 = fn(f64, bool, f64, bool, f64, bool, f64, bool, f64, bool, f64) -> f64;
type Function6 = fn(f64, bool, f64, bool, f64, bool, f64, bool, f64, bool, f64, bool, f64) -> f64;
type Function7 =
    fn(f64, bool, f64, bool, f64, bool, f64, bool, f64, bool, f64, bool, f64, bool, f64) -> f64;
type Function8 = fn(
    f64,
    bool,
    f64,
    bool,
    f64,
    bool,
    f64,
    bool,
    f64,
    bool,
    f64,
    bool,
    f64,
    bool,
    f64,
    bool,
    f64,
) -> f64;

macro_rules! impl_expression_tuple_processor {
    ( $i:tt => $( $x:tt ),+ ) => {
        paste::paste! {
            impl_expression_tuple_processor!(
                @inner
                $( $x ),*
                |
                $( [< T $x >] ),*
                |
                $( [< tile_ $x >] ),*
                |
                $( [< pixel_ $x >] ),*
                |
                $( [< is_nodata_ $x >] ),*
                |
                [< Function $i >]
            );
        }
    };

    // We have `0, 1, 2, …` and `T0, T1, T2, …`
    (@inner $( $I:tt ),+ | $( $T:tt ),+ | $( $TILE:tt ),+ | $( $PIXEL:tt ),+ | $( $IS_NODATA:tt ),+ | $FN_T:ty ) => {
        #[async_trait]
        impl<TO, $($T),*> ExpressionTupleProcessor<TO>
            for (
                $(BoxRasterQueryProcessor<$T>),*
            )
        where
            TO: Pixel,
            $($T : Pixel + AsPrimitive<TO>),*
        {
            type Tuple = ( $(RasterTile2D<$T>),* );

            #[inline]
            async fn queries<'a>(
                &'a self,
                query: RasterQueryRectangle,
                ctx: &'a dyn QueryContext,
            ) -> Result<BoxStream<'a, Result<Self::Tuple>>> {
                // TODO: tile alignment

                let queries = try_join!(
                    $( self.$I.query(query, ctx) ),*
                )?;

                let stream =
                    StreamTupleZip::new(queries).map(|rasters| Ok((
                        $( rasters.$I? ),*
                    )));

                Ok(stream.boxed())
            }

            #[inline]
            fn all_empty(tuple: &Self::Tuple) -> bool {
                $( tuple.$I.grid_array.is_empty() )&&*
            }

            #[inline]
            fn empty_raster(tuple: &Self::Tuple) -> RasterTile2D<TO> {
                tuple.0.clone().convert_data_type()
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
                let expression: Symbol<$FN_T> = unsafe {
                    // we have to "trust" that the function has the signature we expect
                    program.function_nary()?
                };

                let min_batch_size = rasters.0.grid_array.grid_shape().axis_size_x();

                // TODO: allow iterating over empty rasters
                $(
                    let $TILE = rasters.$I.into_materialized_tile();
                )*

                let data = (
                    $(
                        & $TILE.grid_array.data
                    ),*
                )
                    .into_par_iter()
                    .with_min_len(min_batch_size)
                    .map(|( $($PIXEL),* )| {
                        $(
                            let $IS_NODATA = $TILE.is_no_data(* $PIXEL);
                        )*

                        if !map_no_data && ( $($IS_NODATA)||* ) {
                            return out_no_data;
                        }

                        let result = expression(
                            $(
                                $PIXEL.as_(),
                                $IS_NODATA,
                            )*
                            out_no_data.as_(),
                        );
                        TO::from_(result)
                    })
                    .collect();

                Result::<Vec<TO>>::Ok(data)
            }
        }
    };

    // For any input, generate `f64, bool`
    (@input_dtypes $x:tt) => {
        f64, bool
    };
}

impl_expression_tuple_processor!(3 => 0, 1, 2);
impl_expression_tuple_processor!(4 => 0, 1, 2, 3);
impl_expression_tuple_processor!(5 => 0, 1, 2, 3, 4);
impl_expression_tuple_processor!(6 => 0, 1, 2, 3, 4, 5);
impl_expression_tuple_processor!(7 => 0, 1, 2, 3, 4, 5, 6);
impl_expression_tuple_processor!(8 => 0, 1, 2, 3, 4, 5, 6, 7);
