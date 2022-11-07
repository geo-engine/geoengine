use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use geoengine_datatypes::{
    primitives::{RasterQueryRectangle, SpatialPartition2D, TimeInterval},
    raster::{
        ConvertDataType, FromIndexFnParallel, GeoTransform, GridIdx2D, GridIndexAccess,
        GridOrEmpty, GridOrEmpty2D, GridShape2D, GridShapeAccess, MapElementsParallel, Pixel,
        RasterTile2D,
    },
};
use libloading::Symbol;
use num_traits::AsPrimitive;

use crate::{
    adapters::{QueryWrapper, RasterArrayTimeAdapter, RasterTimeAdapter},
    engine::{BoxRasterQueryProcessor, QueryContext, QueryProcessor},
    util::Result,
};

use super::compiled::LinkedExpression;

pub struct ExpressionQueryProcessor<TO, Sources>
where
    TO: Pixel,
{
    pub sources: Sources,
    pub phantom_data: PhantomData<TO>,
    pub program: Arc<LinkedExpression>,
    pub map_no_data: bool,
}

impl<TO, Sources> ExpressionQueryProcessor<TO, Sources>
where
    TO: Pixel,
{
    pub fn new(program: LinkedExpression, sources: Sources, map_no_data: bool) -> Self {
        Self {
            sources,
            program: Arc::new(program),
            phantom_data: PhantomData::default(),
            map_no_data,
        }
    }
}

#[async_trait]
impl<TO, Tuple> QueryProcessor for ExpressionQueryProcessor<TO, Tuple>
where
    TO: Pixel,
    Tuple: ExpressionTupleProcessor<TO>,
{
    type Output = RasterTile2D<TO>;
    type SpatialBounds = SpatialPartition2D;

    async fn _query<'b>(
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

                let (out_time, out_tile_position, out_global_geo_transform, _output_grid_shape) =
                    Tuple::metadata(&rasters);

                let program = self.program.clone();
                let map_no_data = self.map_no_data;

                let out = crate::util::spawn_blocking_with_thread_pool(
                    ctx.thread_pool().clone(),
                    move || Tuple::compute_expression(rasters, &program, map_no_data),
                )
                .await??;

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
    ) -> Result<GridOrEmpty2D<TO>>;
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
    ) -> Result<GridOrEmpty2D<TO>> {
        let expression = unsafe {
            // we have to "trust" that the function has the signature we expect
            program.function_1::<Option<f64>>()?
        };

        let map_fn = |in_value: Option<T1>| {
            // TODO: could be a |in_value: T1| if map no data is false!
            if !map_no_data && in_value.is_none() {
                return None;
            }

            let result = expression(in_value.map(AsPrimitive::as_));

            result.map(TO::from_)
        };

        let res = raster.grid_array.map_elements_parallel(map_fn);

        Result::Ok(res)
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
        let source_a = QueryWrapper { p: &self.0, ctx };

        let source_b = QueryWrapper { p: &self.1, ctx };

        Ok(Box::pin(RasterTimeAdapter::new(source_a, source_b, query)))
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
    ) -> Result<GridOrEmpty2D<TO>> {
        let expression = unsafe {
            // we have to "trust" that the function has the signature we expect
            program.function_2::<Option<f64>, Option<f64>>()?
        };

        let map_fn = |lin_idx: usize| {
            let t0_value = rasters.0.get_at_grid_index_unchecked(lin_idx);
            let t1_value = rasters.1.get_at_grid_index_unchecked(lin_idx);

            if !map_no_data && (t0_value.is_none() || t1_value.is_none()) {
                return None;
            }

            let result = expression(
                t0_value.map(AsPrimitive::as_),
                t1_value.map(AsPrimitive::as_),
            );

            result.map(TO::from_)
        };

        let grid_shape = rasters.0.grid_shape();
        let out = GridOrEmpty::from_index_fn_parallel(&grid_shape, map_fn);

        Result::Ok(out)
    }
}

type Function3 = fn(Option<f64>, Option<f64>, Option<f64>) -> Option<f64>;
type Function4 = fn(Option<f64>, Option<f64>, Option<f64>, Option<f64>) -> Option<f64>;
type Function5 = fn(Option<f64>, Option<f64>, Option<f64>, Option<f64>, Option<f64>) -> Option<f64>;
type Function6 =
    fn(Option<f64>, Option<f64>, Option<f64>, Option<f64>, Option<f64>, Option<f64>) -> Option<f64>;
type Function7 = fn(
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
) -> Option<f64>;
type Function8 = fn(
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
) -> Option<f64>;

macro_rules! impl_expression_tuple_processor {
    ( $i:tt => $( $x:tt ),+ ) => {
        paste::paste! {
            impl_expression_tuple_processor!(
                @inner
                $i
                |
                $( $x ),*
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
    (@inner $N:tt | $( $I:tt ),+ | $( $PIXEL:tt ),+ | $( $IS_NODATA:tt ),+ | $FN_T:ty ) => {
        #[async_trait]
        impl<TO, T1> ExpressionTupleProcessor<TO> for [BoxRasterQueryProcessor<T1>; $N]
        where
            TO: Pixel,
            T1 : Pixel + AsPrimitive<TO>
        {
            type Tuple = [RasterTile2D<T1>; $N];

            #[inline]
            async fn queries<'a>(
                &'a self,
                query: RasterQueryRectangle,
                ctx: &'a dyn QueryContext,
            ) -> Result<BoxStream<'a, Result<Self::Tuple>>> {
                let sources = [$( QueryWrapper { p: &self[$I], ctx } ),*];

                Ok(Box::pin(RasterArrayTimeAdapter::new(sources, query)))
            }

            #[inline]
            fn all_empty(tuple: &Self::Tuple) -> bool {
                $( tuple[$I].grid_array.is_empty() )&&*
            }

            #[inline]
            fn empty_raster(tuple: &Self::Tuple) -> RasterTile2D<TO> {
                tuple[0].clone().convert_data_type()
            }

            #[inline]
            fn metadata(tuple: &Self::Tuple) -> (TimeInterval, GridIdx2D, GeoTransform, GridShape2D) {
                let raster = &tuple[0];

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
            ) -> Result<GridOrEmpty2D<TO>> {
                let expression: Symbol<$FN_T> = unsafe {
                    // we have to "trust" that the function has the signature we expect
                    program.function_nary()?
                };

                let map_fn = |lin_idx: usize| {
                    $(
                        let $PIXEL = rasters[$I].get_at_grid_index_unchecked(lin_idx);
                        let $IS_NODATA = $PIXEL.is_none();
                    )*

                    if !map_no_data && ( $($IS_NODATA)||* ) {
                        return None;
                    }

                    let result = expression(
                        $(
                            $PIXEL.map(AsPrimitive::as_)
                        ),*
                    );

                    result.map(TO::from_)
                };

                let grid_shape = rasters[0].grid_shape();
                let out = GridOrEmpty::from_index_fn_parallel(&grid_shape, map_fn);

                Result::Ok(out)
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
