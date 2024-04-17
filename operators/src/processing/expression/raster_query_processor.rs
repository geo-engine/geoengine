use super::RasterExpressionError;
use crate::{
    engine::{BoxRasterQueryProcessor, QueryContext, QueryProcessor, RasterResultDescriptor},
    util::Result,
};
use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use geoengine_datatypes::{
    primitives::{
        BandSelection, CacheHint, RasterQueryRectangle, RasterSpatialQueryRectangle, TimeInterval,
    },
    raster::{
        ConvertDataType, FromIndexFnParallel, GeoTransform, GridIdx2D, GridIndexAccess,
        GridOrEmpty, GridOrEmpty2D, GridShape2D, GridShapeAccess, MapElementsParallel, Pixel,
        RasterTile2D,
    },
};
use geoengine_expression::LinkedExpression;
use libloading::Symbol;
use num_traits::AsPrimitive;
use std::{marker::PhantomData, sync::Arc};

pub struct ExpressionInput<const N: usize> {
    pub raster: BoxRasterQueryProcessor<f64>,
}

pub struct ExpressionQueryProcessor<TO, Sources>
where
    TO: Pixel,
{
    pub sources: Sources,
    pub result_descriptor: RasterResultDescriptor,
    pub phantom_data: PhantomData<TO>,
    pub program: Arc<LinkedExpression>,
    pub map_no_data: bool,
}

impl<TO, Sources> ExpressionQueryProcessor<TO, Sources>
where
    TO: Pixel,
{
    pub fn new(
        program: LinkedExpression,
        sources: Sources,
        result_descriptor: RasterResultDescriptor,
        map_no_data: bool,
    ) -> Self {
        Self {
            sources,
            result_descriptor,
            program: Arc::new(program),
            phantom_data: PhantomData,
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
    type SpatialQuery = RasterSpatialQueryRectangle;
    type Selection = BandSelection;
    type ResultDescription = RasterResultDescriptor;

    async fn _query<'b>(
        &'b self,
        query: RasterQueryRectangle,
        ctx: &'b dyn QueryContext,
    ) -> Result<BoxStream<'b, Result<Self::Output>>> {
        // rewrite query to request all input bands from the source. They are all combined in the single output band by means of the expression.
        let source_query = RasterQueryRectangle {
            spatial_query: query.spatial_query,
            time_interval: query.time_interval,
            attributes: BandSelection::first_n(Tuple::num_bands()),
        };

        let stream =
            self.sources
                .zip_bands(source_query, ctx)
                .await?
                .and_then(move |rasters| async move {
                    if Tuple::all_empty(&rasters) {
                        return Ok(Tuple::empty_raster(&rasters));
                    }

                    let (
                        out_time,
                        out_tile_position,
                        out_global_geo_transform,
                        _output_grid_shape,
                        cache_hint,
                    ) = Tuple::metadata(&rasters);

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
                        0,
                        out_global_geo_transform,
                        out,
                        cache_hint,
                    ))
                });

        Ok(stream.boxed())
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

#[async_trait]
trait ExpressionTupleProcessor<TO: Pixel>: Send + Sync {
    type Tuple: Send + 'static;

    async fn zip_bands<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Tuple>>>;

    fn all_empty(tuple: &Self::Tuple) -> bool;

    fn empty_raster(tuple: &Self::Tuple) -> RasterTile2D<TO>;

    fn metadata(
        tuple: &Self::Tuple,
    ) -> (
        TimeInterval,
        GridIdx2D,
        GeoTransform,
        GridShape2D,
        CacheHint,
    );

    fn compute_expression(
        tuple: Self::Tuple,
        program: &LinkedExpression,
        map_no_data: bool,
    ) -> Result<GridOrEmpty2D<TO>>;

    fn num_bands() -> u32;
}

#[async_trait]
impl<TO> ExpressionTupleProcessor<TO> for ExpressionInput<1>
where
    TO: Pixel,
    f64: AsPrimitive<TO>,
{
    type Tuple = RasterTile2D<f64>;

    #[inline]
    async fn zip_bands<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Tuple>>> {
        self.raster.query(query, ctx).await
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
    fn metadata(
        tuple: &Self::Tuple,
    ) -> (
        TimeInterval,
        GridIdx2D,
        GeoTransform,
        GridShape2D,
        CacheHint,
    ) {
        let raster = &tuple;

        (
            raster.time,
            raster.tile_position,
            raster.global_geo_transform,
            raster.grid_shape(),
            raster.cache_hint,
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
            program
                .function_1::<Option<f64>>()
                .map_err(RasterExpressionError::from)?
        };

        let map_fn = |in_value: Option<f64>| {
            // TODO: could be a |in_value: T1| if map no data is false!
            if !map_no_data && in_value.is_none() {
                return None;
            }

            let result = expression(in_value);

            result.map(TO::from_)
        };

        let res = raster.grid_array.map_elements_parallel(map_fn);

        Result::Ok(res)
    }

    fn num_bands() -> u32 {
        1
    }
}

// TODO: implement this via macro for 2-8 sources
#[async_trait]
impl<TO> ExpressionTupleProcessor<TO> for ExpressionInput<2>
where
    TO: Pixel,
    f64: AsPrimitive<TO>,
{
    type Tuple = (RasterTile2D<f64>, RasterTile2D<f64>);

    #[inline]
    async fn zip_bands<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Tuple>>> {
        // chunk up the stream to get all bands for a spatial tile at once
        let stream = self.raster.query(query, ctx).await?.chunks(2).map(|chunk| {
            if chunk.len() != 2 {
                // if there are not exactly two tiles, it should mean the last tile was an error and the chunker ended prematurely
                if let Some(Err(e)) = chunk.into_iter().last() {
                    return Err(e);
                }
                // if there is no error, the source did not produce all bands, which likely means a bug in an operator
                unreachable!("the source did not produce all bands");
            }

            let [a, b]: [Result<RasterTile2D<f64>>; 2] = chunk
                .try_into()
                .expect("all chunks should be of length 2 because it was checked above");
            match (a, b) {
                (Ok(t0), Ok(t1)) => Ok((t0, t1)),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        });

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
    fn metadata(
        tuple: &Self::Tuple,
    ) -> (
        TimeInterval,
        GridIdx2D,
        GeoTransform,
        GridShape2D,
        CacheHint,
    ) {
        let raster = &tuple.0;

        (
            raster.time,
            raster.tile_position,
            raster.global_geo_transform,
            raster.grid_shape(),
            tuple.0.cache_hint.merged(&tuple.1.cache_hint),
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
            program
                .function_2::<Option<f64>, Option<f64>>()
                .map_err(RasterExpressionError::from)?
        };

        let map_fn = |lin_idx: usize| {
            let t0_value = rasters.0.get_at_grid_index_unchecked(lin_idx);
            let t1_value = rasters.1.get_at_grid_index_unchecked(lin_idx);

            if !map_no_data && (t0_value.is_none() || t1_value.is_none()) {
                return None;
            }

            let result = expression(t0_value, t1_value);

            result.map(TO::from_)
        };

        let grid_shape = rasters.0.grid_shape();
        let out = GridOrEmpty::from_index_fn_parallel(&grid_shape, map_fn);

        Result::Ok(out)
    }

    fn num_bands() -> u32 {
        2
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
        impl<TO> ExpressionTupleProcessor<TO> for ExpressionInput<$N>
        where
            TO: Pixel,
            f64: AsPrimitive<TO>,
        {
            type Tuple = [RasterTile2D<f64>; $N];

            #[inline]
            async fn zip_bands<'a>(
                &'a self,
                query: RasterQueryRectangle,
                ctx: &'a dyn QueryContext,
            ) -> Result<BoxStream<'a, Result<Self::Tuple>>> {
                // chunk up the stream to get all bands for a spatial tile at once
                let stream = self.raster.query(query, ctx).await?.chunks($N).map(|chunk| {
                    if chunk.len() != $N {
                        // if there are not exactly N tiles, it should mean the last tile was an error and the chunker ended prematurely
                        if let Some(Err(e)) = chunk.into_iter().last() {
                            return Err(e);
                        }
                        // if there is no error, the source did not produce all bands, which likely means a bug in an operator
                        unreachable!("the source did not produce all bands");
                    }

                    let mut ok_tiles = Vec::with_capacity($N);

                    for tile in chunk {
                        match tile {
                            Ok(tile) => ok_tiles.push(tile),
                            Err(e) => return Err(e),
                        }
                    }

                    let tuple: [RasterTile2D<f64>; $N] = ok_tiles
                        .try_into()
                        .expect("all chunks should be of the expected langth because it was checked above");

                    Ok(tuple)
                });

                Ok(stream.boxed())
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
            fn metadata(tuple: &Self::Tuple) -> (TimeInterval, GridIdx2D, GeoTransform, GridShape2D, CacheHint) {
                let raster = &tuple[0];

                (
                    raster.time,
                    raster.tile_position,
                    raster.global_geo_transform,
                    raster.grid_shape(),
                    tuple.iter().fold(CacheHint::max_duration(), |acc, r| acc.merged(&r.cache_hint)),
                )
            }

            fn compute_expression(
                rasters: Self::Tuple,
                program: &LinkedExpression,
                map_no_data: bool,
            ) -> Result<GridOrEmpty2D<TO>> {
                let expression: Symbol<$FN_T> = unsafe {
                    // we have to "trust" that the function has the signature we expect
                    program.function_nary().map_err(RasterExpressionError::from)?
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
                            $PIXEL
                        ),*
                    );

                    result.map(TO::from_)
                };

                let grid_shape = rasters[0].grid_shape();
                let out = GridOrEmpty::from_index_fn_parallel(&grid_shape, map_fn);

                Result::Ok(out)
            }

            fn num_bands() -> u32 {
                $N
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
