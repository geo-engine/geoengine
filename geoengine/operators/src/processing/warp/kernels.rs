//! The resampling methods of [`Warp`](super::Warp) besides nearest neighbour, within the same spatial
//! reference, with the semantics of GDAL's warp kernel (`gdalwarp -r <method>`):
//!
//! - `average`, `mode`, `min` and `max` aggregate the input pixels under the footprint of an output
//!   pixel. An input pixel at the edge of the footprint counts with the fraction it covers in each axis.
//!   If the footprint lies within one input pixel in an axis, that pixel counts fully in that axis.
//!   `average` weights the values by these fractions, `mode` sums the fractions per value and keeps the
//!   value whose sum reaches the maximum first, in row-major order, and `min` and `max` take every
//!   touched pixel.
//! - `bilinear` interpolates between the four input pixel centers around the output pixel center.
//!   Neighbours without data are left out and the weights of the others are renormalised. If the
//!   input pixel under the output pixel center has no data, the output pixel has none either.
//!
//! Input pixels without data are left out; an output pixel without any input pixel has no data.
//! Integer outputs round half up (see [`pixel_from_f64_rounded`]).

use crate::adapters::{
    FoldTileAccu, FoldTileAccuMut, RasterSubQueryAdapter, SubQueryTileAggregator,
};
use crate::engine::{QueryContext, QueryProcessor, RasterQueryProcessor, RasterResultDescriptor};
use crate::util::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use geoengine_datatypes::primitives::{
    BandSelection, CacheHint, RasterQueryRectangle, TimeInterval,
};
use geoengine_datatypes::raster::{
    GeoTransform, Grid, GridBoundingBox2D, GridBounds, GridIdx2D, GridIndexAccess, GridOrEmpty,
    MapElementsParallel, MaskedGrid, Pixel, RasterTile2D, TileInformation, pixel_from_f64_rounded,
};
use rayon::ThreadPool;
use rayon::prelude::*;
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::sync::Arc;

/// A resampling method of [`Warp`](super::Warp) that computes an output pixel from several input pixels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarpKernel {
    Average,
    Mode,
    Min,
    Max,
    Bilinear,
}

/// GDAL's tolerance for footprint edges that lie on a pixel edge
const EPSILON: f64 = 1e-10;

/// GDAL's minimum sum of the weights of a bilinear interpolation
const MIN_BILINEAR_WEIGHT: f64 = 0.00001;

/// The input pixels of an output pixel along one axis, `start..end`, with the weight of the first and
/// the last one; the ones in between count fully.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Span {
    start: isize,
    end: isize,
    first_weight: f64,
    last_weight: f64,
    /// the input pixel under the output pixel center (`bilinear` only)
    center: isize,
}

impl Span {
    /// The pixels `[min, max)` covers, with GDAL's weights (`GWKAverageOrModeComputeSourceCoords`).
    fn footprint(min: f64, max: f64) -> Self {
        let (min, max) = if min <= max { (min, max) } else { (max, min) };
        let start = (min + EPSILON).floor() as isize;
        let mut end = (max - EPSILON).ceil() as isize;
        if end <= start {
            end = start + 1;
        }
        if end - start == 1 {
            return Self {
                start,
                end,
                first_weight: 1.,
                last_weight: 1.,
                center: start,
            };
        }
        Self {
            start,
            end,
            first_weight: 1. - (min - start as f64),
            last_weight: 1. - (end as f64 - max),
            center: start,
        }
    }

    /// The two pixels around `center` with GDAL's bilinear weights (`GWKBilinearResample4Sample`).
    fn bilinear(center: f64) -> Self {
        let start = (center - 0.5).floor();
        let first_weight = 1.5 - (center - start);
        Self {
            start: start as isize,
            end: start as isize + 2,
            first_weight,
            last_weight: 1. - first_weight,
            center: center.floor() as isize,
        }
    }

    #[inline]
    fn weight(&self, idx: isize) -> f64 {
        if idx == self.start {
            self.first_weight
        } else if idx + 1 == self.end {
            self.last_weight
        } else {
            1.
        }
    }

    /// The part of this span within `min..=max`
    #[inline]
    fn clamped(&self, min: isize, max: isize) -> std::ops::Range<isize> {
        self.start.max(min)..self.end.min(max + 1)
    }
}

/// The spans of the output pixels `min..=max` of one axis, for an axis with `out_origin` and
/// `out_pixel_size` over an input axis with `in_origin` and `in_pixel_size`.
fn spans(
    kernel: WarpKernel,
    (min, max): (isize, isize),
    (out_origin, out_pixel_size): (f64, f64),
    (in_origin, in_pixel_size): (f64, f64),
) -> Vec<Span> {
    let to_input =
        |out_idx: f64| (out_origin + out_idx * out_pixel_size - in_origin) / in_pixel_size;
    (min..=max)
        .map(|idx| {
            let idx = idx as f64;
            match kernel {
                WarpKernel::Bilinear => Span::bilinear(to_input(idx + 0.5)),
                _ => Span::footprint(to_input(idx), to_input(idx + 1.)),
            }
        })
        .collect()
}

/// The spans of the pixels of an output tile in both axes
#[derive(Debug, Clone)]
struct TileSpans {
    y: Vec<Span>,
    x: Vec<Span>,
}

impl TileSpans {
    fn new(
        kernel: WarpKernel,
        out_bounds: &GridBoundingBox2D,
        out_geo_transform: &GeoTransform,
        in_geo_transform: &GeoTransform,
    ) -> Self {
        let [min_y, min_x] = out_bounds.min_index().0;
        let [max_y, max_x] = out_bounds.max_index().0;
        let out_origin = out_geo_transform.origin_coordinate();
        let in_origin = in_geo_transform.origin_coordinate();
        Self {
            y: spans(
                kernel,
                (min_y, max_y),
                (out_origin.y, out_geo_transform.y_pixel_size().get()),
                (in_origin.y, in_geo_transform.y_pixel_size().get()),
            ),
            x: spans(
                kernel,
                (min_x, max_x),
                (out_origin.x, out_geo_transform.x_pixel_size().get()),
                (in_origin.x, in_geo_transform.x_pixel_size().get()),
            ),
        }
    }

    /// The input pixels the output tile needs
    fn input_bounds(&self) -> GridBoundingBox2D {
        let range = |spans: &[Span]| {
            spans
                .iter()
                .fold((isize::MAX, isize::MIN), |(min, max), span| {
                    (min.min(span.start), max.max(span.end - 1))
                })
        };
        let (min_y, max_y) = range(&self.y);
        let (min_x, max_x) = range(&self.x);
        GridBoundingBox2D::new_unchecked([min_y, min_x], [max_y, max_x])
    }
}

/// What an output pixel collected so far
#[derive(Debug, Clone)]
enum PixelState<P> {
    /// the weighted sum and the sum of the weights (`average` and `bilinear`), and whether the input
    /// pixel under the output pixel center has data (`bilinear`)
    Sum { sum: f64, weight: f64, center: bool },
    /// the extreme value so far (`min` and `max`)
    Extreme(Option<P>),
    /// the sum of the weights per value and the row-major position at which it last grew (`mode`)
    Mode(Vec<ModeEntry<P>>),
}

#[derive(Debug, Clone, Copy)]
struct ModeEntry<P> {
    value: P,
    /// `f32` as in GDAL, which makes sums like `1 + 1 + 0.999_999_999_999_996` ties
    weight: f32,
    last_position: (isize, isize),
}

impl<P: Pixel> PixelState<P> {
    fn new(kernel: WarpKernel) -> Self {
        match kernel {
            WarpKernel::Average | WarpKernel::Bilinear => Self::Sum {
                sum: 0.,
                weight: 0.,
                center: false,
            },
            WarpKernel::Min | WarpKernel::Max => Self::Extreme(None),
            WarpKernel::Mode => Self::Mode(Vec::new()),
        }
    }

    /// Adds the input pixel at `position` with `value`; `center` if it is under the output pixel center.
    #[inline]
    fn add(
        &mut self,
        kernel: WarpKernel,
        value: P,
        weight: f64,
        position: (isize, isize),
        center: bool,
    ) {
        match self {
            Self::Sum {
                sum,
                weight: total,
                center: has_center,
            } => {
                *has_center |= center;
                if weight > 0. {
                    let value: f64 = value.as_();
                    *sum += weight * value;
                    *total += weight;
                }
            }
            Self::Extreme(extreme) => {
                let value_f64: f64 = value.as_();
                if value_f64.is_nan() {
                    return;
                }
                let replace = match extreme {
                    None => true,
                    Some(current) if kernel == WarpKernel::Min => value < *current,
                    Some(current) => value > *current,
                };
                if replace {
                    *extreme = Some(value);
                }
            }
            Self::Mode(entries) => {
                if weight <= 0. {
                    return;
                }
                #[allow(clippy::cast_possible_truncation)]
                let weight = weight as f32;
                if let Some(entry) = entries.iter_mut().find(|entry| entry.value == value) {
                    entry.weight += weight;
                    entry.last_position = entry.last_position.max(position);
                } else {
                    entries.push(ModeEntry {
                        value,
                        weight,
                        last_position: position,
                    });
                }
            }
        }
    }

    fn finish(&self, kernel: WarpKernel) -> Option<P> {
        match self {
            Self::Sum {
                sum,
                weight,
                center,
            } => {
                let bilinear = kernel == WarpKernel::Bilinear;
                let minimum = if bilinear { MIN_BILINEAR_WEIGHT } else { 0. };
                if *weight <= minimum || (bilinear && !center) {
                    None
                } else {
                    pixel_from_f64_rounded(sum / weight)
                }
            }
            Self::Extreme(extreme) => *extreme,
            // GDAL updates the mode only if a sum exceeds the maximum so far, so of the values with the
            // largest sum, the one that reached it first wins, i.e. the one whose last pixel comes first
            Self::Mode(entries) => entries
                .iter()
                .reduce(|best, entry| match entry.weight.partial_cmp(&best.weight) {
                    Some(Ordering::Greater) => entry,
                    // an exact tie, as GDAL compares the sums
                    Some(Ordering::Equal) if entry.last_position < best.last_position => entry,
                    _ => best,
                })
                .map(|entry| entry.value),
        }
    }
}

/// Resamples a raster onto another grid in the same spatial reference with a [`WarpKernel`].
pub struct WarpKernelProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    source: Q,
    kernel: WarpKernel,
    out_result_descriptor: RasterResultDescriptor,
}

impl<Q, P> WarpKernelProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    /// `out_result_descriptor` must have the spatial reference and the data type of `source`.
    pub fn new(
        source: Q,
        kernel: WarpKernel,
        out_result_descriptor: RasterResultDescriptor,
    ) -> Self {
        debug_assert_eq!(
            source.raster_result_descriptor().data_type,
            out_result_descriptor.data_type
        );
        Self {
            source,
            kernel,
            out_result_descriptor,
        }
    }
}

#[async_trait]
impl<Q, P> RasterQueryProcessor for WarpKernelProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    type RasterType = P;

    async fn _time_query<'a>(
        &'a self,
        query: TimeInterval,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<TimeInterval>>> {
        self.source.time_query(query, ctx).await
    }
}

#[async_trait]
impl<Q, P> QueryProcessor for WarpKernelProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    type Output = RasterTile2D<P>;
    type SpatialBounds = GridBoundingBox2D;
    type Selection = BandSelection;
    type ResultDescription = RasterResultDescriptor;

    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let in_spatial_grid = self.source.result_descriptor().spatial_grid_descriptor();
        let out_tiling_grid = self
            .out_result_descriptor
            .spatial_grid_descriptor()
            .tiling_grid_definition(ctx.tiling_specification());

        let sub_query = WarpKernelSubQuery::<P> {
            kernel: self.kernel,
            input_geo_transform: in_spatial_grid
                .tiling_grid_definition(ctx.tiling_specification())
                .tiling_geo_transform(),
            output_geo_transform: out_tiling_grid.tiling_geo_transform(),
            _phantom_pixel_type: PhantomData,
        };

        let time_stream = self.time_query(query.time_interval(), ctx).await?;

        Ok(Box::pin(RasterSubQueryAdapter::<'a, P, _, _, _>::new(
            &self.source,
            query,
            out_tiling_grid.generate_data_tiling_strategy(),
            ctx,
            sub_query,
            time_stream,
        )))
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.out_result_descriptor
    }
}

#[derive(Debug, Clone)]
struct WarpKernelSubQuery<P> {
    kernel: WarpKernel,
    input_geo_transform: GeoTransform,
    output_geo_transform: GeoTransform,
    _phantom_pixel_type: PhantomData<P>,
}

impl<P> WarpKernelSubQuery<P> {
    fn tile_spans(&self, tile_info: &TileInformation) -> TileSpans {
        TileSpans::new(
            self.kernel,
            &tile_info.global_pixel_bounds(),
            &self.output_geo_transform,
            &self.input_geo_transform,
        )
    }
}

type FoldFn<P> =
    fn(WarpKernelAccu<P>, RasterTile2D<P>) -> BoxFuture<'static, Result<WarpKernelAccu<P>>>;

impl<'a, P: Pixel> SubQueryTileAggregator<'a, P> for WarpKernelSubQuery<P> {
    type FoldFuture = BoxFuture<'static, Result<WarpKernelAccu<P>>>;
    type FoldMethod = FoldFn<P>;
    type TileAccu = WarpKernelAccu<P>;
    type TileAccuFuture = BoxFuture<'a, Result<Self::TileAccu>>;

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        _query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture {
        let kernel = self.kernel;
        let spans = self.tile_spans(&tile_info);
        let pool = pool.clone();
        crate::util::spawn_blocking(move || WarpKernelAccu::new(kernel, tile_info, spans, pool))
            .map_err(From::from)
            .boxed()
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        _query_rect: RasterQueryRectangle,
        time: TimeInterval,
        band_idx: u32,
    ) -> Result<Option<RasterQueryRectangle>> {
        Ok(Some(RasterQueryRectangle::new(
            self.tile_spans(&tile_info).input_bounds(),
            time,
            BandSelection::new_single(band_idx),
        )))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        |accu, tile| {
            let pool = accu.pool.clone();
            crate::util::spawn_blocking_with_thread_pool(pool, move || accu.fold(&tile))
                .map_err(From::from)
                .boxed()
        }
    }
}

/// The state of all pixels of an output tile
#[derive(Debug, Clone)]
pub struct WarpKernelAccu<P: Pixel> {
    kernel: WarpKernel,
    output_tile_info: TileInformation,
    spans: Arc<TileSpans>,
    /// row-major, one per pixel of the output tile
    pixels: Vec<PixelState<P>>,
    time: Option<TimeInterval>,
    cache_hint: CacheHint,
    pool: Arc<ThreadPool>,
}

impl<P: Pixel> WarpKernelAccu<P> {
    fn new(
        kernel: WarpKernel,
        output_tile_info: TileInformation,
        spans: TileSpans,
        pool: Arc<ThreadPool>,
    ) -> Self {
        let size = spans.x.len() * spans.y.len();
        Self {
            kernel,
            output_tile_info,
            spans: Arc::new(spans),
            pixels: vec![PixelState::new(kernel); size],
            time: None,
            cache_hint: CacheHint::max_duration(),
            pool,
        }
    }

    /// Adds the pixels of `tile` to the output pixels whose spans they are in.
    fn fold(mut self, tile: &RasterTile2D<P>) -> Self {
        let _span = tracing::debug_span!("warp_kernel_fold").entered();

        self.time = Some(tile.time);
        self.cache_hint.merge_with(&tile.cache_hint);

        let GridOrEmpty::Grid(grid) = &tile.grid_array else {
            return self;
        };
        let in_bounds = tile.tile_information().global_pixel_bounds();
        let [in_min_y, in_min_x] = in_bounds.min_index().0;
        let [in_max_y, in_max_x] = in_bounds.max_index().0;
        let [tile_offset_y, tile_offset_x] = in_bounds.min_index().0;

        let kernel = self.kernel;
        let spans = self.spans.clone();
        let width = spans.x.len();
        // the output columns whose spans intersect the tile
        let columns: Vec<(usize, std::ops::Range<isize>)> = spans
            .x
            .iter()
            .enumerate()
            .map(|(column, span)| (column, span.clamped(in_min_x, in_max_x)))
            .filter(|(_, range)| !range.is_empty())
            .collect();
        if columns.is_empty() {
            return self;
        }

        let add_row = |(row, pixels): (usize, &mut [PixelState<P>])| {
            let y_span = spans.y[row];
            for in_y in y_span.clamped(in_min_y, in_max_y) {
                let weight_y = y_span.weight(in_y);
                for (column, x_range) in &columns {
                    let x_span = spans.x[*column];
                    let state = &mut pixels[*column];
                    for in_x in x_range.clone() {
                        let local: GridIdx2D = [in_y - tile_offset_y, in_x - tile_offset_x].into();
                        if let Some(value) = grid.get_at_grid_index_unchecked(local) {
                            let center = in_y == y_span.center && in_x == x_span.center;
                            state.add(
                                kernel,
                                value,
                                weight_y * x_span.weight(in_x),
                                (in_y, in_x),
                                center,
                            );
                        }
                    }
                }
            }
        };

        let pixels = &mut self.pixels;
        self.pool.install(|| {
            pixels
                .par_chunks_mut(width)
                .enumerate()
                .filter(|(row, _)| !spans.y[*row].clamped(in_min_y, in_max_y).is_empty())
                .for_each(add_row);
        });

        self
    }
}

#[async_trait]
impl<P: Pixel> FoldTileAccu for WarpKernelAccu<P> {
    type RasterType = P;

    async fn into_tile(self) -> Result<RasterTile2D<P>> {
        let kernel = self.kernel;
        let shape = self.output_tile_info.tile_size_in_pixels;
        let pixels = self.pixels;
        let pool = self.pool.clone();
        let grid = crate::util::spawn_blocking_with_thread_pool(pool, move || {
            let values: Vec<Option<P>> = pixels
                .par_iter()
                .map(|state| state.finish(kernel))
                .collect();
            if values.iter().all(Option::is_none) {
                return Ok(GridOrEmpty::new_empty_shape(shape));
            }
            let mask = values.iter().map(Option::is_some).collect();
            let data = values
                .into_iter()
                .map(|value| value.unwrap_or_else(P::zero))
                .collect();
            Ok::<_, crate::error::Error>(GridOrEmpty::from(MaskedGrid::new(
                Grid::new(shape, data)?,
                Grid::new(shape, mask)?,
            )?))
        })
        .await??;

        Ok(RasterTile2D::new_with_tile_info(
            self.time.expect("there is at least one input"),
            self.output_tile_info,
            0,
            grid,
            self.cache_hint,
        ))
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

impl<P: Pixel> FoldTileAccuMut for WarpKernelAccu<P> {
    fn set_time(&mut self, time: TimeInterval) {
        self.time = Some(time);
    }

    fn set_cache_hint(&mut self, cache_hint: CacheHint) {
        self.cache_hint = cache_hint;
    }
}

/// Converts the pixels of `source` to `POut` with [`pixel_from_f64_rounded`], as GDAL writes
/// resampled values into another data type.
pub struct RoundingTypeConversionProcessor<Q, PIn, POut> {
    source: Q,
    result_descriptor: RasterResultDescriptor,
    _phantom: PhantomData<(PIn, POut)>,
}

impl<Q, PIn, POut> RoundingTypeConversionProcessor<Q, PIn, POut>
where
    Q: RasterQueryProcessor<RasterType = PIn>,
    PIn: Pixel,
    POut: Pixel,
{
    /// `result_descriptor` is the one of `source` with the data type of `POut`.
    pub fn new(source: Q, result_descriptor: RasterResultDescriptor) -> Self {
        debug_assert_eq!(result_descriptor.data_type, POut::TYPE);
        Self {
            source,
            result_descriptor,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<Q, PIn, POut> QueryProcessor for RoundingTypeConversionProcessor<Q, PIn, POut>
where
    Q: RasterQueryProcessor<RasterType = PIn>,
    PIn: Pixel,
    POut: Pixel,
{
    type Output = RasterTile2D<POut>;
    type SpatialBounds = GridBoundingBox2D;
    type Selection = BandSelection;
    type ResultDescription = RasterResultDescriptor;

    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let stream = self.source.raster_query(query, ctx).await?;
        let pool = ctx.thread_pool().clone();
        Ok(stream
            .and_then(move |tile| {
                crate::util::spawn_blocking_with_thread_pool(pool.clone(), move || {
                    tile.map_elements_parallel(|value: Option<PIn>| {
                        value.and_then(|value| pixel_from_f64_rounded::<POut>(value.as_()))
                    })
                })
                .map_err(From::from)
            })
            .boxed())
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

#[async_trait]
impl<Q, PIn, POut> RasterQueryProcessor for RoundingTypeConversionProcessor<Q, PIn, POut>
where
    Q: RasterQueryProcessor<RasterType = PIn>,
    PIn: Pixel,
    POut: Pixel,
{
    type RasterType = POut;

    async fn _time_query<'a>(
        &'a self,
        query: TimeInterval,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<TimeInterval>>> {
        self.source.time_query(query, ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_computes_footprint_spans_like_gdal() {
        // inside one pixel
        assert_eq!(
            Span::footprint(2.25, 2.75),
            Span {
                start: 2,
                end: 3,
                first_weight: 1.,
                last_weight: 1.,
                center: 2,
            }
        );
        // on pixel edges
        assert_eq!(
            Span::footprint(2., 4.),
            Span {
                start: 2,
                end: 4,
                first_weight: 1.,
                last_weight: 1.,
                center: 2,
            }
        );
        // partly covered edge pixels
        let span = Span::footprint(1.5, 4.25);
        assert_eq!((span.start, span.end), (1, 5));
        assert_eq!(
            (1..5).map(|idx| span.weight(idx)).collect::<Vec<_>>(),
            vec![0.5, 1., 1., 0.25]
        );
    }

    #[test]
    fn it_computes_bilinear_spans_like_gdal() {
        // a quarter pixel right of the center of pixel 2
        let span = Span::bilinear(2.75);
        assert_eq!((span.start, span.end), (2, 4));
        assert_eq!((span.weight(2), span.weight(3)), (0.75, 0.25));
        // on the center of pixel 2
        let span = Span::bilinear(2.5);
        assert_eq!((span.start, span.weight(2), span.weight(3)), (2, 1., 0.));
        // the center pixel
        assert_eq!(
            (Span::bilinear(2.75).center, Span::bilinear(3.25).center),
            (2, 3)
        );
    }

    #[test]
    fn it_picks_the_mode_that_reaches_the_maximum_first() {
        let mut state = PixelState::<u8>::new(WarpKernel::Mode);
        // 1, 2, 2, 1: 2 reaches 2 first
        state.add(WarpKernel::Mode, 1, 1., (0, 0), false);
        state.add(WarpKernel::Mode, 2, 1., (0, 1), false);
        state.add(WarpKernel::Mode, 2, 1., (1, 0), false);
        state.add(WarpKernel::Mode, 1, 1., (1, 1), false);
        assert_eq!(state.finish(WarpKernel::Mode), Some(2));

        // the order of adding does not matter, only the positions
        let mut state = PixelState::<u8>::new(WarpKernel::Mode);
        state.add(WarpKernel::Mode, 1, 1., (1, 1), false);
        state.add(WarpKernel::Mode, 2, 1., (1, 0), false);
        state.add(WarpKernel::Mode, 1, 1., (0, 0), false);
        state.add(WarpKernel::Mode, 2, 1., (0, 1), false);
        assert_eq!(state.finish(WarpKernel::Mode), Some(2));

        // weights count
        let mut state = PixelState::<u8>::new(WarpKernel::Mode);
        state.add(WarpKernel::Mode, 1, 0.25, (0, 0), false);
        state.add(WarpKernel::Mode, 1, 0.25, (0, 1), false);
        state.add(WarpKernel::Mode, 2, 1., (1, 0), false);
        assert_eq!(state.finish(WarpKernel::Mode), Some(2));
    }

    #[test]
    fn it_averages_with_weights_and_rounds() {
        let mut state = PixelState::<u8>::new(WarpKernel::Average);
        state.add(WarpKernel::Average, 1, 0.5, (0, 0), false);
        state.add(WarpKernel::Average, 2, 0.5, (0, 1), false);
        assert_eq!(state.finish(WarpKernel::Average), Some(2));

        let mut state = PixelState::<f32>::new(WarpKernel::Average);
        state.add(WarpKernel::Average, 1., 0.25, (0, 0), false);
        state.add(WarpKernel::Average, 2., 0.75, (0, 1), false);
        assert_eq!(state.finish(WarpKernel::Average), Some(1.75));

        assert_eq!(
            PixelState::<u8>::new(WarpKernel::Average).finish(WarpKernel::Average),
            None
        );
    }
}
