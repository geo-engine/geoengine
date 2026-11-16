//! The resampling methods of [`Warp`](super::Warp) besides nearest neighbour, with the semantics of
//! GDAL's warp kernel (`gdalwarp -r <method>`):
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
//!
//! With another spatial reference, the footprint of an output pixel is the bounding box of its
//! upper left and lower right corner in the input, as in GDAL, and `bilinear` interpolates around
//! its projected center. Output pixels whose corners or center cannot be projected have no data.

use crate::adapters::TileReprojectionSubqueryGridInfo;
use crate::adapters::{
    FoldTileAccu, FoldTileAccuMut, RasterSubQueryAdapter, SubQueryTileAggregator,
};
use crate::engine::{QueryContext, QueryProcessor, RasterQueryProcessor, RasterResultDescriptor};
use crate::util::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use geoengine_datatypes::operations::reproject::Reproject;
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BandSelection, BoundingBox2D, CacheHint, Coordinate2D,
    RasterQueryRectangle, SpatialPartition2D, TimeInterval,
};
use geoengine_datatypes::raster::{
    GeoTransform, Grid, GridBoundingBox2D, GridBounds, GridIdx2D, GridIndexAccess,
    GridIntersection, GridOrEmpty, GridSize, MapElementsParallel, MaskedGrid, Pixel, RasterTile2D,
    TileInformation, pixel_from_f64_rounded,
};
use geoengine_datatypes::spatial_reference::{
    CoordinateProjection, DefaultCoordinateProjector, SpatialReference,
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

/// The spans of the pixels of an output tile
#[derive(Debug, Clone)]
enum TileSpans {
    /// within the same spatial reference: one span per row and one per column
    Separable { y: Vec<Span>, x: Vec<Span> },
    /// with another spatial reference: the spans (y, x) of each pixel, row-major, `None` for pixels
    /// that cannot be projected
    PerPixel {
        width: usize,
        pixels: Vec<Option<(Span, Span)>>,
    },
}

impl TileSpans {
    fn separable(
        kernel: WarpKernel,
        out_bounds: &GridBoundingBox2D,
        out_geo_transform: &GeoTransform,
        in_geo_transform: &GeoTransform,
    ) -> Self {
        let [min_y, min_x] = out_bounds.min_index().0;
        let [max_y, max_x] = out_bounds.max_index().0;
        let out_origin = out_geo_transform.origin_coordinate();
        let in_origin = in_geo_transform.origin_coordinate();
        Self::Separable {
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

    /// The spans of the pixels of the output tile `tile_info` in `projection.out_srs`, projected
    /// into the input grid in `projection.in_srs`: the corners of each pixel for the footprint
    /// methods, its center for `bilinear`.
    fn projected(
        kernel: WarpKernel,
        tile_info: &TileInformation,
        projection: &KernelProjection,
        pool: &ThreadPool,
    ) -> Result<Self> {
        let [min_y, min_x] = tile_info.global_pixel_bounds().min_index().0;
        let [height, width] = [
            tile_info.tile_size_in_pixels.axis_size_y(),
            tile_info.tile_size_in_pixels.axis_size_x(),
        ];
        let out_geo_transform = projection.grids.out_spatial_grid.geo_transform();
        let in_geo_transform = projection.grids.in_spatial_grid.geo_transform();
        let in_origin = in_geo_transform.origin_coordinate();
        let in_pixel_size = (
            in_geo_transform.x_pixel_size().get(),
            in_geo_transform.y_pixel_size().get(),
        );
        let to_input_pixels = |coordinate: Option<Coordinate2D>| {
            coordinate.map(|c| {
                (
                    (c.y - in_origin.y) / in_pixel_size.1,
                    (c.x - in_origin.x) / in_pixel_size.0,
                )
            })
        };
        let coordinate = |y: f64, x: f64| {
            let origin = out_geo_transform.origin_coordinate();
            Coordinate2D::new(
                origin.x + (min_x as f64 + x) * out_geo_transform.x_pixel_size().get(),
                origin.y + (min_y as f64 + y) * out_geo_transform.y_pixel_size().get(),
            )
        };

        let pixels = pool.install(|| {
            if kernel == WarpKernel::Bilinear {
                let centers: Vec<Coordinate2D> = (0..height * width)
                    .into_par_iter()
                    .map(|i| coordinate((i / width) as f64 + 0.5, (i % width) as f64 + 0.5))
                    .collect();
                let centers = projection.project(&centers, pool)?;
                Ok::<_, crate::error::Error>(
                    centers
                        .into_par_iter()
                        .map(|center| {
                            to_input_pixels(center)
                                .map(|(y, x)| (Span::bilinear(y), Span::bilinear(x)))
                        })
                        .collect(),
                )
            } else {
                let corners: Vec<Coordinate2D> = (0..=height)
                    .into_par_iter()
                    .flat_map_iter(|y| (0..=width).map(move |x| coordinate(y as f64, x as f64)))
                    .collect();
                let corners = projection.project(&corners, pool)?;
                let corner = |y: usize, x: usize| to_input_pixels(corners[y * (width + 1) + x]);
                Ok((0..height * width)
                    .into_par_iter()
                    .map(|i| {
                        let (y, x) = (i / width, i % width);
                        // GDAL's footprint: the upper left and the lower right corner
                        let (upper_left, lower_right) = (corner(y, x)?, corner(y + 1, x + 1)?);
                        Some((
                            Span::footprint(upper_left.0, lower_right.0),
                            Span::footprint(upper_left.1, lower_right.1),
                        ))
                    })
                    .collect())
            }
        })?;

        Ok(Self::PerPixel { width, pixels })
    }

    fn width(&self) -> usize {
        match self {
            Self::Separable { x, .. } => x.len(),
            Self::PerPixel { width, .. } => *width,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Separable { y, x } => y.len() * x.len(),
            Self::PerPixel { pixels, .. } => pixels.len(),
        }
    }

    /// The input pixels the output tile needs, within the same spatial reference
    fn separable_input_bounds(y: &[Span], x: &[Span]) -> GridBoundingBox2D {
        let range = |spans: &[Span]| {
            spans
                .iter()
                .fold((isize::MAX, isize::MIN), |(min, max), span| {
                    (min.min(span.start), max.max(span.end - 1))
                })
        };
        let (min_y, max_y) = range(y);
        let (min_x, max_x) = range(x);
        GridBoundingBox2D::new_unchecked([min_y, min_x], [max_y, max_x])
    }
}

/// How the output pixels of a [`WarpKernelProcessor`] with another spatial reference relate to
/// the input pixels
#[derive(Debug, Clone, Copy)]
pub struct KernelProjection {
    pub in_srs: SpatialReference,
    pub out_srs: SpatialReference,
    /// the tiling grids of input and output
    pub grids: TileReprojectionSubqueryGridInfo,
}

impl KernelProjection {
    /// Projects `coordinates` from the output into the input spatial reference, `None` for
    /// coordinates outside of the output grid (as in the nearest-neighbour reprojection) or that
    /// cannot be projected.
    fn project(
        &self,
        coordinates: &[Coordinate2D],
        pool: &ThreadPool,
    ) -> Result<Vec<Option<Coordinate2D>>> {
        const CHUNK_SIZE: usize = 64 * 512;
        let valid_area = self.grids.out_spatial_grid.spatial_partition().as_bbox();
        let mut projected = vec![None; coordinates.len()];
        pool.install(|| {
            projected
                .par_chunks_mut(CHUNK_SIZE)
                .zip(coordinates.par_chunks(CHUNK_SIZE))
                .try_for_each(|(projected, coordinates)| {
                    let Some(bounds) = BoundingBox2D::from_coord_ref_iter(coordinates.iter())
                    else {
                        return Ok(());
                    };
                    let projector =
                        DefaultCoordinateProjector::from_known_srs(self.out_srs, self.in_srs)?;
                    // the whole chunk at once if it lies in the valid area, else coordinate by coordinate
                    if valid_area.contains_bbox(&bounds)
                        && let Ok(coordinates) = projector.project_coordinates(coordinates)
                    {
                        for (projected, coordinate) in projected.iter_mut().zip(coordinates) {
                            *projected = Some(coordinate);
                        }
                        return Ok(());
                    }
                    for (projected, coordinate) in projected.iter_mut().zip(coordinates) {
                        *projected = valid_area
                            .contains_coordinate(coordinate)
                            .then(|| projector.project_coordinate(*coordinate).ok())
                            .flatten();
                    }
                    Result::<(), crate::error::Error>::Ok(())
                })
        })?;
        Ok(projected)
    }

    /// The input pixels the output tile `tile_info` needs within `query_bounds`: the projected
    /// bounds of the tile, with a margin of one pixel for the footprints and the bilinear
    /// neighbours at its edges. `None` if the tile cannot be projected.
    fn input_bounds(
        &self,
        tile_info: &TileInformation,
        query_bounds: &GridBoundingBox2D,
    ) -> Result<Option<GridBoundingBox2D>> {
        let Some(out_bounds) = self
            .grids
            .out_spatial_grid
            .grid_bounds()
            .intersection(&tile_info.global_pixel_bounds())
            .and_then(|bounds| bounds.intersection(query_bounds))
        else {
            return Ok(None);
        };
        let out_partition: SpatialPartition2D = self
            .grids
            .out_spatial_grid
            .geo_transform()
            .grid_to_spatial_bounds(&out_bounds);
        let projector = DefaultCoordinateProjector::from_known_srs(self.out_srs, self.in_srs)?;
        match out_partition.reproject(&projector) {
            Ok(in_partition) => {
                let bounds = self
                    .grids
                    .in_spatial_grid
                    .geo_transform()
                    .spatial_to_grid_bounds(&in_partition);
                let [min_y, min_x] = bounds.min_index().0;
                let [max_y, max_x] = bounds.max_index().0;
                Ok(Some(GridBoundingBox2D::new_unchecked(
                    [min_y - 1, min_x - 1],
                    [max_y + 1, max_x + 1],
                )))
            }
            // the projection can return an empty box, which contains no pixels
            Err(geoengine_datatypes::error::Error::OutputBboxEmpty { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
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

/// Resamples a raster onto another grid with a [`WarpKernel`], in the same spatial reference or,
/// with a [`KernelProjection`], in another one.
pub struct WarpKernelProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
{
    source: Q,
    kernel: WarpKernel,
    projection: Option<KernelProjection>,
    out_result_descriptor: RasterResultDescriptor,
}

impl<Q, P> WarpKernelProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    /// `out_result_descriptor` must have the data type of `source`, and the spatial reference of
    /// `source` if `projection` is `None`.
    pub fn new(
        source: Q,
        kernel: WarpKernel,
        projection: Option<KernelProjection>,
        out_result_descriptor: RasterResultDescriptor,
    ) -> Self {
        debug_assert_eq!(
            source.raster_result_descriptor().data_type,
            out_result_descriptor.data_type
        );
        Self {
            source,
            kernel,
            projection,
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
            projection: self.projection,
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
    projection: Option<KernelProjection>,
    input_geo_transform: GeoTransform,
    output_geo_transform: GeoTransform,
    _phantom_pixel_type: PhantomData<P>,
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
        query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture {
        let kernel = self.kernel;
        let projection = self.projection;
        let (output_geo_transform, input_geo_transform) =
            (self.output_geo_transform, self.input_geo_transform);
        let time = query_rect.time_interval();
        let pool = pool.clone();
        crate::util::spawn_blocking(move || {
            let spans = match projection {
                Some(projection) => TileSpans::projected(kernel, &tile_info, &projection, &pool)?,
                None => TileSpans::separable(
                    kernel,
                    &tile_info.global_pixel_bounds(),
                    &output_geo_transform,
                    &input_geo_transform,
                ),
            };
            Ok(WarpKernelAccu::new(kernel, tile_info, spans, time, pool))
        })
        .map_err(From::from)
        .and_then(|result| async { result })
        .boxed()
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        time: TimeInterval,
        band_idx: u32,
    ) -> Result<Option<RasterQueryRectangle>> {
        let bounds = match &self.projection {
            Some(projection) => {
                projection.input_bounds(&tile_info, &query_rect.spatial_bounds())?
            }
            None => {
                let TileSpans::Separable { y, x } = TileSpans::separable(
                    self.kernel,
                    &tile_info.global_pixel_bounds(),
                    &self.output_geo_transform,
                    &self.input_geo_transform,
                ) else {
                    unreachable!("the spans within the same spatial reference are separable")
                };
                Some(TileSpans::separable_input_bounds(&y, &x))
            }
        };
        Ok(bounds.map(|bounds| {
            RasterQueryRectangle::new(bounds, time, BandSelection::new_single(band_idx))
        }))
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
    time: TimeInterval,
    cache_hint: CacheHint,
    pool: Arc<ThreadPool>,
}

impl<P: Pixel> WarpKernelAccu<P> {
    fn new(
        kernel: WarpKernel,
        output_tile_info: TileInformation,
        spans: TileSpans,
        time: TimeInterval,
        pool: Arc<ThreadPool>,
    ) -> Self {
        Self {
            kernel,
            output_tile_info,
            pixels: vec![PixelState::new(kernel); spans.len()],
            spans: Arc::new(spans),
            time,
            cache_hint: CacheHint::max_duration(),
            pool,
        }
    }

    /// Adds the pixels of `tile` to the output pixels whose spans they are in.
    fn fold(mut self, tile: &RasterTile2D<P>) -> Self {
        let _span = tracing::debug_span!("warp_kernel_fold").entered();

        self.time = tile.time;
        self.cache_hint.merge_with(&tile.cache_hint);

        let GridOrEmpty::Grid(grid) = &tile.grid_array else {
            return self;
        };
        let in_bounds = tile.tile_information().global_pixel_bounds();
        let [in_min_y, in_min_x] = in_bounds.min_index().0;
        let [in_max_y, in_max_x] = in_bounds.max_index().0;

        let kernel = self.kernel;
        let spans = self.spans.clone();
        let width = spans.width();
        // adds the input pixels of `tile` in the spans of one output pixel to its state
        let add = |state: &mut PixelState<P>, y_span: Span, x_span: Span| {
            let x_range = x_span.clamped(in_min_x, in_max_x);
            if x_range.is_empty() {
                return;
            }
            for in_y in y_span.clamped(in_min_y, in_max_y) {
                let weight_y = y_span.weight(in_y);
                for in_x in x_range.clone() {
                    let local: GridIdx2D = [in_y - in_min_y, in_x - in_min_x].into();
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
        };

        let pixels = &mut self.pixels;
        self.pool.install(|| match spans.as_ref() {
            TileSpans::Separable { y, x } => {
                // the output columns whose spans intersect the tile
                let columns: Vec<usize> = (0..x.len())
                    .filter(|column| !x[*column].clamped(in_min_x, in_max_x).is_empty())
                    .collect();
                pixels
                    .par_chunks_mut(width)
                    .enumerate()
                    .filter(|(row, _)| !y[*row].clamped(in_min_y, in_max_y).is_empty())
                    .for_each(|(row, pixels)| {
                        for column in &columns {
                            add(&mut pixels[*column], y[row], x[*column]);
                        }
                    });
            }
            TileSpans::PerPixel {
                pixels: pixel_spans,
                ..
            } => {
                pixels
                    .par_chunks_mut(width)
                    .zip(pixel_spans.par_chunks(width))
                    .for_each(|(pixels, spans)| {
                        for (state, spans) in pixels.iter_mut().zip(spans) {
                            if let Some((y_span, x_span)) = spans {
                                add(state, *y_span, *x_span);
                            }
                        }
                    });
            }
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
            self.time,
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
        self.time = time;
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
