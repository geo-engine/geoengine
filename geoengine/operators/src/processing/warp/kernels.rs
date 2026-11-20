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
//! Input pixels without data and NaN values are left out; an output pixel without any input pixel
//! has no data.
//! Integer outputs round half up (see [`pixel_from_f64_rounded`]).
//!
//! With another spatial reference, the footprint of an output pixel is the bounding box of its
//! upper left and lower right corner in the input, as in GDAL, and `bilinear` interpolates around
//! its projected center. Output pixels whose corners or center cannot be projected have no data.

use crate::adapters::{ApproxProjection, TileReprojectionSubqueryGridInfo};
use crate::adapters::{
    FoldTileAccu, FoldTileAccuMut, RasterSubQueryAdapter, SubQueryTileAggregator,
    source_box_mapping, subquery_footprints,
};
use crate::engine::{
    Footprint, FootprintPrecision, QueryContext, QueryProcessor, RasterQueryProcessor,
    RasterResultDescriptor,
};
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
    GridIntersection, GridOrEmpty, GridShape2D, GridSize, MapElementsParallel, MaskedGrid, Pixel,
    RasterDataType, RasterTile2D, TileInformation, TilingStrategy, pixel_from_f64_rounded,
};
use geoengine_datatypes::spatial_reference::{
    CoordinateProjection, DefaultCoordinateProjector, SpatialReference,
};
use num_traits::AsPrimitive;
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

/// GDAL's tolerance for footprint edges that lie on a pixel edge, relative to the magnitude of the
/// pixel coordinate (see [`tolerance`])
const EPSILON: f64 = 1e-10;

/// The tolerance for a footprint edge at the input pixel coordinate `value`.
///
/// GDAL compares coordinates relative to its dataset, which are small. Here, they are global pixel
/// indices, which can be large on fine grids, e.g. ≈ 10⁷ at 0.0001°. Their rounding error grows with
/// their magnitude and exceeds a fixed `1e-10`, so that a pixel that only touches the edge would count.
#[inline]
fn tolerance(value: f64) -> f64 {
    EPSILON * value.abs().max(1.)
}

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
        let start = (min + tolerance(min)).floor() as isize;
        let mut end = (max - tolerance(max)).ceil() as isize;
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
            // at most 1, for an edge that lies within the tolerance outside of the span
            first_weight: (1. - (min - start as f64)).min(1.),
            last_weight: (1. - (end as f64 - max)).min(1.),
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

/// The position (y, x) of a projected `coordinate` in the input pixels of `in_geo_transform`, `None`
/// if it could not be projected. A projection can also return non-finite coordinates instead of an
/// error, e.g. outside of its area of use, which count as not projected, too.
fn to_input_pixels(
    coordinate: Option<Coordinate2D>,
    in_geo_transform: &GeoTransform,
) -> Option<(f64, f64)> {
    let origin = in_geo_transform.origin_coordinate();
    coordinate
        .filter(|c| c.x.is_finite() && c.y.is_finite())
        .map(|c| {
            (
                (c.y - origin.y) / in_geo_transform.y_pixel_size().get(),
                (c.x - origin.x) / in_geo_transform.x_pixel_size().get(),
            )
        })
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
        let to_input_pixels =
            |coordinate: Option<Coordinate2D>| to_input_pixels(coordinate, &in_geo_transform);
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
                let centers = projection.project(&centers, width, pool)?;
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
                let corners = projection.project(&corners, width + 1, pool)?;
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

    /// The spans (y, x) of each pixel, row-major, `None` for pixels that cannot be projected
    fn iter(&self) -> Box<dyn Iterator<Item = Option<(Span, Span)>> + '_> {
        match self {
            Self::Separable { y, x } => Box::new(
                y.iter()
                    .flat_map(move |y| x.iter().map(move |x| Some((*y, *x)))),
            ),
            Self::PerPixel { pixels, .. } => Box::new(pixels.iter().copied()),
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
    /// the largest error in input pixels of the approximate projection, 0 to project exactly (see
    /// [`ApproxProjection`])
    pub approximation_error: geoengine_datatypes::util::NonNegativeFiniteF64,
}

impl KernelProjection {
    /// Projects `coordinates`, rows of `row_len` coordinates, from the output into the input
    /// spatial reference, `None` for coordinates outside of the output grid (as in the
    /// nearest-neighbour reprojection) or that cannot be projected. Rows within the output grid are
    /// projected approximately, with an error of at most `approximation_error` input pixels (see
    /// [`ApproxProjection`]).
    fn project(
        &self,
        coordinates: &[Coordinate2D],
        row_len: usize,
        pool: &ThreadPool,
    ) -> Result<Vec<Option<Coordinate2D>>> {
        let chunk_size = row_len * (64 * 512_usize).div_ceil(row_len);
        let valid_area = self.grids.out_spatial_grid.spatial_partition().as_bbox();
        let in_geo_transform = self.grids.in_spatial_grid.geo_transform();
        let mut projected = vec![None; coordinates.len()];
        pool.install(|| {
            projected
                .par_chunks_mut(chunk_size)
                .zip(coordinates.par_chunks(chunk_size))
                .try_for_each(|(projected, coordinates)| {
                    let Some(bounds) = BoundingBox2D::from_coord_ref_iter(coordinates.iter())
                    else {
                        return Ok(());
                    };
                    let projector =
                        DefaultCoordinateProjector::from_known_srs(self.out_srs, self.in_srs)?;
                    // row by row if the chunk lies in the valid area, else coordinate by coordinate
                    if valid_area.contains_bbox(&bounds) {
                        let approx = ApproxProjection::new(
                            &projector,
                            &in_geo_transform,
                            self.approximation_error,
                        );
                        for (projected, row) in projected
                            .chunks_mut(row_len)
                            .zip(coordinates.chunks(row_len))
                        {
                            approx.project_row(row, projected);
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

    /// The input pixels the output tile `tile_info` needs: the projected bounds of the tile, with a
    /// margin of one pixel for the footprints and the bilinear neighbours at its edges. `None` if
    /// the tile cannot be projected.
    ///
    /// This covers the whole tile, also where it reaches beyond the query rectangle, as a tile is
    /// always filled completely, independently of the query.
    fn input_bounds(&self, tile_info: &TileInformation) -> Result<Option<GridBoundingBox2D>> {
        let Some(out_bounds) = self
            .grids
            .out_spatial_grid
            .grid_bounds()
            .intersection(&tile_info.global_pixel_bounds())
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
    /// the sum of the weights per value (`mode`)
    Mode(ModeState<P>),
    /// the result, once all input pixels of the output pixel were added (see [`WarpKernelAccu::fold`])
    Done(Option<P>),
}

/// The sums of the weights per value of the input pixels of one output pixel (`mode`).
///
/// Few values, the common case, are kept as a list sorted by value. Inputs with 8 bits switch to a
/// histogram over all 256 values once the list gets longer than [`SPARSE_MODE_ENTRIES`], so that a
/// pixel keeps at most 2 KB.
#[derive(Debug, Clone)]
enum ModeState<P> {
    Sparse(Vec<ModeEntry<P>>),
    Dense(Box<DenseMode>),
}

/// The length of the list of a [`ModeState`] at which inputs with 8 bits switch to a histogram
const SPARSE_MODE_ENTRIES: usize = 32;

#[derive(Debug, Clone, Copy)]
struct ModeEntry<P> {
    value: P,
    /// `f32` as in GDAL, which makes sums like `1 + 1 + 0.999_999_999_999_996` ties
    weight: f32,
    /// the row-major position of the last input pixel with this value in the footprint
    last_position: u32,
}

/// The histogram of a [`ModeState`] over the 256 values of an 8-bit type; a weight of 0 means that
/// the value does not occur
#[derive(Debug, Clone)]
struct DenseMode {
    weights: [f32; 256],
    last_positions: [u32; 256],
}

/// An upper bound of the memory that `mode` keeps for one output tile (see
/// [`mode_memory_per_tile`])
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModeMemoryEstimate {
    /// the bytes of the pixel states of an output tile
    pub bytes: u64,
    /// the input pixels an output pixel touches at most
    pub footprint_pixels: u64,
}

/// Checks that `estimate` for `mode` on `data_type` keeps at most `budget` bytes.
///
/// # Errors
/// If it exceeds `budget`, or a footprint has more input pixels than its positions can hold.
pub fn ensure_mode_fits_memory_budget(
    estimate: &ModeMemoryEstimate,
    budget: usize,
    data_type: RasterDataType,
) -> Result<()> {
    const MB: u64 = 1024 * 1024;
    let budget = budget as u64;
    // the positions in a footprint are `u32` (see `ModeEntry`)
    if estimate.bytes > budget || estimate.footprint_pixels > u64::from(u32::MAX) {
        return Err(crate::error::Error::WarpModeExceedsMemoryBudget {
            estimated_mb: estimate.bytes.div_ceil(MB),
            budget_mb: budget / MB,
            footprint_pixels: estimate.footprint_pixels,
            data_type,
        });
    }
    Ok(())
}

/// The memory of the state of `mode` for an output pixel with `footprint_pixels` input pixels of
/// `data_type`, at most
fn mode_pixel_bytes(data_type: RasterDataType, footprint_pixels: u64) -> u64 {
    let (distinct_values, entry_bytes) = match data_type {
        RasterDataType::U8 | RasterDataType::I8 => (256, size_of::<ModeEntry<u8>>()),
        RasterDataType::U16 | RasterDataType::I16 => (65_536, size_of::<ModeEntry<u16>>()),
        RasterDataType::U32 | RasterDataType::I32 | RasterDataType::F32 => {
            (u64::MAX, size_of::<ModeEntry<u32>>())
        }
        RasterDataType::U64 | RasterDataType::I64 | RasterDataType::F64 => {
            (u64::MAX, size_of::<ModeEntry<u64>>())
        }
    };
    let entries = footprint_pixels.min(distinct_values);
    if distinct_values == 256 && entries > SPARSE_MODE_ENTRIES as u64 {
        size_of::<DenseMode>() as u64
    } else {
        // the capacity of the list doubles
        entries
            .checked_next_power_of_two()
            .unwrap_or(u64::MAX)
            .saturating_mul(entry_bytes as u64)
    }
}

/// The memory that `mode` keeps at most at the same time for the output pixels with `spans`, with
/// input tiles of `tile_shape`.
///
/// Unlike [`mode_memory_per_tile`], this uses the actual footprints, so it also holds when the
/// output grid is rotated against the input grid. An output pixel keeps its state from the first
/// input tile of its footprint until the one with its lower right input pixel, as the input tiles
/// come row by row (see [`WarpKernelAccu::fold`]).
fn mode_memory_of_spans(
    spans: &TileSpans,
    data_type: RasterDataType,
    tile_shape: GridShape2D,
) -> ModeMemoryEstimate {
    let [tile_height, tile_width] = tile_shape.shape_array.map(|size| size as isize);
    let tile_of = |y: isize, x: isize| [y.div_euclid(tile_height), x.div_euclid(tile_width)];

    // the input tiles of the footprints
    let (mut min, mut max) = ([isize::MAX; 2], [isize::MIN; 2]);
    for (y, x) in spans.iter().flatten() {
        let (first, last) = (tile_of(y.start, x.start), tile_of(y.end - 1, x.end - 1));
        for axis in 0..2 {
            min[axis] = min[axis].min(first[axis]);
            max[axis] = max[axis].max(last[axis]);
        }
    }
    let states = spans.len() as u64 * size_of::<PixelState<u64>>() as u64;
    if min[0] > max[0] {
        return ModeMemoryEstimate {
            bytes: states,
            footprint_pixels: 0,
        };
    }
    // the input tiles row by row, as they come
    #[allow(clippy::cast_sign_loss)]
    let columns = (max[1] - min[1] + 1) as usize;
    #[allow(clippy::cast_sign_loss)]
    let order = |[y, x]: [isize; 2]| (y - min[0]) as usize * columns + (x - min[1]) as usize;
    #[allow(clippy::cast_sign_loss)]
    let tiles = (max[0] - min[0] + 1) as usize * columns;

    // the bytes of the pixels that start at a tile and of those that are finalized after it
    let (mut starting, mut ending) = (vec![0_u64; tiles], vec![0_u64; tiles]);
    let mut footprint_pixels = 0;
    for (y, x) in spans.iter().flatten() {
        #[allow(clippy::cast_sign_loss)]
        let footprint = (y.end - y.start) as u64 * (x.end - x.start) as u64;
        footprint_pixels = footprint_pixels.max(footprint);
        let bytes = mode_pixel_bytes(data_type, footprint);
        starting[order(tile_of(y.start, x.start))] += bytes;
        ending[order(tile_of(y.end - 1, x.end - 1))] += bytes;
    }
    let (mut open, mut peak) = (0_u64, 0_u64);
    for (starting, ending) in starting.into_iter().zip(ending) {
        open += starting;
        peak = peak.max(open);
        open -= ending;
    }

    ModeMemoryEstimate {
        bytes: states.saturating_add(peak),
        footprint_pixels,
    }
}

/// An upper bound of the memory that `mode` keeps for one output tile of `tile_shape` pixels, for
/// inputs of `data_type` whose pixels are `ratio` times smaller than the output pixels (y, x), with
/// `threads` threads that compute output rows at the same time.
///
/// [`WarpKernelAccu::fold`] finalizes an output pixel after the input tile with its last input pixel,
/// so the states of the other pixels have no entries yet or no more. At most these pixels are open:
/// - two output rows whose footprints reach into the next row of input tiles,
/// - one output pixel per output row of the current row of input tiles, whose footprint reaches into
///   the next input tile to the right,
/// - the output pixels of the current input tile in the rows that the threads add at the same time.
///
/// Each open pixel keeps an entry per distinct value, at most one per input pixel of its footprint and
/// at most one per value of `data_type`, or a histogram of 2 KB for 8-bit inputs.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
pub fn mode_memory_per_tile(
    data_type: RasterDataType,
    (ratio_y, ratio_x): (f64, f64),
    tile_shape: GridShape2D,
    threads: usize,
) -> ModeMemoryEstimate {
    let [tile_height, tile_width] = tile_shape.shape_array.map(|size| size as u64);
    // the input pixels an output pixel touches along an axis, with partly covered ones at both edges
    let along = |ratio: f64| ratio.ceil() as u64 + 1;
    let footprint_pixels = along(ratio_y).saturating_mul(along(ratio_x));

    let pixel_bytes = mode_pixel_bytes(data_type, footprint_pixels);

    // the output pixels that one input tile touches along an axis
    let per_input_tile =
        |size: u64, ratio: f64| ((size as f64 / ratio).ceil() as u64 + 1).min(size);
    let rows = per_input_tile(tile_height, ratio_y);
    let columns = per_input_tile(tile_width, ratio_x);
    let open_pixels = (2 * tile_width + rows + (threads as u64).min(rows) * columns)
        .min(tile_height * tile_width);

    let states = tile_height * tile_width * size_of::<PixelState<u64>>() as u64;
    ModeMemoryEstimate {
        bytes: states.saturating_add(open_pixels.saturating_mul(pixel_bytes)),
        footprint_pixels,
    }
}

/// The bin of `value` in a [`DenseMode`], `None` if `P` has more than 8 bits
#[inline]
fn dense_bin<P: Pixel>(value: P) -> Option<usize> {
    match P::TYPE {
        RasterDataType::U8 => Some(usize::from(AsPrimitive::<u8>::as_(value))),
        RasterDataType::I8 => Some(usize::from(
            AsPrimitive::<i8>::as_(value).cast_unsigned() ^ 0x80,
        )),
        _ => None,
    }
}

/// The value of the bin `bin` of a [`DenseMode`]
fn dense_value<P: Pixel>(bin: usize) -> Option<P> {
    let offset = if P::TYPE == RasterDataType::I8 {
        128.
    } else {
        0.
    };
    pixel_from_f64_rounded(bin as f64 - offset)
}

/// Of the values with the largest sum of weights, the one that reached it first. GDAL updates the
/// mode only if a sum exceeds the maximum so far, so that is the value whose last pixel comes first.
fn mode_of<P: Copy>(entries: impl Iterator<Item = ModeEntry<P>>) -> Option<P> {
    entries
        .reduce(|best, entry| match entry.weight.partial_cmp(&best.weight) {
            Some(Ordering::Greater) => entry,
            // an exact tie, as GDAL compares the sums
            Some(Ordering::Equal) if entry.last_position < best.last_position => entry,
            _ => best,
        })
        .map(|entry| entry.value)
}

impl<P: Pixel> ModeState<P> {
    #[inline]
    fn add(&mut self, value: P, weight: f32, position: u32) {
        match self {
            Self::Sparse(entries) => {
                let found = entries.binary_search_by(|entry| {
                    entry.value.partial_cmp(&value).unwrap_or(Ordering::Equal)
                });
                match found {
                    Ok(idx) => {
                        let entry = &mut entries[idx];
                        entry.weight += weight;
                        entry.last_position = entry.last_position.max(position);
                    }
                    Err(idx) => {
                        if entries.len() >= SPARSE_MODE_ENTRIES
                            && let Some(dense) = Self::to_dense(entries)
                        {
                            *self = Self::Dense(dense);
                            self.add(value, weight, position);
                            return;
                        }
                        entries.insert(
                            idx,
                            ModeEntry {
                                value,
                                weight,
                                last_position: position,
                            },
                        );
                    }
                }
            }
            Self::Dense(dense) => {
                if let Some(bin) = dense_bin(value) {
                    dense.weights[bin] += weight;
                    dense.last_positions[bin] = dense.last_positions[bin].max(position);
                }
            }
        }
    }

    /// The histogram of `entries`, `None` if `P` has more than 8 bits
    fn to_dense(entries: &[ModeEntry<P>]) -> Option<Box<DenseMode>> {
        dense_bin(P::zero())?;
        let mut dense = Box::new(DenseMode {
            weights: [0.; 256],
            last_positions: [0; 256],
        });
        for entry in entries {
            let bin = dense_bin(entry.value)?;
            dense.weights[bin] = entry.weight;
            dense.last_positions[bin] = entry.last_position;
        }
        Some(dense)
    }

    fn finish(&self) -> Option<P> {
        match self {
            Self::Sparse(entries) => mode_of(entries.iter().copied()),
            Self::Dense(dense) => mode_of(
                dense
                    .weights
                    .iter()
                    .zip(&dense.last_positions)
                    .enumerate()
                    .filter(|(_, (weight, _))| **weight > 0.)
                    .filter_map(|(bin, (&weight, &last_position))| {
                        Some(ModeEntry {
                            value: dense_value::<P>(bin)?,
                            weight,
                            last_position,
                        })
                    }),
            ),
        }
    }
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
            WarpKernel::Mode => Self::Mode(ModeState::Sparse(Vec::new())),
        }
    }

    /// Adds the input pixel with `value` at the row-major `position` in the footprint; `center` if it
    /// is under the output pixel center.
    #[inline]
    fn add(&mut self, kernel: WarpKernel, value: P, weight: f64, position: u32, center: bool) {
        // an unmasked NaN counts as no data in every method, as GDAL skips invalid float values
        let value_f64: f64 = value.as_();
        if value_f64.is_nan() {
            return;
        }
        match self {
            Self::Sum {
                sum,
                weight: total,
                center: has_center,
            } => {
                *has_center |= center;
                if weight > 0. {
                    *sum += weight * value_f64;
                    *total += weight;
                }
            }
            Self::Extreme(extreme) => {
                let replace = match extreme {
                    None => true,
                    Some(current) if kernel == WarpKernel::Min => value < *current,
                    Some(current) => value > *current,
                };
                if replace {
                    *extreme = Some(value);
                }
            }
            Self::Mode(mode) => {
                if weight > 0. {
                    #[allow(clippy::cast_possible_truncation)]
                    mode.add(value, weight as f32, position);
                }
            }
            Self::Done(_) => {
                debug_assert!(
                    false,
                    "an input pixel came after the last one of its footprint"
                );
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
            Self::Mode(mode) => mode.finish(),
            Self::Done(value) => *value,
        }
    }

    /// Replaces the state by its result, which frees the memory of `mode`
    #[inline]
    fn finalize(&mut self, kernel: WarpKernel) {
        if !matches!(self, Self::Done(_)) {
            *self = Self::Done(self.finish(kernel));
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
    /// the limit for the memory of `mode` per output tile
    mode_memory_budget: usize,
}

impl<Q, P> WarpKernelProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    /// `out_result_descriptor` must have the data type of `source`, and the spatial reference of
    /// `source` if `projection` is `None`.
    ///
    /// With `projection`, `mode` checks for each output tile that its footprints keep at most
    /// `mode_memory_budget` bytes (see [`mode_memory_of_spans`]). Without, the footprints are the
    /// same for all tiles, so [`mode_memory_per_tile`] checks that once at initialization.
    pub fn new(
        source: Q,
        kernel: WarpKernel,
        projection: Option<KernelProjection>,
        out_result_descriptor: RasterResultDescriptor,
        mode_memory_budget: usize,
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
            mode_memory_budget,
        }
    }
}

impl<Q, P> WarpKernelProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    /// The tiles of the output and how each of them is computed from the source.
    fn sub_query(&self, ctx: &dyn QueryContext) -> (TilingStrategy, WarpKernelSubQuery<P>) {
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
            mode_memory_budget: (self.kernel == WarpKernel::Mode && self.projection.is_some())
                .then_some(self.mode_memory_budget),
            tile_shape: ctx.tiling_specification().tile_size_in_pixels,
            _phantom_pixel_type: PhantomData,
        };
        (out_tiling_grid.generate_data_tiling_strategy(), sub_query)
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

    async fn _footprint_query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        precision: FootprintPrecision,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Footprint>>> {
        let (tiling_strategy, sub_query) = self.sub_query(ctx);
        // an output pixel reads the input pixels under it and, for `bilinear`, their neighbours
        let output_bounds = source_box_mapping(
            [1, 1],
            sub_query.input_geo_transform,
            sub_query.output_geo_transform,
            self.projection.map(|projection| {
                (
                    projection.in_srs,
                    projection.out_srs,
                    projection.approximation_error,
                )
            }),
        );
        subquery_footprints(
            &self.source,
            query,
            tiling_strategy,
            &sub_query,
            precision,
            ctx,
            output_bounds,
        )
        .await
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
        let (tiling_strategy, sub_query) = self.sub_query(ctx);

        Ok(
            RasterSubQueryAdapter::<'a, P, _, _>::with_source_time_steps(
                &self.source,
                query,
                tiling_strategy,
                ctx,
                sub_query,
            )
            .await?
            .box_pin(),
        )
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
    /// the limit for the memory of `mode` per output tile, if it is checked per tile
    mode_memory_budget: Option<usize>,
    /// the size of the input tiles
    tile_shape: GridShape2D,
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
        let (mode_memory_budget, tile_shape) = (self.mode_memory_budget, self.tile_shape);
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
            if let Some(budget) = mode_memory_budget {
                let estimate = mode_memory_of_spans(&spans, P::TYPE, tile_shape);
                ensure_mode_fits_memory_budget(&estimate, budget, P::TYPE)?;
            }
            Ok(WarpKernelAccu::new(kernel, tile_info, spans, time, pool))
        })
        .map_err(From::from)
        .and_then(|result| async { result })
        .boxed()
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        _query_rect: RasterQueryRectangle,
        time: TimeInterval,
        band_idx: u32,
    ) -> Result<Option<RasterQueryRectangle>> {
        let bounds = match &self.projection {
            Some(projection) => projection.input_bounds(&tile_info)?,
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
    /// the position of the last input tile, to check that they come row by row
    last_tile_position: Option<[isize; 2]>,
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
            last_tile_position: None,
            time,
            cache_hint: CacheHint::max_duration(),
            pool,
        }
    }

    /// Adds the pixels of `tile` to the output pixels whose spans they are in.
    ///
    /// The input tiles come row by row (see the raster tile order in `geoengine/AGENTS.md`), so of the
    /// tiles under an output pixel, the one with its lower right input pixel comes last. After that
    /// tile, the output pixel is finalized, which frees the memory of `mode`. So only the output
    /// pixels whose footprint reaches beyond the current tile stay open, instead of all pixels of the
    /// output tile.
    fn fold(mut self, tile: &RasterTile2D<P>) -> Self {
        let _span = tracing::debug_span!("warp_kernel_fold").entered();

        let tile_position = tile.tile_position.0;
        debug_assert!(
            self.last_tile_position
                .is_none_or(|last| last < tile_position),
            "input tiles must come row by row, got {tile_position:?} after {:?}",
            self.last_tile_position
        );
        self.last_tile_position = Some(tile_position);
        self.time = tile.time;
        self.cache_hint.merge_with(&tile.cache_hint);

        let input = InputTile::new(tile);
        let kernel = self.kernel;
        let spans = self.spans.clone();
        let width = spans.width();

        let pixels = &mut self.pixels;
        self.pool.install(|| match spans.as_ref() {
            TileSpans::Separable { y, x } => {
                // the output columns whose spans intersect the tile, and whether this is the last
                // tile of their spans along the x axis
                let columns: Vec<(usize, std::ops::Range<isize>, bool)> = x
                    .iter()
                    .enumerate()
                    .map(|(column, span)| (column, input.x_range(*span), input.is_last_x(*span)))
                    .filter(|(_, range, _)| !range.is_empty())
                    .collect();
                if columns.is_empty() {
                    return;
                }
                pixels
                    .par_chunks_mut(width)
                    .enumerate()
                    .filter(|(row, _)| !input.y_range(y[*row]).is_empty())
                    .for_each(|(row, pixels)| {
                        let y_span = y[row];
                        for (column, x_range, _) in &columns {
                            input.add(kernel, &mut pixels[*column], y_span, x[*column], x_range);
                        }
                        // this tile has the lower right input pixel of these output pixels
                        if input.is_last_y(y_span) {
                            for (column, _, last) in &columns {
                                if *last {
                                    pixels[*column].finalize(kernel);
                                }
                            }
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
                            let Some((y_span, x_span)) = *spans else {
                                continue;
                            };
                            let x_range = input.x_range(x_span);
                            if x_range.is_empty() || input.y_range(y_span).is_empty() {
                                continue;
                            }
                            input.add(kernel, state, y_span, x_span, &x_range);
                            // this tile has the lower right input pixel of this output pixel
                            if input.is_last_y(y_span) && input.is_last_x(x_span) {
                                state.finalize(kernel);
                            }
                        }
                    });
            }
        });

        self
    }
}

/// An input tile in the fold of [`WarpKernelAccu`]
struct InputTile<'a, P: Pixel> {
    /// `None` for an empty tile
    grid: Option<&'a MaskedGrid<GridShape2D, P>>,
    min: [isize; 2],
    max: [isize; 2],
}

impl<'a, P: Pixel> InputTile<'a, P> {
    fn new(tile: &'a RasterTile2D<P>) -> Self {
        let bounds = tile.tile_information().global_pixel_bounds();
        Self {
            grid: match &tile.grid_array {
                GridOrEmpty::Grid(grid) => Some(grid),
                GridOrEmpty::Empty(_) => None,
            },
            min: bounds.min_index().0,
            max: bounds.max_index().0,
        }
    }

    fn y_range(&self, span: Span) -> std::ops::Range<isize> {
        span.clamped(self.min[0], self.max[0])
    }

    fn x_range(&self, span: Span) -> std::ops::Range<isize> {
        span.clamped(self.min[1], self.max[1])
    }

    /// Whether this tile has the last input pixel of `span` along the y axis
    fn is_last_y(&self, span: Span) -> bool {
        (self.min[0]..=self.max[0]).contains(&(span.end - 1))
    }

    /// Whether this tile has the last input pixel of `span` along the x axis
    fn is_last_x(&self, span: Span) -> bool {
        (self.min[1]..=self.max[1]).contains(&(span.end - 1))
    }

    /// Adds the input pixels within `y_span` and `x_range` (the part of `x_span` in this tile) to
    /// the state of one output pixel
    fn add(
        &self,
        kernel: WarpKernel,
        state: &mut PixelState<P>,
        y_span: Span,
        x_span: Span,
        x_range: &std::ops::Range<isize>,
    ) {
        let Some(grid) = self.grid else {
            return;
        };
        // the row-major position in the footprint, which fits into `u32` (see
        // `InitializedWarp::with_mode_memory_budget` and `mode_memory_of_spans`)
        let footprint_width = x_span.end - x_span.start;
        for in_y in self.y_range(y_span) {
            let weight_y = y_span.weight(in_y);
            let row_position = (in_y - y_span.start) * footprint_width;
            for in_x in x_range.clone() {
                let local: GridIdx2D = [in_y - self.min[0], in_x - self.min[1]].into();
                if let Some(value) = grid.get_at_grid_index_unchecked(local) {
                    let center = in_y == y_span.center && in_x == x_span.center;
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    let position = (row_position + in_x - x_span.start) as u32;
                    state.add(
                        kernel,
                        value,
                        weight_y * x_span.weight(in_x),
                        position,
                        center,
                    );
                }
            }
        }
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
    fn it_ignores_rounding_errors_of_large_pixel_indices() {
        // 0.0003° over 0.0001°: the output pixel 10⁷ covers the input pixels 3 × 10⁷ .. 3 × 10⁷ + 3,
        // but `1e7 * 0.0003 / 0.0001` is 29_999_999.999_999_993, below the edge by more than 1e-10
        let spans = spans(
            WarpKernel::Average,
            (10_000_000, 10_000_002),
            (0., 0.0003),
            (0., 0.0001),
        );

        for (k, span) in spans.into_iter().enumerate() {
            let start = 30_000_000 + 3 * k as isize;
            assert_eq!(
                (span.start, span.end),
                (start, start + 3),
                "output pixel {k}"
            );
            assert!(
                (span.first_weight - 1.).abs() < 1e-6 && (span.last_weight - 1.).abs() < 1e-6,
                "output pixel {k}: {span:?}"
            );
        }
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
        state.add(WarpKernel::Mode, 1, 1., 0, false);
        state.add(WarpKernel::Mode, 2, 1., 1, false);
        state.add(WarpKernel::Mode, 2, 1., 2, false);
        state.add(WarpKernel::Mode, 1, 1., 3, false);
        assert_eq!(state.finish(WarpKernel::Mode), Some(2));

        // the order of adding does not matter, only the positions
        let mut state = PixelState::<u8>::new(WarpKernel::Mode);
        state.add(WarpKernel::Mode, 1, 1., 3, false);
        state.add(WarpKernel::Mode, 2, 1., 2, false);
        state.add(WarpKernel::Mode, 1, 1., 0, false);
        state.add(WarpKernel::Mode, 2, 1., 1, false);
        assert_eq!(state.finish(WarpKernel::Mode), Some(2));

        // weights count
        let mut state = PixelState::<u8>::new(WarpKernel::Mode);
        state.add(WarpKernel::Mode, 1, 0.25, 0, false);
        state.add(WarpKernel::Mode, 1, 0.25, 1, false);
        state.add(WarpKernel::Mode, 2, 1., 2, false);
        assert_eq!(state.finish(WarpKernel::Mode), Some(2));
    }

    /// Adds 40 values, more than [`SPARSE_MODE_ENTRIES`], with a tie of two of them, and returns the
    /// mode and whether the state switched to a histogram
    fn mode_of_many<P: Pixel>(value: impl Fn(i32) -> P, tie: (i32, i32)) -> (Option<P>, bool) {
        let mut state = PixelState::<P>::new(WarpKernel::Mode);
        let mut position = 0;
        for k in 0..40 {
            state.add(WarpKernel::Mode, value(k), 1., position, false);
            position += 1;
        }
        // both reach a weight of 2; `tie.1` gets there first
        state.add(WarpKernel::Mode, value(tie.1), 1., position, false);
        state.add(WarpKernel::Mode, value(tie.0), 1., position + 1, false);
        let dense = matches!(state, PixelState::Mode(ModeState::Dense(_)));
        (state.finish(WarpKernel::Mode), dense)
    }

    #[test]
    fn it_switches_to_a_histogram_for_8_bits_with_the_same_mode() {
        // `u8`: 100..140
        assert_eq!(
            mode_of_many(|k| u8::try_from(100 + k).unwrap(), (3, 30)),
            (Some(130), true)
        );
        // `i8`: -20..20, including the switch of the sign
        assert_eq!(
            mode_of_many(|k| i8::try_from(k - 20).unwrap(), (25, 5)),
            (Some(-15), true)
        );
        // more than 8 bits keep the sorted list
        assert_eq!(
            mode_of_many(|k| u16::try_from(1000 * k).unwrap(), (3, 30)),
            (Some(30_000), false)
        );
    }

    #[test]
    fn it_estimates_the_memory_of_mode_per_tile() {
        const MB: u64 = 1024 * 1024;
        let estimate = |data_type, factor: f64| {
            mode_memory_per_tile(data_type, (factor, factor), [512, 512].into(), 16)
        };

        // 8 bits keep at most a histogram per open pixel, whatever the factor
        assert!(estimate(RasterDataType::U8, 256.).bytes < 16 * MB);
        assert!(estimate(RasterDataType::I8, 4096.).bytes < 16 * MB);
        // a few values per pixel at small factors
        assert!(estimate(RasterDataType::U16, 2.).bytes < 16 * MB);
        // up to 33 × 33 values per pixel
        let u16_32 = estimate(RasterDataType::U16, 32.);
        assert_eq!(u16_32.footprint_pixels, 33 * 33);
        assert!(
            u16_32.bytes > 32 * MB && u16_32.bytes < 64 * MB,
            "{u16_32:?}"
        );
        // beyond the default budget
        assert!(estimate(RasterDataType::U16, 64.).bytes > 64 * MB);
        assert!(estimate(RasterDataType::I64, 64.).bytes > 64 * MB);
    }

    #[test]
    fn it_finalizes_output_pixels_after_the_tile_with_their_last_input_pixel() {
        use geoengine_datatypes::util::test::{AsciiRasterTiles, TestDefault};

        // 6 × 6 classes of 1° in tiles of 3 × 3; the upper right tile is empty
        let tiles = AsciiRasterTiles::new(indoc::indoc! {"
            [0, 10)
            +-------+-------+
            | 1 1 2 | _ _ _ |
            | 1 2 _ | _ _ _ |
            | 5 5 6 | _ _ _ |
            +-------+-------+
            | 5 6 7 | 6 _ _ |
            | 7 7 7 | 8 8 9 |
            | 7 7 7 | 9 9 8 |
            +-------+-------+
        "})
        .tile_origin([-2, 0])
        .tiles::<u8>();
        assert!(tiles[1].is_empty());

        // one output tile of 3 × 3 pixels of 2°, so footprints span input tiles
        let out_geo_transform = GeoTransform::new_unchecked((0., 0.).into(), 2., -2.);
        let out_tile = TileInformation::new([-1, 0].into(), [3, 3].into(), out_geo_transform);
        let spans = TileSpans::separable(
            WarpKernel::Mode,
            &out_tile.global_pixel_bounds(),
            &out_geo_transform,
            &GeoTransform::test_default(),
        );
        let pool = Arc::new(rayon::ThreadPoolBuilder::new().build().unwrap());
        let mut accu = WarpKernelAccu::new(
            WarpKernel::Mode,
            out_tile,
            spans,
            TimeInterval::new_unchecked(0, 10),
            pool,
        );
        let done = |accu: &WarpKernelAccu<u8>| {
            accu.pixels
                .iter()
                .map(|state| matches!(state, PixelState::Done(_)))
                .collect::<Vec<_>>()
        };

        let mut tiles = tiles.iter();
        accu = accu.fold(tiles.next().unwrap());
        // the first output pixel lies in the first input tile
        assert_eq!(
            done(&accu),
            [true, false, false, false, false, false, false, false, false]
        );
        // the empty tile is the last one of the other two pixels of the first row
        accu = accu.fold(tiles.next().unwrap());
        assert_eq!(
            done(&accu),
            [true, true, true, false, false, false, false, false, false]
        );
        for tile in tiles {
            accu = accu.fold(tile);
        }
        assert_eq!(done(&accu), [true; 9]);

        assert_eq!(
            accu.pixels
                .iter()
                .map(|state| state.finish(WarpKernel::Mode))
                .collect::<Vec<_>>(),
            [
                Some(1),
                Some(2),
                None,
                Some(5),
                Some(6),
                None,
                Some(7),
                Some(7),
                Some(9)
            ]
        );
    }

    #[test]
    fn it_measures_the_mode_memory_of_the_open_footprints() {
        let span = |start: f64, end: f64| Span::footprint(start, end);
        // input tiles of 3 × 3 pixels, so the tiles (0, 0), (0, 1), (1, 0) and (1, 1)
        let spans = TileSpans::PerPixel {
            width: 4,
            pixels: vec![
                // 4 pixels, within tile (0, 0)
                Some((span(0., 2.), span(0., 2.))),
                // 4 pixels, from tile (0, 0) to (1, 1)
                Some((span(2., 4.), span(2., 4.))),
                // 2 pixels, within tile (1, 0)
                Some((span(3., 5.), span(0., 1.))),
                None,
            ],
        };

        let estimate = mode_memory_of_spans(&spans, RasterDataType::U16, [3, 3].into());

        // at tile (0, 0), the first two pixels are open, with lists of 4 entries
        let entry = size_of::<ModeEntry<u16>>() as u64;
        let states = 4 * size_of::<PixelState<u64>>() as u64;
        assert_eq!(estimate.bytes, states + 2 * 4 * entry);
        assert_eq!(estimate.footprint_pixels, 4);
        assert_eq!(
            mode_memory_of_spans(
                &TileSpans::PerPixel {
                    width: 1,
                    pixels: vec![None],
                },
                RasterDataType::U16,
                [3, 3].into(),
            )
            .bytes,
            size_of::<PixelState<u64>>() as u64
        );
    }

    #[test]
    fn it_treats_non_finite_coordinates_as_not_projected() {
        let geo_transform = GeoTransform::new_unchecked((10., 20.).into(), 2., -4.);

        assert_eq!(
            to_input_pixels(Some((14., 12.).into()), &geo_transform),
            Some((2., 2.))
        );
        assert_eq!(to_input_pixels(None, &geo_transform), None);
        for coordinate in [
            (f64::NAN, 12.),
            (14., f64::NAN),
            (f64::INFINITY, 12.),
            (14., f64::NEG_INFINITY),
        ] {
            assert_eq!(
                to_input_pixels(Some(coordinate.into()), &geo_transform),
                None,
                "{coordinate:?}"
            );
        }
    }

    #[test]
    fn it_finalizes_a_pixel_once() {
        let mut state = PixelState::<u8>::new(WarpKernel::Mode);
        state.add(WarpKernel::Mode, 7, 1., 0, false);
        state.finalize(WarpKernel::Mode);
        assert!(matches!(state, PixelState::Done(Some(7))));
        state.finalize(WarpKernel::Mode);
        assert_eq!(state.finish(WarpKernel::Mode), Some(7));
    }

    #[test]
    fn it_averages_with_weights_and_rounds() {
        let mut state = PixelState::<u8>::new(WarpKernel::Average);
        state.add(WarpKernel::Average, 1, 0.5, 0, false);
        state.add(WarpKernel::Average, 2, 0.5, 1, false);
        assert_eq!(state.finish(WarpKernel::Average), Some(2));

        let mut state = PixelState::<f32>::new(WarpKernel::Average);
        state.add(WarpKernel::Average, 1., 0.25, 0, false);
        state.add(WarpKernel::Average, 2., 0.75, 1, false);
        assert_eq!(state.finish(WarpKernel::Average), Some(1.75));

        assert_eq!(
            PixelState::<u8>::new(WarpKernel::Average).finish(WarpKernel::Average),
            None
        );
    }
}
