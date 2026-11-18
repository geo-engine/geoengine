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
//! Input pixels without data and NaN values are left out; an output pixel without any input pixel
//! has no data.
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
    GridShape2D, MapElementsParallel, MaskedGrid, Pixel, RasterDataType, RasterTile2D,
    TileInformation, pixel_from_f64_rounded,
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
    let pixel_bytes = if distinct_values == 256 && entries > SPARSE_MODE_ENTRIES as u64 {
        size_of::<DenseMode>() as u64
    } else {
        // the capacity of the list doubles
        entries
            .checked_next_power_of_two()
            .unwrap_or(u64::MAX)
            .saturating_mul(entry_bytes as u64)
    };

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
    /// the position of the last input tile, to check that they come row by row
    last_tile_position: Option<[isize; 2]>,
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
            last_tile_position: None,
            time: None,
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
        self.time = Some(tile.time);
        self.cache_hint.merge_with(&tile.cache_hint);

        let grid = match &tile.grid_array {
            GridOrEmpty::Grid(grid) => Some(grid),
            GridOrEmpty::Empty(_) => None,
        };
        let in_bounds = tile.tile_information().global_pixel_bounds();
        let [in_min_y, in_min_x] = in_bounds.min_index().0;
        let [in_max_y, in_max_x] = in_bounds.max_index().0;
        let [tile_offset_y, tile_offset_x] = in_bounds.min_index().0;

        let kernel = self.kernel;
        let spans = self.spans.clone();
        let width = spans.x.len();
        // the output columns whose spans intersect the tile, and whether this is the last tile of
        // their spans along the x axis
        let columns: Vec<(usize, std::ops::Range<isize>, bool)> = spans
            .x
            .iter()
            .enumerate()
            .map(|(column, span)| {
                let last = (in_min_x..=in_max_x).contains(&(span.end - 1));
                (column, span.clamped(in_min_x, in_max_x), last)
            })
            .filter(|(_, range, _)| !range.is_empty())
            .collect();
        if columns.is_empty() {
            return self;
        }

        let add_row = |(row, pixels): (usize, &mut [PixelState<P>])| {
            let y_span = spans.y[row];
            if let Some(grid) = grid {
                for in_y in y_span.clamped(in_min_y, in_max_y) {
                    let weight_y = y_span.weight(in_y);
                    for (column, x_range, _) in &columns {
                        let x_span = spans.x[*column];
                        let state = &mut pixels[*column];
                        // the row-major position in the footprint, which fits into `u32` (see
                        // `InitializedWarp::ensure_mode_fits_memory_budget`)
                        let footprint_width = x_span.end - x_span.start;
                        let row_position = (in_y - y_span.start) * footprint_width;
                        for in_x in x_range.clone() {
                            let local: GridIdx2D =
                                [in_y - tile_offset_y, in_x - tile_offset_x].into();
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

            // this tile has the lower right input pixel of these output pixels
            if (in_min_y..=in_max_y).contains(&(y_span.end - 1)) {
                for (column, _, last) in &columns {
                    if *last {
                        pixels[*column].finalize(kernel);
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
        let spans = TileSpans::new(
            WarpKernel::Mode,
            &out_tile.global_pixel_bounds(),
            &out_geo_transform,
            &GeoTransform::test_default(),
        );
        let pool = Arc::new(rayon::ThreadPoolBuilder::new().build().unwrap());
        let mut accu = WarpKernelAccu::new(WarpKernel::Mode, out_tile, spans, pool);
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
