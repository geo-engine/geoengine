//! Rasterization of (multi) polygons.

mod scanline;

use self::scanline::{cols_within, row_spans, rows_within};
use crate::engine::{
    BoxRasterQueryProcessor, CanonicOperatorName, ExecutionContext, InitializedRasterOperator,
    InitializedSources, InitializedVectorOperator, Operator, OperatorName,
    ProcessingGraphOperatorPath, QueryContext, QueryProcessor, RasterBandDescriptor,
    RasterBandDescriptors, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
    SingleVectorSource, SpatialGridDescriptor, TimeDescriptor, TypedRasterOperator,
    TypedRasterQueryProcessor, VectorQueryProcessor, VectorResultDescriptor,
};
use crate::error::{self, Error};
use crate::optimization::{OptimizableOperator, OptimizationError};
use crate::util::pixel_aggregation::{
    Boxed, ConstantAggregator, CountAggregator, FirstAggregator, LastAggregator, MaxAggregator,
    MeanAggregator, MinAggregator, PercentileEstimateAggregator, PixelAggregator, PropagateNoData,
    SumAggregator, finish_states, values_to_grid,
};
use crate::util::{Result, spawn_blocking_with_thread_pool};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, stream};
use geoengine_datatypes::collections::{
    FeatureCollectionInfos, IntoGeometryIterator, MultiPolygonCollection, VectorDataType,
};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BandSelection, BoundingBox2D, CacheHint, ColumnSelection, Coordinate2D,
    FeatureDataType, Measurement, MultiPolygonAccess, MultiPolygonRef, RasterQueryRectangle,
    SpatialPartition2D, SpatialPartitioned, SpatialResolution, TimeInstance, TimeInterval,
    TimeStep, VectorQueryRectangle, find_next_best_overview_level_resolution,
};
use geoengine_datatypes::raster::{
    GeoTransform, GridBoundingBox2D, GridOrEmpty, GridSize, Pixel, RasterDataType, RasterTile2D,
    TileInformation,
};
use geoengine_datatypes::util::FiniteF64;
use rayon::iter::{IndexedParallelIterator, ParallelIterator};
use rayon::slice::ParallelSliceMut;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;

/// An operator that rasterizes (multi) polygons.
///
/// A pixel is covered by a polygon if its center lies inside the polygon.
/// Pixels that are not covered by any polygon are no-data.
pub type PolygonRasterization = Operator<PolygonRasterizationParams, SingleVectorSource>;

impl OperatorName for PolygonRasterization {
    const TYPE_NAME: &'static str = "PolygonRasterization";
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolygonRasterizationParams {
    /// The pixel size of the output raster.
    ///
    /// The rows run from north to south, i.e., the geo transform is `(origin_coordinate, x, -y)`.
    pub spatial_resolution: SpatialResolution,
    /// A pixel corner to which the pixel grid is aligned
    #[serde(default)]
    pub origin_coordinate: Coordinate2D,
    /// The value that a polygon burns into the pixels it covers
    #[serde(default)]
    pub value: BurnValue,
    /// The data type of the output raster, defaults to a type that fits the value
    #[serde(default)]
    pub output_type: Option<RasterDataType>,
    /// The measurement of the output band, derived from the value if not set
    #[serde(default)]
    pub output_measurement: Option<Measurement>,
    /// How the time slices of the output raster are derived from the features
    #[serde(default)]
    pub time: RasterizationTime,
}

/// The value that a polygon burns into the pixels it covers
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum BurnValue {
    /// Each covered pixel gets this value
    Constant { value: FiniteF64 },
    /// Each pixel gets the number of polygons that cover it
    Count,
    /// Each pixel gets the value of a numeric column, aggregated over all polygons that cover it.
    #[serde(rename_all = "camelCase")]
    Attribute {
        column: String,
        aggregation: OverlapAggregation,
        /// If `true` (default), features with a null value are skipped.
        /// Otherwise, they make all pixels they cover no-data.
        #[serde(default = "default_ignore_no_data")]
        ignore_no_data: bool,
    },
}

fn default_ignore_no_data() -> bool {
    true
}

impl Default for BurnValue {
    fn default() -> Self {
        Self::Constant {
            value: FiniteF64::new_unchecked(1.),
        }
    }
}

/// How the values of overlapping polygons are combined
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OverlapAggregation {
    First,
    Last,
    Min,
    Max,
    Sum,
    Mean,
    /// The median, which is exact for up to five values and estimated by the P² algorithm for more
    MedianEstimate,
}

/// How the time slices of the output raster are derived from the features
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum RasterizationTime {
    /// Every start and end time of a feature is a slice boundary, i.e., the slices are the
    /// intervals between consecutive boundaries.
    /// For example, features valid during `[0, 10)` and `[5, 20)` yield the slices `[0, 5)`,
    /// `[5, 10)` and `[10, 20)`, plus the slices without features before and after them.
    #[default]
    FeatureTimes,
    /// Regular time slices of length `step`, aligned to `reference` (default: Unix epoch).
    /// Each slice contains all features whose time interval intersects it.
    Regular {
        step: TimeStep,
        reference: Option<TimeInstance>,
    },
    /// The times of the features are ignored, and all features are rasterized into one
    /// time slice that is valid forever
    Ignore,
}

/// How many slice boundaries one scan of the vector input collects for [`RasterizationTime::FeatureTimes`]
const TIME_SCAN_BATCH_SIZE: usize = 1024;

#[async_trait]
impl RasterOperator for PolygonRasterization {
    async fn _initialize(
        self: Box<Self>,
        path: ProcessingGraphOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        Ok(self
            .initialize_with_time_scan_batch_size(path, context, TIME_SCAN_BATCH_SIZE)
            .await?
            .boxed())
    }

    span_fn!(PolygonRasterization);
}

impl PolygonRasterization {
    async fn initialize_with_time_scan_batch_size(
        self: Box<Self>,
        path: ProcessingGraphOperatorPath,
        context: &dyn ExecutionContext,
        time_scan_batch_size: usize,
    ) -> Result<InitializedPolygonRasterization> {
        ensure!(
            time_scan_batch_size > 0,
            error::InvalidOperatorSpec {
                reason: "The time scan batch size must be positive".to_string(),
            }
        );

        let name = CanonicOperatorName::from(&self);

        let source = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?
            .vector;
        let in_desc = source.result_descriptor();

        ensure!(
            in_desc.data_type == VectorDataType::MultiPolygon,
            error::InvalidType {
                expected: VectorDataType::MultiPolygon.to_string(),
                found: in_desc.data_type.to_string(),
            }
        );

        let band = output_band(&self.params, in_desc)?;

        let data_type = self.params.output_type.unwrap_or(match self.params.value {
            BurnValue::Constant { .. } => RasterDataType::U8,
            BurnValue::Count => RasterDataType::U32,
            BurnValue::Attribute { .. } => RasterDataType::F64,
        });

        if let BurnValue::Constant { value } = self.params.value {
            ensure!(
                data_type.is_valid(value.into()),
                error::InvalidOperatorSpec {
                    reason: format!("The constant value {value} is not a valid {data_type:?}."),
                }
            );
        }

        let spatial_bounds = in_desc
            .bbox
            .or_else(|| {
                in_desc
                    .spatial_reference
                    .as_option()
                    .and_then(|srs| srs.area_of_use_projected::<BoundingBox2D>().ok())
            })
            .ok_or(Error::NoSpatialBoundsAvailable)?;

        let resolution = self.params.spatial_resolution;
        let geo_transform = GeoTransform::new(
            self.params.origin_coordinate,
            resolution.x().get(),
            -resolution.y().get(),
        )?;
        let pixel_bounds =
            geo_transform.spatial_to_grid_bounds(&SpatialPartition2D::new_unchecked(
                spatial_bounds.upper_left(),
                spatial_bounds.lower_right(),
            ));

        let time = output_time(self.params.time, in_desc.time);

        let result_descriptor = RasterResultDescriptor {
            data_type,
            spatial_reference: in_desc.spatial_reference,
            time,
            spatial_grid: SpatialGridDescriptor::source_from_parts(geo_transform, pixel_bounds),
            bands: RasterBandDescriptors::new(vec![band])?,
        };

        Ok(InitializedPolygonRasterization {
            name,
            path,
            params: self.params,
            source,
            result_descriptor,
            spatial_bounds,
            time_scan_batch_size,
        })
    }
}

/// The band of the output raster with the explicit or a derived measurement.
fn output_band(
    params: &PolygonRasterizationParams,
    in_desc: &VectorResultDescriptor,
) -> Result<RasterBandDescriptor> {
    let (name, derived_measurement) = match &params.value {
        BurnValue::Constant { .. } => ("band".to_string(), Measurement::Unitless),
        BurnValue::Count => (
            "count".to_string(),
            Measurement::continuous("count".to_string(), None),
        ),
        BurnValue::Attribute {
            column,
            aggregation,
            ..
        } => {
            let column_info =
                in_desc
                    .columns
                    .get(column)
                    .ok_or_else(|| Error::ColumnDoesNotExist {
                        column: column.clone(),
                    })?;

            ensure!(
                matches!(
                    column_info.data_type,
                    FeatureDataType::Int | FeatureDataType::Float | FeatureDataType::Category
                ),
                error::InvalidType {
                    expected: "a numeric column (int, float or category)".to_string(),
                    found: format!("{:?}", column_info.data_type),
                }
            );

            (
                column.clone(),
                derive_attribute_measurement(&column_info.measurement, *aggregation),
            )
        }
    };

    Ok(RasterBandDescriptor::new(
        name,
        params
            .output_measurement
            .clone()
            .unwrap_or(derived_measurement),
    ))
}

/// The time descriptor of the output raster for the time bounds of the input.
fn output_time(time: RasterizationTime, input_bounds: Option<TimeInterval>) -> TimeDescriptor {
    match time {
        RasterizationTime::FeatureTimes => TimeDescriptor::new_irregular(input_bounds),
        RasterizationTime::Regular { step, reference } => TimeDescriptor::new_regular(
            input_bounds,
            reference.unwrap_or(TimeInstance::EPOCH_START),
            step,
        ),
        RasterizationTime::Ignore => TimeDescriptor::new_irregular(Some(TimeInterval::default())),
    }
}

/// The measurement of an attribute value after aggregating it.
///
/// `first`, `last`, `min` and `max` select one of the values, so the measurement of the column is kept.
/// `sum`, `mean` and `median` compute new values, which are no longer classes.
fn derive_attribute_measurement(
    column_measurement: &Measurement,
    aggregation: OverlapAggregation,
) -> Measurement {
    match aggregation {
        OverlapAggregation::First
        | OverlapAggregation::Last
        | OverlapAggregation::Min
        | OverlapAggregation::Max => column_measurement.clone(),
        OverlapAggregation::Sum | OverlapAggregation::Mean | OverlapAggregation::MedianEstimate => {
            match column_measurement {
                Measurement::Continuous(_) => column_measurement.clone(),
                Measurement::Unitless | Measurement::Classification(_) => Measurement::Unitless,
            }
        }
    }
}

pub struct InitializedPolygonRasterization {
    name: CanonicOperatorName,
    path: ProcessingGraphOperatorPath,
    params: PolygonRasterizationParams,
    source: Box<dyn InitializedVectorOperator>,
    result_descriptor: RasterResultDescriptor,
    spatial_bounds: BoundingBox2D,
    time_scan_batch_size: usize,
}

impl InitializedRasterOperator for InitializedPolygonRasterization {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let input = self
            .source
            .query_processor()?
            .multi_polygon()
            .ok_or_else(|| Error::InvalidType {
                expected: VectorDataType::MultiPolygon.to_string(),
                found: self.source.result_descriptor().data_type.to_string(),
            })?;

        let shared = Arc::new(SharedQueryState {
            input,
            result_descriptor: self.result_descriptor.clone(),
            value: self.params.value.clone(),
            time: self.params.time,
            spatial_bounds: self.spatial_bounds,
            time_scan_batch_size: self.time_scan_batch_size,
        });

        Ok(crate::call_generic_raster_processor!(
            self.result_descriptor.data_type,
            PolygonRasterizationQueryProcessor::create_boxed(shared.clone())
        ))
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        PolygonRasterization::TYPE_NAME
    }

    fn path(&self) -> ProcessingGraphOperatorPath {
        self.path.clone()
    }

    fn optimize(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<TypedRasterOperator, OptimizationError> {
        self.ensure_resolution_is_compatible_for_optimization(target_resolution)?;

        // the target resolution is not finer than the rasterization resolution, as ensured above
        let spatial_resolution = find_next_best_overview_level_resolution(
            self.result_descriptor.spatial_grid.spatial_resolution(),
            target_resolution,
        );

        Ok(PolygonRasterization {
            params: PolygonRasterizationParams {
                spatial_resolution,
                ..self.params.clone()
            },
            sources: SingleVectorSource {
                vector: self.source.optimize(target_resolution)?,
            },
        }
        .typed())
    }
}

/// The parts of the query processor that do not depend on the output pixel type
struct SharedQueryState {
    input: Box<dyn VectorQueryProcessor<VectorType = MultiPolygonCollection>>,
    result_descriptor: RasterResultDescriptor,
    value: BurnValue,
    time: RasterizationTime,
    /// The bounds of the input, used to scan all features for their times
    spatial_bounds: BoundingBox2D,
    time_scan_batch_size: usize,
}

pub struct PolygonRasterizationQueryProcessor<T> {
    shared: Arc<SharedQueryState>,
    _pixel_type: PhantomData<T>,
}

impl<T: Pixel> PolygonRasterizationQueryProcessor<T> {
    fn create_boxed(shared: Arc<SharedQueryState>) -> BoxRasterQueryProcessor<T> {
        Self {
            shared,
            _pixel_type: PhantomData,
        }
        .boxed()
    }
}

#[async_trait]
impl<T: Pixel> QueryProcessor for PolygonRasterizationQueryProcessor<T> {
    type Output = RasterTile2D<T>;
    type SpatialBounds = GridBoundingBox2D;
    type Selection = BandSelection;
    type ResultDescription = RasterResultDescriptor;

    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let tiling_grid = self
            .shared
            .result_descriptor
            .tiling_grid_definition(ctx.tiling_specification());
        let tile_infos = tiling_grid
            .generate_data_tiling_strategy()
            .tile_information_iterator_from_pixel_bounds(query.spatial_bounds());

        let time_slices = self.time_query(query.time_interval(), ctx).await?;

        let tiles = time_slices
            .map_ok(move |time_slice| {
                stream::iter(
                    tile_infos
                        .clone()
                        .map(move |tile_info| Ok((time_slice, tile_info))),
                )
            })
            .try_flatten()
            .and_then(move |(time_slice, tile_info)| {
                self.rasterize_tile(time_slice, tile_info, ctx)
            });

        Ok(tiles.boxed())
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.shared.result_descriptor
    }
}

#[async_trait]
impl<T: Pixel> RasterQueryProcessor for PolygonRasterizationQueryProcessor<T> {
    type RasterType = T;

    async fn _time_query<'a>(
        &'a self,
        query: TimeInterval,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<TimeInterval>>> {
        match self.shared.time {
            RasterizationTime::FeatureTimes => Ok(self.shared.feature_time_slices(query, ctx)),
            RasterizationTime::Regular { .. } => {
                let regular_time = self
                    .shared
                    .result_descriptor
                    .time
                    .dimension
                    .unwrap_regular()
                    .ok_or_else(|| Error::InvalidOperatorSpec {
                        reason: "Regular rasterization must have a regular time dimension"
                            .to_string(),
                    })?;
                Ok(stream::iter(regular_time.intersecting_intervals(query)?.map(Ok)).boxed())
            }
            RasterizationTime::Ignore => {
                Ok(stream::once(async { Ok(TimeInterval::default()) }).boxed())
            }
        }
    }
}

impl<T: Pixel> PolygonRasterizationQueryProcessor<T> {
    async fn rasterize_tile(
        &self,
        time_slice: TimeInterval,
        tile_info: TileInformation,
        ctx: &dyn QueryContext,
    ) -> Result<RasterTile2D<T>> {
        let shape = tile_info.tile_size_in_pixels();

        let (values, cache_hint) = self.shared.burn_value(time_slice, tile_info, ctx).await?;

        let grid_array = match values {
            Some(values) => {
                spawn_blocking_with_thread_pool(ctx.thread_pool().clone(), move || {
                    let values: Vec<Option<T>> = values
                        .into_iter()
                        .map(|value| value.map(T::from_))
                        .collect();
                    values_to_grid(shape, &values)
                })
                .await??
            }
            None => GridOrEmpty::new_empty_shape(shape),
        };

        Ok(RasterTile2D::new_with_tile_info(
            time_slice, tile_info, 0, grid_array, cache_hint,
        ))
    }
}

impl SharedQueryState {
    /// Burns the features into the pixels of the tile with the aggregator of the configured value.
    async fn burn_value(
        &self,
        time_slice: TimeInterval,
        tile_info: TileInformation,
        ctx: &dyn QueryContext,
    ) -> Result<(Option<Vec<Option<f64>>>, CacheHint)> {
        let (aggregation, ignore_no_data) = match &self.value {
            BurnValue::Constant { value } => {
                return self
                    .burn(
                        ConstantAggregator((*value).into()),
                        time_slice,
                        tile_info,
                        ctx,
                    )
                    .await;
            }
            BurnValue::Count => {
                return self
                    .burn(CountAggregator::default(), time_slice, tile_info, ctx)
                    .await;
            }
            BurnValue::Attribute {
                aggregation,
                ignore_no_data,
                ..
            } => (*aggregation, *ignore_no_data),
        };

        macro_rules! burn_attribute {
            ($aggregator:expr) => {
                self.burn_attribute($aggregator, ignore_no_data, time_slice, tile_info, ctx)
                    .await
            };
        }

        match aggregation {
            OverlapAggregation::First => burn_attribute!(FirstAggregator::default()),
            OverlapAggregation::Last => burn_attribute!(LastAggregator::default()),
            OverlapAggregation::Min => burn_attribute!(MinAggregator::default()),
            OverlapAggregation::Max => burn_attribute!(MaxAggregator::default()),
            OverlapAggregation::Sum => burn_attribute!(SumAggregator::default()),
            OverlapAggregation::Mean => burn_attribute!(MeanAggregator),
            // the estimator is much larger than a value, and most pixels of a tile are usually not covered
            OverlapAggregation::MedianEstimate => {
                burn_attribute!(Boxed(PercentileEstimateAggregator::new(0.5)))
            }
        }
    }

    /// Burns the attribute values with `aggregator`.
    ///
    /// Unless `ignore_no_data` is set, a feature with a null value makes all pixels it covers no-data.
    async fn burn_attribute<A: PixelAggregator<f64> + 'static>(
        &self,
        aggregator: A,
        ignore_no_data: bool,
        time_slice: TimeInterval,
        tile_info: TileInformation,
        ctx: &dyn QueryContext,
    ) -> Result<(Option<Vec<Option<f64>>>, CacheHint)> {
        if ignore_no_data {
            self.burn(aggregator, time_slice, tile_info, ctx).await
        } else {
            self.burn(PropagateNoData(aggregator), time_slice, tile_info, ctx)
                .await
        }
    }

    /// Burns all features that intersect the tile and the time slice into the pixels of the tile.
    ///
    /// Returns `None` if no feature intersects the tile.
    async fn burn<A: PixelAggregator<f64> + 'static>(
        &self,
        aggregator: A,
        time_slice: TimeInterval,
        tile_info: TileInformation,
        ctx: &dyn QueryContext,
    ) -> Result<(Option<Vec<Option<f64>>>, CacheHint)> {
        let (vector_time, feature_time_filter) = match self.time {
            RasterizationTime::Ignore => (TimeInterval::default(), None),
            RasterizationTime::FeatureTimes | RasterizationTime::Regular { .. } => {
                (time_slice, Some(time_slice))
            }
        };
        let (column, ignore_no_data): (Option<Arc<str>>, bool) = match &self.value {
            BurnValue::Attribute {
                column,
                ignore_no_data,
                ..
            } => (Some(column.as_str().into()), *ignore_no_data),
            BurnValue::Constant { .. } | BurnValue::Count => (None, true),
        };

        let vector_query = VectorQueryRectangle::new(
            tile_info.spatial_partition().as_bbox(),
            vector_time,
            ColumnSelection::all(),
        );
        let mut chunks = self.input.query(vector_query, ctx).await?;

        let aggregator = Arc::new(aggregator);
        let mut states: Option<Vec<A::State>> = None;
        let mut cache_hint = CacheHint::max_duration();

        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            cache_hint.merge_with(&chunk.cache_hint);

            let aggregator = aggregator.clone();
            let column = column.clone();
            states = spawn_blocking_with_thread_pool(ctx.thread_pool().clone(), move || {
                burn_chunk(
                    aggregator.as_ref(),
                    states,
                    &chunk,
                    &tile_info,
                    feature_time_filter,
                    column.as_deref(),
                    ignore_no_data,
                )
            })
            .await??;
        }

        let Some(states) = states else {
            return Ok((None, cache_hint));
        };

        let values = spawn_blocking_with_thread_pool(ctx.thread_pool().clone(), move || {
            finish_states(aggregator.as_ref(), &states)
        })
        .await?;

        Ok((Some(values), cache_hint))
    }

    /// The time slices between consecutive start and end times of all features.
    ///
    /// The vector stream is not sorted by time, so the next slice boundary is only known after
    /// reading all features.
    /// To keep the memory bounded, each scan of the input collects only the next
    /// `time_scan_batch_size` boundaries and the input is scanned again for the following ones.
    fn feature_time_slices<'a>(
        &'a self,
        query: TimeInterval,
        ctx: &'a dyn QueryContext,
    ) -> BoxStream<'a, Result<TimeInterval>> {
        stream::try_unfold(
            Some(None),
            move |cursor: Option<Option<TimeInstance>>| async move {
                // `None`: done, `Some(None)`: first scan, `Some(Some(t))`: the next slice starts at `t`
                let Some(cursor) = cursor else {
                    return Ok::<_, Error>(None);
                };

                let (slice_start, scan) = match cursor {
                    None => {
                        // the first slice starts at the latest boundary at or before the query start
                        let scan = self
                            .scan_boundaries(
                                TimeInterval::default(),
                                query.start(),
                                Some(query.start()),
                                ctx,
                            )
                            .await?;
                        (scan.last_at_or_before.unwrap_or(TimeInstance::MIN), scan)
                    }
                    Some(start) => {
                        // only features that end after `start` have boundaries after it
                        let scan = self
                            .scan_boundaries(
                                TimeInterval::new_unchecked(start, TimeInstance::MAX),
                                start,
                                None,
                                ctx,
                            )
                            .await?;
                        (start, scan)
                    }
                };

                let exhausted = scan.next.len() < self.time_scan_batch_size;

                let mut slices = Vec::with_capacity(scan.next.len() + 1);
                let mut start = slice_start;
                for &end in &scan.next {
                    slices.push(TimeInterval::new_unchecked(start, end));
                    start = end;
                }
                if exhausted {
                    slices.push(TimeInterval::new_unchecked(start, TimeInstance::MAX));
                }

                // the slices are contiguous, so all slices after the first one that does not intersect
                // the query are beyond the query
                let intersecting: Vec<TimeInterval> = slices
                    .into_iter()
                    .take_while(|slice| slice.intersects(&query))
                    .collect();

                let next_cursor = if exhausted
                    || !TimeInterval::new_unchecked(start, TimeInstance::MAX).intersects(&query)
                {
                    None
                } else {
                    Some(Some(start))
                };

                Ok(Some((
                    stream::iter(intersecting.into_iter().map(Ok)),
                    next_cursor,
                )))
            },
        )
        .try_flatten()
        .boxed()
    }

    /// Scans the features of `scan_time` for their start and end times.
    async fn scan_boundaries(
        &self,
        scan_time: TimeInterval,
        after: TimeInstance,
        at_or_before: Option<TimeInstance>,
        ctx: &dyn QueryContext,
    ) -> Result<BoundaryScan> {
        let vector_query =
            VectorQueryRectangle::new(self.spatial_bounds, scan_time, ColumnSelection::all());
        let mut chunks = self.input.query(vector_query, ctx).await?;

        let mut scan = BoundaryScan {
            last_at_or_before: None,
            next: BTreeSet::new(),
        };

        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            for time in chunk.time_intervals() {
                for boundary in [time.start(), time.end()] {
                    if boundary == TimeInstance::MIN || boundary == TimeInstance::MAX {
                        continue;
                    }

                    if let Some(at_or_before) = at_or_before
                        && boundary <= at_or_before
                    {
                        scan.last_at_or_before = scan.last_at_or_before.max(Some(boundary));
                    }

                    if boundary > after {
                        scan.insert_bounded(boundary, self.time_scan_batch_size);
                    }
                }
            }
        }

        Ok(scan)
    }
}

/// The result of one scan for time slice boundaries
struct BoundaryScan {
    /// The latest boundary at or before the start of the query
    last_at_or_before: Option<TimeInstance>,
    /// The smallest boundaries after the cursor
    next: BTreeSet<TimeInstance>,
}

impl BoundaryScan {
    fn insert_bounded(&mut self, boundary: TimeInstance, max_len: usize) {
        if self.next.len() < max_len {
            self.next.insert(boundary);
        } else if self.next.last().is_some_and(|&last| boundary < last)
            && self.next.insert(boundary)
        {
            self.next.pop_last();
        }
    }
}

/// A feature of a chunk that covers pixels of the tile
struct FeatureToBurn<'c> {
    geometry: MultiPolygonRef<'c>,
    /// `None` for no-data
    value: Option<f64>,
    rows: Range<usize>,
}

/// Burns the features of `chunk` into `states`, creating them on the first covered pixel.
fn burn_chunk<A: PixelAggregator<f64>>(
    aggregator: &A,
    states: Option<Vec<A::State>>,
    chunk: &MultiPolygonCollection,
    tile_info: &TileInformation,
    time_filter: Option<TimeInterval>,
    column: Option<&str>,
    ignore_no_data: bool,
) -> Result<Option<Vec<A::State>>> {
    let geo_transform = &tile_info.tile_geo_transform();
    let shape = tile_info.tile_size_in_pixels;

    let values: Option<Vec<Option<f64>>> = match column {
        Some(column) => Some(chunk.data(column)?.float_options_iter().collect()),
        None => None,
    };

    let mut features = Vec::new();
    for (index, (geometry, time)) in chunk.geometries().zip(chunk.time_intervals()).enumerate() {
        if time_filter.is_some_and(|time_filter| !time.intersects(&time_filter)) {
            continue;
        }

        let value = match &values {
            Some(values) => match values[index] {
                Some(value) if value.is_finite() => Some(value),
                // null (or non-finite) values are no-data, which covers no pixel if it is ignored
                _ if ignore_no_data => continue,
                _ => None,
            },
            None => Some(1.),
        };

        let Some(bbox) = geometry.bbox() else {
            continue;
        };
        let rows = rows_within(geo_transform, shape, &bbox);
        if rows.is_empty() || cols_within(geo_transform, shape, &bbox).is_empty() {
            continue;
        }

        features.push(FeatureToBurn {
            geometry,
            value,
            rows,
        });
    }

    if features.is_empty() {
        return Ok(states);
    }

    let mut states = states.unwrap_or_else(|| {
        std::iter::repeat_with(A::State::default)
            .take(shape.number_of_elements())
            .collect()
    });

    states
        .par_chunks_mut(shape.axis_size_x())
        .enumerate()
        .for_each_init(
            || (Vec::new(), Vec::new()),
            |(crossings, spans), (row, row_states)| {
                for feature in &features {
                    if !feature.rows.contains(&row) {
                        continue;
                    }

                    row_spans(
                        geo_transform,
                        shape,
                        row,
                        feature.geometry.polygons(),
                        crossings,
                        spans,
                    );

                    for span in spans.iter() {
                        for state in &mut row_states[span.clone()] {
                            aggregator.add(state, feature.value);
                        }
                    }
                }
            },
        );

    Ok(Some(states))
}

#[cfg(test)]
mod tests;
