use super::gdal_worker_process::{GdalDatasetParameters, GdalPoolDispatcher, GridAndProperties};
use crate::engine::{
    CanonicOperatorName, MetaData, OperatorData, OperatorName, QueryProcessor,
    SpatialGridDescriptor, WorkflowOperatorPath,
};
use crate::optimization::{OptimizableOperator, OptimizationError, SourcesMustNotUseOverviews};
use crate::source::gdal_source::reader::GdalPoolReader;
use crate::source::gdal_worker_process::{GdalReaderMode, OverviewReaderState, ReaderState};
use crate::{
    engine::{
        InitializedRasterOperator, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
        SourceOperator, TypedRasterQueryProcessor,
    },
    util::Result,
};
use async_trait::async_trait;
pub use error::GdalSourceError;
use futures::{Future, TryFutureExt, TryStreamExt};
use futures::{
    Stream,
    stream::{self, BoxStream, StreamExt},
};
use gdal::raster::GdalType;
use geoengine_datatypes::raster::ChangeGridBounds;
use geoengine_datatypes::{
    dataset::NamedData,
    primitives::{
        BandSelection, CacheHint, RasterQueryRectangle, SpatialResolution, TimeInterval,
        TryIrregularTimeFillIterExt, TryRegularTimeFillIterExt, find_next_best_overview_level,
    },
    raster::{
        EmptyGrid, GeoTransform, GridBoundingBox2D, Pixel, RasterDataType, RasterProperties,
        RasterTile2D, SpatialGridDefinition, TileInformation, TilingSpecification, TilingStrategy,
    },
};
use itertools::Itertools;
pub use loading_info::{
    GdalLoadingInfo, GdalLoadingInfoTemporalSlice, GdalLoadingInfoTemporalSliceIterator,
    GdalMetaDataList, GdalMetaDataRegular, GdalMetaDataStatic, GdalMetadataNetCdfCf,
};
use num::FromPrimitive;
use num::integer::div_floor;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::marker::PhantomData;
use tracing::{debug, warn};

mod db_types;
pub mod error;
pub mod loading_info;
mod reader;

/// Parameters for the GDAL Source Operator
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GdalSourceParameters {
    pub data: NamedData,
    #[serde(default)]
    pub overview_level: Option<u32>, // TODO: should also allow a resolution? Add resample method?
}

impl GdalSourceParameters {
    #[must_use]
    pub fn new(data: NamedData) -> Self {
        Self {
            data,
            overview_level: None,
        }
    }

    #[must_use]
    pub fn new_with_overview_level(data: NamedData, overview_level: u32) -> Self {
        Self {
            data,
            overview_level: Some(overview_level),
        }
    }

    #[must_use]
    pub fn with_overview_level(mut self, overview_level: Option<u32>) -> Self {
        self.overview_level = overview_level;
        self
    }
}

impl OperatorData for GdalSourceParameters {
    fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
        data_names.push(self.data.clone());
    }
}

type GdalMetaData =
    Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>;

pub struct GdalRasterLoader;

impl GdalRasterLoader {
    pub async fn load_tile_grid_props<T: Pixel + GdalType + FromPrimitive>(
        dataset_params: GdalDatasetParameters,
        reader_mode: GdalReaderMode,
        tile_information: TileInformation,
        gdal_worker: GdalPoolDispatcher,
    ) -> Result<Option<GridAndProperties<T, GridBoundingBox2D>>, GdalSourceError> {
        tracing::trace!(
            "Loading tile {:?}, from {}, band: {}",
            &tile_information,
            dataset_params.file_path.display(),
            dataset_params.rasterband_channel
        );

        let ds_spatial_grid = dataset_params.spatial_grid_definition();
        let tile_spatial_grid = tile_information.spatial_grid_definition();
        tracing::trace!(
            "ds_spatial_grid: {:?}, tile_spatial_grid {:?}",
            ds_spatial_grid,
            tile_spatial_grid
        );
        let gdal_read_advise =
            reader_mode.tiling_to_dataset_read_advise(&ds_spatial_grid, &tile_spatial_grid);

        let Some(gdal_read_advise) = gdal_read_advise else {
            tracing::trace!(
                "Tile {:?} not intersecting dataset grid or gdal grid {:?}",
                &tile_information,
                dataset_params.file_path
            );
            return Ok(None);
        };

        let res = GdalPoolReader::from(gdal_worker)
            .load_tile_data_process(dataset_params, gdal_read_advise)
            .await?;

        Ok(Some(res))
    }

    /// Load a single tile using the process manager.
    async fn load_tile_async<T: Pixel + GdalType + FromPrimitive>(
        dataset_params: Option<GdalDatasetParameters>,
        reader_mode: GdalReaderMode,
        tile_information: TileInformation,
        tile_time: TimeInterval,
        cache_hint: CacheHint,
        gdal_worker: GdalPoolDispatcher,
    ) -> Result<RasterTile2D<T>, GdalSourceError> {
        match dataset_params {
            // TODO: discuss if we need this check here. The metadata provider should only pass on loading infos if the query intersects the datasets bounds! And the tiling strategy should only generate tiles that intersect the querys bbox.
            Some(ds) => {
                let grid_and_props =
                    Self::load_tile_grid_props(ds, reader_mode, tile_information, gdal_worker)
                        .await?;
                let tile = match grid_and_props {
                    Some(gp) => RasterTile2D::new_with_tile_info_and_properties(
                        tile_time,
                        tile_information,
                        0,
                        gp.grid.unbounded(),
                        gp.properties,
                        cache_hint,
                    ),
                    None => {
                        debug!(
                            "Tile {:?} not intersecting dataset grid or gdal grid",
                            &tile_information.global_tile_position
                        );
                        Self::create_no_data_tile(tile_information, tile_time, cache_hint)
                    }
                };
                Ok(tile)
            }
            _ => {
                debug!(
                    "Skipping tile without GdalDatasetParameters {:?}",
                    &tile_information
                );

                Ok(Self::create_no_data_tile(
                    tile_information,
                    tile_time,
                    cache_hint,
                ))
            }
        }
    }

    fn create_no_data_tile<T: Pixel>(
        tile_info: TileInformation,
        tile_time: TimeInterval,
        cache_hint: CacheHint,
    ) -> RasterTile2D<T> {
        RasterTile2D::new_with_tile_info_and_properties(
            tile_time,
            tile_info,
            0,
            EmptyGrid::new(tile_info.tile_size_in_pixels).into(),
            RasterProperties::default(),
            cache_hint,
        )
    }
}

// This is where the source attaches!
/// A stream of futures producing `RasterTile2D` for a single slice in time
fn temporal_slice_tile_future_stream<T: Pixel + GdalType + FromPrimitive>(
    spatial_bounds: GridBoundingBox2D,
    info: GdalLoadingInfoTemporalSlice,
    tiling_strategy: TilingStrategy,
    reader_mode: GdalReaderMode,
    gdal_worker: GdalPoolDispatcher,
) -> impl Stream<Item = impl Future<Output = Result<RasterTile2D<T>>>> + use<T> {
    stream::iter(tiling_strategy.tile_information_iterator_from_pixel_bounds(spatial_bounds)).map(
        move |tile| {
            GdalRasterLoader::load_tile_async(
                info.params.clone(),
                reader_mode,
                tile,
                info.time,
                info.cache_ttl.into(),
                gdal_worker.clone(),
            )
            .map_err(Into::into)
        },
    )
}

pub struct GdalSourceProcessor<T>
where
    T: Pixel,
{
    pub produced_result_descriptor: RasterResultDescriptor,
    pub tiling_specification: TilingSpecification,
    pub meta_data: GdalMetaData,
    pub overview_level: u32,
    pub original_resolution_spatial_grid: Option<SpatialGridDefinition>,
    pub _phantom_data: PhantomData<T>,
}

impl<T> GdalSourceProcessor<T>
where
    T: gdal::raster::GdalType + Pixel,
{
    pub fn new(
        produced_result_descriptor: RasterResultDescriptor,
        tiling_specification: TilingSpecification,
        meta_data: GdalMetaData,
        overview_level: u32,
        original_resolution_spatial_grid: Option<SpatialGridDefinition>,
    ) -> Self {
        Self {
            produced_result_descriptor,
            tiling_specification,
            meta_data,
            overview_level,
            original_resolution_spatial_grid,
            _phantom_data: PhantomData,
        }
    }

    pub fn new_no_overview(
        produced_result_descriptor: RasterResultDescriptor,
        tiling_specification: TilingSpecification,
        meta_data: GdalMetaData,
    ) -> Self {
        Self::new(
            produced_result_descriptor,
            tiling_specification,
            meta_data,
            0,
            None,
        )
    }
}

#[async_trait]
impl<P> QueryProcessor for GdalSourceProcessor<P>
where
    P: Pixel + gdal::raster::GdalType + FromPrimitive,
{
    type Output = RasterTile2D<P>;
    type SpatialBounds = GridBoundingBox2D;
    type Selection = BandSelection;
    type ResultDescription = RasterResultDescriptor;

    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<BoxStream<Result<Self::Output>>> {
        ensure!(
            query.attributes().as_slice() == [0],
            crate::error::GdalSourceDoesNotSupportQueryingOtherBandsThanTheFirstOneYet
        );
        tracing::trace!(
            "Querying GdalSourceProcessor<{:?}> with: {:?}.",
            P::TYPE,
            &query
        );
        // this is the result descriptor of the operator. It already incorporates the overview level AND shifts the origin to the tiling origin
        let result_descriptor = self.result_descriptor();

        let grid_produced_by_source_desc = result_descriptor.spatial_grid;
        let grid_produced_by_source = grid_produced_by_source_desc
            .source_spatial_grid_definition()
            .expect("the source grid definition should be present in a source...");
        // A `GeoTransform` maps pixel space to world space.
        // Usually a SRS has axis directions pointing "up" (y-axis) and "up" (y-axis).
        // We are not aware of spatial reference systems where the x-axis points to the right.
        // However, there are spatial reference systems where the y-axis points downwards.
        // The standard "pixel-space" starts at the top-left corner of a `GeoTransform` and points down-right.
        // Therefore, the pixel size on the x-axis is always increasing
        let pixel_size_x = grid_produced_by_source.geo_transform().x_pixel_size();
        debug_assert!(pixel_size_x.is_sign_positive());
        // and the y-axis should only be positive if the y-axis of the spatial reference system also "points down".
        // NOTE: at the moment we do not allow "down pointing" y-axis.
        let pixel_size_y = grid_produced_by_source.geo_transform().y_pixel_size();
        debug_assert!(pixel_size_y.is_sign_negative());

        // The data origin is not neccessarily the origin of the tileing we want to use.
        // TODO: maybe derive tilling origin reference from the data projection
        let produced_tiling_grid =
            grid_produced_by_source_desc.tiling_grid_definition(self.tiling_specification);

        let tiling_strategy = produced_tiling_grid.generate_data_tiling_strategy();

        let reader_mode = match self.original_resolution_spatial_grid {
            None => GdalReaderMode::OriginalResolution(ReaderState {
                dataset_spatial_grid: grid_produced_by_source,
            }),
            Some(original_resolution_spatial_grid) => {
                GdalReaderMode::OverviewLevel(OverviewReaderState {
                    original_dataset_grid: original_resolution_spatial_grid,
                    overview_level: self.overview_level,
                })
            }
        };

        let loading_info = self.meta_data.loading_info(query.clone()).await?;

        tracing::trace!(
            "Reader mode: {:?}. Original grid: {:?} and target grid: {:?}. Loadinginfo  start_time_of_output_stream: {:?}, end_time_of_output_stream: {:?}",
            reader_mode,
            self.original_resolution_spatial_grid,
            produced_tiling_grid,
            loading_info.start_time_of_output_stream,
            loading_info.end_time_of_output_stream
        );

        debug_assert!(
            loading_info.start_time_of_output_stream < loading_info.end_time_of_output_stream,
            "Data time validity must not be a TimeInstance. Is ({:?}, {:?}]",
            loading_info.start_time_of_output_stream,
            loading_info.end_time_of_output_stream
        );

        let query_time = query.time_interval();
        let skipping_loading_info = loading_info
            .info
            .filter_ok(move |s: &GdalLoadingInfoTemporalSlice| s.time.intersects(&query_time)); // Check that the time slice intersects the query time

        let filled_loading_info_stream = match self.result_descriptor().time.dimension {
            geoengine_datatypes::primitives::TimeDimension::Regular(regular_time_dimension) => {
                let times_fill_iter = skipping_loading_info
                    .try_time_regular_range_fill(regular_time_dimension, query_time);

                stream::iter(times_fill_iter).boxed()
            }
            geoengine_datatypes::primitives::TimeDimension::Irregular => {
                let time_bounds = TimeInterval::new_unchecked(
                    loading_info
                        .start_time_of_output_stream
                        .expect("must exist"),
                    loading_info.end_time_of_output_stream.expect("must exist"),
                );
                let times_fill_iter =
                    skipping_loading_info.try_time_irregular_range_fill(time_bounds);

                stream::iter(times_fill_iter).boxed()
            }
        };

        let stream = filled_loading_info_stream
                        .inspect_ok(move |r| {
                            tracing::trace!("GdalSource _query now producing time slice: {:?}", r.time);
                            if !r.time.intersects(&query_time) {
                                warn!(
                                    "GdalSource _query producing time slice that does not intersect query time: {:?} vs {:?}",
                                    r.time, query_time
                                );
                            }
                        });

        let loaded_source_stream = load_source_stream::<P, _>(
            stream,
            query.spatial_bounds(),
            tiling_strategy,
            reader_mode,
            ctx.get_gdal_worker(),
        );

        Ok(loaded_source_stream.boxed())
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.produced_result_descriptor
    }
}

#[async_trait]
impl<T> RasterQueryProcessor for GdalSourceProcessor<T>
where
    T: Pixel + gdal::raster::GdalType + FromPrimitive,
{
    type RasterType = T;

    async fn _time_query<'a>(
        &'a self,
        query: TimeInterval,
        ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<BoxStream<'a, Result<TimeInterval>>> {
        let rdt = self.raster_result_descriptor().time;

        let q_bounds = self
            .raster_result_descriptor()
            .tiling_grid_definition(ctx.tiling_specification())
            .tiling_grid_bounds();
        let q_rect = RasterQueryRectangle::new(q_bounds, query, BandSelection::first());
        let ldif = self.meta_data.loading_info(q_rect).await?;
        let unique_times = ldif.info.map(|s| s.map(|s| s.time));

        let res_stream = match rdt.dimension {
            geoengine_datatypes::primitives::TimeDimension::Regular(regular_time_dimension) => {
                let times_fill_iter = unique_times.try_time_regular_range_fill(
                    regular_time_dimension,
                    TimeInterval::new_unchecked(
                        ldif.start_time_of_output_stream
                            .expect("start time must be set for irregular time dimension"), // TODO: make this a requirement of the meta data trait
                        ldif.end_time_of_output_stream
                            .expect("end time must be set for irregular time dimension"),
                    ),
                );
                stream::iter(times_fill_iter).boxed()
            }
            geoengine_datatypes::primitives::TimeDimension::Irregular => {
                let times_fill_iter =
                    unique_times.try_time_irregular_range_fill(TimeInterval::new_unchecked(
                        ldif.start_time_of_output_stream
                            .expect("start time must be set for regular time dimension"), // TODO: make this a requirement of the meta data trait
                        ldif.end_time_of_output_stream
                            .expect("end time must be set for regular time dimension"),
                    ));
                stream::iter(times_fill_iter).boxed()
            }
        };

        Ok(res_stream
            .inspect_ok(|ti| {
                tracing::trace!("GdalSource time_query producing time interval: {:?}", ti);
            })
            .boxed())
    }
}

pub type GdalSource = SourceOperator<GdalSourceParameters>;

impl OperatorName for GdalSource {
    const TYPE_NAME: &'static str = "GdalSource";
}

fn load_source_stream<P, S>(
    source_stream: S,
    spatial_query: GridBoundingBox2D,
    tiling_strategy: TilingStrategy,
    reader_mode: GdalReaderMode,
    gdal_worker: GdalPoolDispatcher,
) -> impl Stream<Item = Result<RasterTile2D<P>>> + use<P, S>
where
    P: Pixel + GdalType + FromPrimitive,
    S: Stream<Item = Result<GdalLoadingInfoTemporalSlice>>,
{
    source_stream
        .map_ok(move |info| {
            temporal_slice_tile_future_stream(
                spatial_query,
                info,
                tiling_strategy,
                reader_mode,
                gdal_worker.clone(),
            )
            .map(Result::Ok)
        })
        .try_flatten()
        .try_buffered(8) // TODO: make this configurable
}

fn overview_level_spatial_grid(
    source_spatial_grid: SpatialGridDefinition,
    overview_level: u32,
) -> Option<SpatialGridDefinition> {
    if overview_level > 0 {
        tracing::trace!("Using overview level {overview_level}");
        let geo_transform = GeoTransform::new(
            source_spatial_grid.geo_transform.origin_coordinate,
            source_spatial_grid.geo_transform.x_pixel_size() * f64::from(overview_level),
            source_spatial_grid.geo_transform.y_pixel_size() * f64::from(overview_level),
        );
        let grid_bounds = GridBoundingBox2D::new_min_max(
            div_floor(
                source_spatial_grid.grid_bounds.y_min(),
                overview_level as isize,
            ),
            div_floor(
                source_spatial_grid.grid_bounds.y_max(),
                overview_level as isize,
            ),
            div_floor(
                source_spatial_grid.grid_bounds.x_min(),
                overview_level as isize,
            ),
            div_floor(
                source_spatial_grid.grid_bounds.x_max(),
                overview_level as isize,
            ),
        )
        .expect("overview level must be a positive integer");

        Some(SpatialGridDefinition::new(geo_transform, grid_bounds))
    } else {
        tracing::trace!("Using original resolution (ov = 0)");
        None
    }
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for GdalSource {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let data_id = context.resolve_named_data(&self.params.data).await?;
        let meta_data: GdalMetaData = context.meta_data(&data_id).await?;

        tracing::trace!(
            "Initializing GdalSource for {:?}. GdalSource path: {:?}",
            &self.params.data,
            path
        );

        let meta_data_result_descriptor = meta_data.result_descriptor().await?;

        let op_name = CanonicOperatorName::from(&self);
        let op = if self.params.overview_level.is_none() {
            InitializedGdalSourceOperator::initialize_original_resolution(
                op_name,
                path,
                self.params.data,
                meta_data,
                meta_data_result_descriptor,
                context.tiling_specification(),
            )
        } else {
            // generate a result descriptor with the overview level
            InitializedGdalSourceOperator::initialize_with_overview_level(
                op_name,
                path,
                self.params.data,
                meta_data,
                meta_data_result_descriptor,
                context.tiling_specification(),
                self.params.overview_level.unwrap_or(0),
            )
        };

        Ok(op.boxed())
    }

    span_fn!(GdalSource);
}

#[derive(Clone)]
pub struct InitializedGdalSourceOperator {
    pub name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    pub meta_data: GdalMetaData,
    pub produced_result_descriptor: RasterResultDescriptor,
    pub tiling_specification: TilingSpecification,
    pub data_name: NamedData,
    // the overview level to use. 0/1 means the highest resolution
    pub overview_level: u32,
    pub original_resolution_spatial_grid: Option<SpatialGridDefinition>,
}

impl InitializedGdalSourceOperator {
    pub fn initialize_original_resolution(
        name: CanonicOperatorName,
        path: WorkflowOperatorPath,
        data_name: NamedData,
        meta_data: GdalMetaData,
        result_descriptor: RasterResultDescriptor,
        tiling_specification: TilingSpecification,
    ) -> Self {
        InitializedGdalSourceOperator {
            name,
            path,
            data_name,
            produced_result_descriptor: result_descriptor,
            meta_data,
            tiling_specification,
            overview_level: 0,
            original_resolution_spatial_grid: None,
        }
    }

    /// Initializes the operator with a result descriptor that incorporates the overview level. The original resolution spatial grid is stored in the operator for later use in the query processor when reading the data.
    /// If the overview level is 0, this method behaves the same as `initialize_original_resolution`.
    ///
    /// # Panics
    /// Panics if the result descriptor does not contain a source spatial grid definition or if the overview level is greater than 0 but the overview level spatial grid could not be created.
    /// The latter can only happen if the overview level is greater than 0 but the source spatial grid is smaller than the overview level, which should not happen in practice.
    pub fn initialize_with_overview_level(
        name: CanonicOperatorName,
        path: WorkflowOperatorPath,
        data_name: NamedData,
        meta_data: GdalMetaData,
        result_descriptor: RasterResultDescriptor,
        tiling_specification: TilingSpecification,
        overview_level: u32,
    ) -> Self {
        let source_resolution_spatial_grid = result_descriptor
            .spatial_grid_descriptor()
            .source_spatial_grid_definition()
            .expect("Source data must be a source grid definition...");

        let (result_descriptor, original_grid) = if let Some(ovr_spatial_grid) =
            overview_level_spatial_grid(source_resolution_spatial_grid, overview_level)
        {
            let ovr_res = RasterResultDescriptor {
                spatial_grid: SpatialGridDescriptor::new_source(ovr_spatial_grid),
                ..result_descriptor
            };
            (ovr_res, Some(source_resolution_spatial_grid))
        } else {
            (result_descriptor, None)
        };

        InitializedGdalSourceOperator {
            name,
            path,
            produced_result_descriptor: result_descriptor,
            meta_data,
            tiling_specification,
            data_name,
            overview_level,
            original_resolution_spatial_grid: original_grid,
        }
    }
}

impl InitializedRasterOperator for InitializedGdalSourceOperator {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.produced_result_descriptor
    }

    #[allow(clippy::too_many_lines)]
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        Ok(match self.result_descriptor().data_type {
            RasterDataType::U8 => TypedRasterQueryProcessor::U8(
                GdalSourceProcessor {
                    produced_result_descriptor: self.produced_result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    overview_level: self.overview_level,
                    original_resolution_spatial_grid: self.original_resolution_spatial_grid,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::U16 => TypedRasterQueryProcessor::U16(
                GdalSourceProcessor {
                    produced_result_descriptor: self.produced_result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    overview_level: self.overview_level,
                    original_resolution_spatial_grid: self.original_resolution_spatial_grid,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::U32 => TypedRasterQueryProcessor::U32(
                GdalSourceProcessor {
                    produced_result_descriptor: self.produced_result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    overview_level: self.overview_level,
                    original_resolution_spatial_grid: self.original_resolution_spatial_grid,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::U64 => {
                return Err(GdalSourceError::UnsupportedRasterType {
                    raster_type: RasterDataType::U64,
                })?;
            }
            RasterDataType::I8 => {
                return Err(GdalSourceError::UnsupportedRasterType {
                    raster_type: RasterDataType::I8,
                })?;
            }
            RasterDataType::I16 => TypedRasterQueryProcessor::I16(
                GdalSourceProcessor {
                    produced_result_descriptor: self.produced_result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    overview_level: self.overview_level,
                    original_resolution_spatial_grid: self.original_resolution_spatial_grid,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::I32 => TypedRasterQueryProcessor::I32(
                GdalSourceProcessor {
                    produced_result_descriptor: self.produced_result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    overview_level: self.overview_level,
                    original_resolution_spatial_grid: self.original_resolution_spatial_grid,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::I64 => {
                return Err(GdalSourceError::UnsupportedRasterType {
                    raster_type: RasterDataType::I64,
                })?;
            }
            RasterDataType::F32 => TypedRasterQueryProcessor::F32(
                GdalSourceProcessor {
                    produced_result_descriptor: self.produced_result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    overview_level: self.overview_level,
                    original_resolution_spatial_grid: self.original_resolution_spatial_grid,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::F64 => TypedRasterQueryProcessor::F64(
                GdalSourceProcessor {
                    produced_result_descriptor: self.produced_result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    overview_level: self.overview_level,
                    original_resolution_spatial_grid: self.original_resolution_spatial_grid,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
        })
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        GdalSource::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }

    fn data(&self) -> Option<String> {
        Some(self.data_name.to_string())
    }

    fn optimize(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<Box<dyn RasterOperator>, OptimizationError> {
        self.ensure_resolution_is_compatible_for_optimization(target_resolution)?;

        // TODO: handle cases where the original workflow explicitly loads overviews in the source
        ensure!(
            self.overview_level == 0,
            SourcesMustNotUseOverviews {
                data: self.data_name.to_string(),
                oveview_level: self.overview_level
            }
        );

        // as overview level is always 0 for now, the result descriptor contains the native resolution
        // TODO: when allowing to optimize upon overview levels, compute the native resolution first
        let native_resolution = self
            .produced_result_descriptor
            .spatial_grid
            .spatial_resolution();

        // TODO: get available overviews levels from the dataset metadata (not available yet) and only load these.
        //       Then, we might have to prepend a Resampling operator to match the target resolution.
        //       For now, we just load the overview level regardless and let gdal handle the resamṕling.
        let next_best_overview_level =
            find_next_best_overview_level(native_resolution, target_resolution);

        Ok(GdalSource {
            params: GdalSourceParameters {
                data: self.data_name.clone(),
                overview_level: Some(next_best_overview_level),
            },
        }
        .boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use crate::source::gdal_worker_process::{
        FileNotFoundHandling, GdalDatasetGeoTransform, GdalMetadataMapping, GdalProcessPool,
        GdalProcessPoolAccess, GdalSourceTimePlaceholder, TimeReference, WorkerConfig,
    };
    use crate::util::Result;
    use crate::util::gdal::add_ndvi_dataset;
    use geoengine_datatypes::hashmap;
    use geoengine_datatypes::primitives::DateTimeParseFormat;
    use geoengine_datatypes::primitives::{
        AxisAlignedRectangle, Coordinate2D, SpatialPartition2D, TimeInstance,
    };
    use geoengine_datatypes::raster::{
        BoundedGrid, EmptyGrid2D, GridBoundingBox, GridBounds, GridIdx2D, GridShape2D, GridSize,
        RasterPropertiesEntryType, RasterPropertiesKey, SpatialGridDefinition, TileInformation,
        TilesEqualIgnoringCacheHint, TilingStrategy,
    };
    use geoengine_datatypes::util::{gdal::hide_gdal_errors, test::TestDefault};

    async fn query_gdal_source(
        exe_ctx: &MockExecutionContext,
        query_ctx: &MockQueryContext,
        name: NamedData,
        spatial_query: GridBoundingBox2D,
        time_interval: TimeInterval,
    ) -> Vec<Result<RasterTile2D<u8>>> {
        let op = GdalSource {
            params: GdalSourceParameters::new(name.clone()),
        }
        .boxed();

        let o = op
            .initialize(WorkflowOperatorPath::initialize_root(), exe_ctx)
            .await
            .unwrap();

        o.query_processor()
            .unwrap()
            .get_u8()
            .unwrap()
            .raster_query(
                RasterQueryRectangle::new(spatial_query, time_interval, BandSelection::first()),
                query_ctx,
            )
            .await
            .unwrap()
            .collect()
            .await
    }

    #[test]
    fn it_deserializes() {
        let json_string = r#"
            {
                "type": "GdalSource",
                "params": {
                    "data": "ns:dataset"
                }
            }"#;

        let operator: GdalSource = serde_json::from_str(json_string).unwrap();

        assert_eq!(
            operator,
            GdalSource {
                params: GdalSourceParameters::new(NamedData::with_namespaced_name("ns", "dataset")),
            }
        );
    }

    #[test]
    fn tiling_strategy_origin() {
        let tile_size_in_pixels = [600, 600];
        let dataset_upper_right_coord = (-180.0, 90.0).into();
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let dataset_geo_transform = GeoTransform::new(
            dataset_upper_right_coord,
            dataset_x_pixel_size,
            dataset_y_pixel_size,
        );

        let partition = SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap();

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: dataset_geo_transform,
        };

        assert_eq!(
            origin_split_tileing_strategy
                .geo_transform
                .upper_left_pixel_idx(&partition),
            [0, 0].into()
        );
        assert_eq!(
            origin_split_tileing_strategy
                .geo_transform
                .lower_right_pixel_idx(&partition),
            [1800 - 1, 3600 - 1].into()
        );

        let tile_grid = origin_split_tileing_strategy.tile_grid_box(partition);
        assert_eq!(tile_grid.axis_size(), [3, 6]);
        assert_eq!(tile_grid.min_index(), [0, 0].into());
        assert_eq!(tile_grid.max_index(), [2, 5].into());
    }

    #[test]
    fn tiling_strategy_zero() {
        let tile_size_in_pixels = [600, 600];
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let central_geo_transform = GeoTransform::new_with_coordinate_x_y(
            0.0,
            dataset_x_pixel_size,
            0.0,
            dataset_y_pixel_size,
        );

        let partition = SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap();

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: central_geo_transform,
        };

        assert_eq!(
            origin_split_tileing_strategy
                .geo_transform
                .upper_left_pixel_idx(&partition),
            [-900, -1800].into()
        );
        assert_eq!(
            origin_split_tileing_strategy
                .geo_transform
                .lower_right_pixel_idx(&partition),
            [1800 / 2 - 1, 3600 / 2 - 1].into()
        );

        let tile_grid = origin_split_tileing_strategy.tile_grid_box(partition);
        assert_eq!(tile_grid.axis_size(), [4, 6]);
        assert_eq!(tile_grid.min_index(), [-2, -3].into());
        assert_eq!(tile_grid.max_index(), [1, 2].into());
    }

    #[test]
    fn tile_idx_iterator() {
        let tile_size_in_pixels = [600, 600];
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;
        let central_geo_transform = GeoTransform::new_with_coordinate_x_y(
            0.0,
            dataset_x_pixel_size,
            0.0,
            dataset_y_pixel_size,
        );

        let grid_bounds = GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap();

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: central_geo_transform,
        };

        let vres: Vec<GridIdx2D> = origin_split_tileing_strategy
            .tile_idx_iterator_from_grid_bounds(grid_bounds)
            .collect();
        assert_eq!(vres.len(), 4 * 6);
        assert_eq!(vres[0], [-2, -3].into());
        assert_eq!(vres[1], [-2, -2].into());
        assert_eq!(vres[2], [-2, -1].into());
        assert_eq!(vres[23], [1, 2].into());
    }

    #[test]
    fn tile_information_iterator() {
        let tile_size_in_pixels = [600, 600];
        let dataset_x_pixel_size = 0.1;
        let dataset_y_pixel_size = -0.1;

        let central_geo_transform = GeoTransform::new_with_coordinate_x_y(
            0.0,
            dataset_x_pixel_size,
            0.0,
            dataset_y_pixel_size,
        );

        let grid_bounds = GridBoundingBox2D::new([-900, -1800], [899, 1799]).unwrap();

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: central_geo_transform,
        };

        let vres: Vec<TileInformation> = origin_split_tileing_strategy
            .tile_information_iterator_from_pixel_bounds(grid_bounds)
            .collect();
        assert_eq!(vres.len(), 4 * 6);
        assert_eq!(
            vres[0],
            TileInformation::new(
                [-2, -3].into(),
                tile_size_in_pixels.into(),
                central_geo_transform,
            )
        );
        assert_eq!(
            vres[1],
            TileInformation::new(
                [-2, -2].into(),
                tile_size_in_pixels.into(),
                central_geo_transform,
            )
        );
        assert_eq!(
            vres[12],
            TileInformation::new(
                [0, -3].into(),
                tile_size_in_pixels.into(),
                central_geo_transform,
            )
        );
        assert_eq!(
            vres[23],
            TileInformation::new(
                [1, 2].into(),
                tile_size_in_pixels.into(),
                central_geo_transform,
            )
        );
    }

    #[test]
    fn replace_time_placeholder() {
        let params = GdalDatasetParameters {
            file_path: "/foo/bar_%TIME%.tiff".into(),
            rasterband_channel: 0,
            geo_transform: TestDefault::test_default(),
            width: 360,
            height: 180,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(0.),
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        };
        let replaced = params
            .replace_time_placeholders(
                &hashmap! {
                    "%TIME%".to_string() => GdalSourceTimePlaceholder {
                        format: DateTimeParseFormat::custom("%f".to_string()),
                        reference: TimeReference::Start,
                    },
                },
                TimeInterval::new_instant(TimeInstance::from_millis_unchecked(22)).unwrap(),
            )
            .unwrap();
        assert_eq!(
            replaced.file_path.to_string_lossy(),
            "/foo/bar_022000000.tiff".to_string()
        );
        assert_eq!(params.rasterband_channel, replaced.rasterband_channel);
        assert_eq!(params.geo_transform, replaced.geo_transform);
        assert_eq!(params.width, replaced.width);
        assert_eq!(params.height, replaced.height);
        assert_eq!(
            params.file_not_found_handling,
            replaced.file_not_found_handling
        );
    }

    #[tokio::test]
    async fn test_query_single_time_slice() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let id = add_ndvi_dataset(&mut exe_ctx);
        let spatial_query = GridBoundingBox2D::new([-256, -256], [255, 255]).unwrap();

        let time_interval = TimeInterval::new_unchecked(1_388_534_400_000, 1_388_534_400_001); // 2014-01-01

        let c = query_gdal_source(&exe_ctx, &query_ctx, id, spatial_query, time_interval).await;
        let c: Vec<RasterTile2D<u8>> = c.into_iter().map(Result::unwrap).collect();

        assert_eq!(c.len(), 4);

        assert_eq!(
            c[0].time,
            TimeInterval::new_unchecked(1_388_534_400_000, 1_391_212_800_000)
        );

        assert_eq!(
            c[0].tile_information().global_tile_position(),
            [-1, -1].into()
        );

        assert_eq!(
            c[1].tile_information().global_tile_position(),
            [-1, 0].into()
        );

        assert_eq!(
            c[2].tile_information().global_tile_position(),
            [0, -1].into()
        );

        assert_eq!(
            c[3].tile_information().global_tile_position(),
            [0, 0].into()
        );
    }

    #[tokio::test]
    async fn test_query_multi_time_slices() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let id = add_ndvi_dataset(&mut exe_ctx);

        let spatial_query = GridBoundingBox2D::new([-256, -256], [255, 255]).unwrap();

        let time_interval = TimeInterval::new_unchecked(1_388_534_400_000, 1_393_632_000_000); // 2014-01-01 - 2014-03-01

        let c = query_gdal_source(&exe_ctx, &query_ctx, id, spatial_query, time_interval).await;
        let c: Vec<RasterTile2D<u8>> = c.into_iter().map(Result::unwrap).collect();

        assert_eq!(c.len(), 8);

        assert_eq!(
            c[0].time,
            TimeInterval::new_unchecked(1_388_534_400_000, 1_391_212_800_000)
        );

        assert_eq!(
            c[5].time,
            TimeInterval::new_unchecked(1_391_212_800_000, 1_393_632_000_000)
        );
    }

    #[tokio::test]
    async fn test_query_before_data() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let id = add_ndvi_dataset(&mut exe_ctx);

        let spatial_query = GridBoundingBox2D::new([-256, -256], [255, 255]).unwrap();
        let time_interval = TimeInterval::new_unchecked(1_380_585_600_000, 1_380_585_600_000); // 2013-10-01 - 2013-10-01

        let c = query_gdal_source(&exe_ctx, &query_ctx, id, spatial_query, time_interval).await;
        let c: Vec<RasterTile2D<u8>> = c.into_iter().map(Result::unwrap).collect();

        assert_eq!(c.len(), 4);

        assert_eq!(
            c[0].time,
            TimeInterval::new_unchecked(TimeInstance::MIN, 1_388_534_400_000) // bot - 2014-01-01
        );
    }

    #[tokio::test]
    async fn test_query_after_data() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let id = add_ndvi_dataset(&mut exe_ctx);

        let spatial_query = GridBoundingBox2D::new([-256, -256], [255, 255]).unwrap();
        let time_interval = TimeInterval::new_unchecked(1_420_074_000_000, 1_420_074_000_000); // 2015-01-01 - 2015-01-01

        let c = query_gdal_source(&exe_ctx, &query_ctx, id, spatial_query, time_interval).await;
        let c: Vec<RasterTile2D<u8>> = c.into_iter().map(Result::unwrap).collect();

        assert_eq!(c.len(), 4);

        assert_eq!(
            c[0].time,
            TimeInterval::new_unchecked(1_404_172_800_000, TimeInstance::MAX) // 2014-07-01 - eot
        );
    }

    #[tokio::test]
    async fn test_nodata() {
        hide_gdal_errors();

        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = exe_ctx.mock_query_context_test_default();
        let id = add_ndvi_dataset(&mut exe_ctx);

        let spatial_query = GridBoundingBox2D::new([-256, -256], [255, 255]).unwrap();
        let time_interval = TimeInterval::new_unchecked(1_385_856_000_000, 1_388_534_400_000); // 2013-12-01 - 2014-01-01

        let c = query_gdal_source(&exe_ctx, &query_ctx, id, spatial_query, time_interval).await;
        let c: Vec<RasterTile2D<u8>> = c.into_iter().map(Result::unwrap).collect();

        assert_eq!(c.len(), 4);

        let tile_1 = &c[0];

        assert_eq!(
            tile_1.time,
            TimeInterval::new_unchecked(TimeInstance::MIN, 1_388_534_400_000)
        );

        assert!(tile_1.is_empty());
    }

    fn tile_information_with_partition_and_shape(
        partition: SpatialPartition2D,
        shape: GridShape2D,
    ) -> TileInformation {
        let real_geotransform = GeoTransform::new(
            partition.upper_left(),
            partition.size_x() / shape.axis_size_x() as f64,
            -partition.size_y() / shape.axis_size_y() as f64,
        );

        TileInformation {
            tile_size_in_pixels: shape,
            global_tile_position: [0, 0].into(),
            global_geo_transform: real_geotransform,
        }
    }

    #[tokio::test]
    async fn timestep_without_params() {
        let output_bounds =
            SpatialPartition2D::new_unchecked((-90., 90.).into(), (90., -90.).into());
        let output_shape: GridShape2D = [256, 256].into();

        let tile_info = tile_information_with_partition_and_shape(output_bounds, output_shape);
        let time_interval = TimeInterval::new_unchecked(1_388_534_400_000, 1_391_212_800_000); // 2014-01-01 - 2014-01-15
        let params = None;
        let reader_mode = GdalReaderMode::OriginalResolution(ReaderState {
            dataset_spatial_grid: SpatialGridDefinition::new(
                tile_info.global_geo_transform,
                GridShape2D::new([3600, 1800]).bounding_box(),
            ),
        });

        let gpp = GdalProcessPool::new(2, 2, 2, WorkerConfig::default());
        let gw: GdalPoolDispatcher = gpp.get_gdal_worker();

        let tile = GdalRasterLoader::load_tile_async::<f64>(
            params,
            reader_mode,
            tile_info,
            time_interval,
            CacheHint::default(),
            gw,
        )
        .await;

        assert!(tile.is_ok());

        let expected = RasterTile2D::<f64>::new_with_tile_info(
            time_interval,
            tile_info,
            0,
            EmptyGrid2D::new(output_shape).into(),
            CacheHint::default(),
        );

        assert!(tile.unwrap().tiles_equal_ignoring_cache_hint(&expected));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn deserialize_dataset_parameters() {
        let dataset_parameters = GdalDatasetParameters {
            file_path: "path-to-data.tiff".into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (-180., 90.).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 3600,
            height: 1800,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(f64::NAN),
            properties_mapping: Some(vec![
                GdalMetadataMapping {
                    source_key: RasterPropertiesKey {
                        domain: None,
                        key: "AREA_OR_POINT".to_string(),
                    },
                    target_type: RasterPropertiesEntryType::String,
                    target_key: RasterPropertiesKey {
                        domain: None,
                        key: "AREA_OR_POINT".to_string(),
                    },
                },
                GdalMetadataMapping {
                    source_key: RasterPropertiesKey {
                        domain: Some("IMAGE_STRUCTURE".to_string()),
                        key: "COMPRESSION".to_string(),
                    },
                    target_type: RasterPropertiesEntryType::String,
                    target_key: RasterPropertiesKey {
                        domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                        key: "COMPRESSION".to_string(),
                    },
                },
            ]),
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        };

        let dataset_parameters_json = serde_json::to_value(&dataset_parameters).unwrap();

        assert_eq!(
            dataset_parameters_json,
            serde_json::json!({
                "filePath": "path-to-data.tiff",
                "rasterbandChannel": 1,
                "geoTransform": {
                    "originCoordinate": {
                        "x": -180.,
                        "y": 90.
                    },
                    "xPixelSize": 0.1,
                    "yPixelSize": -0.1
                },
                "width": 3600,
                "height": 1800,
                "fileNotFoundHandling": "NoData",
                "noDataValue": "nan",
                "propertiesMapping": [{
                        "source_key": {
                            "domain": null,
                            "key": "AREA_OR_POINT"
                        },
                        "target_key": {
                            "domain": null,
                            "key": "AREA_OR_POINT"
                        },
                        "target_type": "String"
                    },
                    {
                        "source_key": {
                            "domain": "IMAGE_STRUCTURE",
                            "key": "COMPRESSION"
                        },
                        "target_key": {
                            "domain": "IMAGE_STRUCTURE_INFO",
                            "key": "COMPRESSION"
                        },
                        "target_type": "String"
                    }
                ],
                "gdalOpenOptions": null,
                "gdalConfigOptions": null,
                "allowAlphabandAsMask": true,
                "retry": null,
            })
        );

        let deserialized_parameters =
            serde_json::from_value::<GdalDatasetParameters>(dataset_parameters_json).unwrap();

        // since there is NaN in the data, we can't check for equality on the whole object

        assert_eq!(
            deserialized_parameters.file_path,
            dataset_parameters.file_path,
        );
        assert_eq!(
            deserialized_parameters.rasterband_channel,
            dataset_parameters.rasterband_channel,
        );
        assert_eq!(
            deserialized_parameters.geo_transform,
            dataset_parameters.geo_transform,
        );
        assert_eq!(deserialized_parameters.width, dataset_parameters.width);
        assert_eq!(deserialized_parameters.height, dataset_parameters.height);
        assert_eq!(
            deserialized_parameters.file_not_found_handling,
            dataset_parameters.file_not_found_handling,
        );
        assert!(
            deserialized_parameters.no_data_value.unwrap().is_nan()
                && dataset_parameters.no_data_value.unwrap().is_nan()
        );
        assert_eq!(
            deserialized_parameters.properties_mapping,
            dataset_parameters.properties_mapping,
        );
        assert_eq!(
            deserialized_parameters.gdal_open_options,
            dataset_parameters.gdal_open_options,
        );
        assert_eq!(
            deserialized_parameters.gdal_config_options,
            dataset_parameters.gdal_config_options,
        );
    }

    #[tokio::test]
    async fn it_attaches_cache_hint() {
        let output_bounds =
            SpatialPartition2D::new_unchecked((-90., 90.).into(), (90., -90.).into());
        let output_shape: GridShape2D = [256, 256].into();

        let tile_info = tile_information_with_partition_and_shape(output_bounds, output_shape);
        let time_interval = TimeInterval::new_unchecked(1_388_534_400_000, 1_391_212_800_000); // 2014-01-01 - 2014-01-15
        let params = None;

        let gpp = GdalProcessPool::new(2, 2, 2, WorkerConfig::default());
        let gw: GdalPoolDispatcher = gpp.get_gdal_worker();

        let tile = GdalRasterLoader::load_tile_async::<f64>(
            params,
            GdalReaderMode::OriginalResolution(ReaderState {
                dataset_spatial_grid: SpatialGridDefinition::new(
                    tile_info.global_geo_transform,
                    GridShape2D::new([3600, 1800]).bounding_box(),
                ),
            }),
            tile_info,
            time_interval,
            CacheHint::seconds(1234),
            gw,
        )
        .await;

        assert!(tile.is_ok());

        let expected = RasterTile2D::<f64>::new_with_tile_info(
            time_interval,
            tile_info,
            0,
            EmptyGrid2D::new(output_shape).into(),
            CacheHint::seconds(1234),
        );

        assert!(tile.unwrap().tiles_equal_ignoring_cache_hint(&expected));
    }

    #[test]
    fn it_computes_spatial_grids_for_overviews() {
        let spatial_grid_definition = SpatialGridDefinition::new(
            GeoTransform::new(Coordinate2D::new(0., 0.), 0.1, -0.1),
            GridBoundingBox::new([-900, -1800], [899, 1799]).unwrap(),
        );

        let spatial_grid_definition_2x =
            overview_level_spatial_grid(spatial_grid_definition, 2).unwrap();
        let expected_spatial_grid_definition_2x = SpatialGridDefinition::new(
            GeoTransform::new(Coordinate2D::new(0., 0.), 0.2, -0.2),
            GridBoundingBox::new([-450, -900], [449, 899]).unwrap(),
        );
        assert_eq!(
            spatial_grid_definition_2x,
            expected_spatial_grid_definition_2x
        );

        let spatial_grid_definition_4x =
            overview_level_spatial_grid(spatial_grid_definition, 4).unwrap();
        let expected_spatial_grid_definition_4x = SpatialGridDefinition::new(
            GeoTransform::new(Coordinate2D::new(0., 0.), 0.4, -0.4),
            GridBoundingBox::new([-225, -450], [224, 449]).unwrap(),
        );
        assert_eq!(
            spatial_grid_definition_4x,
            expected_spatial_grid_definition_4x
        );

        let spatial_grid_definition_8x =
            overview_level_spatial_grid(spatial_grid_definition, 8).unwrap();
        let expected_spatial_grid_definition_8x = SpatialGridDefinition::new(
            GeoTransform::new(Coordinate2D::new(0., 0.), 0.8, -0.8),
            GridBoundingBox::new([-113, -225], [112, 224]).unwrap(),
        );
        assert_eq!(
            spatial_grid_definition_8x,
            expected_spatial_grid_definition_8x
        );
    }
}
