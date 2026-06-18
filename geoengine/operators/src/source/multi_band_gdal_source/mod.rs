use crate::engine::{
    CanonicOperatorName, MetaData, OperatorData, OperatorName, QueryContext, QueryProcessor,
    SpatialGridDescriptor, WorkflowOperatorPath,
};
use crate::optimization::{OptimizableOperator, OptimizationError, SourcesMustNotUseOverviews};
use crate::source::gdal_source::reader::{
    GdalReadAdvise as PoolGdalReadAdvise, GdalReadWindow as PoolGdalReadWindow,
};
use crate::source::gdal_source::{
    GdalPoolWorkerInstance, GdalProcessPoolError, process::GdalErrorKind,
};
use crate::source::multi_band_gdal_source::reader::GdalReadAdvise;
use crate::source::{
    FileNotFoundHandling, GdalDatasetParameters, IpcChannelMessage, IpcChannelMessagePayload,
    IpcProcessError,
};
use crate::{
    engine::{
        InitializedRasterOperator, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
        SourceOperator, TypedRasterQueryProcessor,
    },
    util::Result,
};
use async_trait::async_trait;
pub use error::GdalSourceError;
use futures::stream::{self, BoxStream, StreamExt};
use gdal::raster::GdalType;
use geoengine_datatypes::{
    dataset::NamedData,
    primitives::{
        BandSelection, QueryRectangle, RasterQueryRectangle, SpatialPartition2D, SpatialResolution,
        TimeInterval, find_next_best_overview_level,
    },
    raster::{
        ChangeGridBounds, EmptyGrid, GeoTransform, GridBlit, GridBoundingBox2D, GridIdx2D,
        GridOrEmpty, GridShape2D, MaskedGrid, Pixel, RasterDataType, RasterProperties,
        RasterTile2D, SpatialGridDefinition, TileInformation, TilingSpatialGridDefinition,
        TilingSpecification,
    },
};
pub use loading_info::{GdalMultiBand, MultiBandGdalLoadingInfo, TileFile};
use num::{FromPrimitive, integer::div_ceil, integer::div_floor};
use reader::{GdalReaderMode, GridAndProperties, OverviewReaderState, ReaderState};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::marker::PhantomData;
use std::time::Instant;
use tracing::{debug, trace};

mod error;
mod loading_info;
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

type MultiBandGdalMetaData = Box<
    dyn MetaData<
            MultiBandGdalLoadingInfo,
            RasterResultDescriptor,
            MultiBandGdalLoadingInfoQueryRectangle,
        >,
>;

/// A query rectangle on rasters in coordinate (instead of pixel) space. This is used to request tiles that are anchored in coordinates space.
/// Additionaly you can also specify to fetch tiles (with gdal params).
/// Set `fetch_tiles` to false if you only want to access the time steps.
#[derive(Clone, Debug)]
pub struct MultiBandGdalLoadingInfoQueryRectangle {
    pub query_rectangle: QueryRectangle<SpatialPartition2D, BandSelection>,
    pub fetch_tiles: bool,
}

impl MultiBandGdalLoadingInfoQueryRectangle {
    pub fn new(
        spatial_bounds: SpatialPartition2D,
        time_interval: TimeInterval,
        attributes: BandSelection,
        fetch_tiles: bool,
    ) -> Self {
        Self {
            query_rectangle: QueryRectangle::new(spatial_bounds, time_interval, attributes),
            fetch_tiles,
        }
    }

    pub fn with_query_rectangle(
        query_rectangle: QueryRectangle<SpatialPartition2D, BandSelection>,
        fetch_tiles: bool,
    ) -> Self {
        Self {
            query_rectangle,
            fetch_tiles,
        }
    }
}

fn raster_query_rectangle_to_loading_info_query_rectangle(
    raster_query_rectangle: &RasterQueryRectangle,
    tiling_spatial_grid: TilingSpatialGridDefinition,
    fetch_tiles: bool,
) -> MultiBandGdalLoadingInfoQueryRectangle {
    MultiBandGdalLoadingInfoQueryRectangle::new(
        tiling_spatial_grid
            .tiling_geo_transform()
            .grid_to_spatial_bounds(&raster_query_rectangle.spatial_bounds()),
        raster_query_rectangle.time_interval(),
        raster_query_rectangle.attributes().clone(),
        fetch_tiles,
    )
}

pub struct GdalSourceProcessor<T>
where
    T: Pixel,
{
    pub produced_result_descriptor: RasterResultDescriptor,
    pub tiling_specification: TilingSpecification,
    pub meta_data: MultiBandGdalMetaData,
    pub overview_level: u32,
    pub original_resolution_spatial_grid: Option<SpatialGridDefinition>,
    pub _phantom_data: PhantomData<T>,
}

struct GdalRasterLoader {}

impl GdalRasterLoader {
    /// Loads a tile using the separate process and the provided parameters and read advise.
    /// This is the method where the source operator attaches to the process pool.
    /// The method sends a message to the process pool and waits for the response. The response is then converted to a `RasterTile2D` and returned.
    /// The method also handles the case where the file is not found and the `file_not_found_handling` is set to `NoData` by returning a tile filled with nodata values.
    /// # Errors
    /// Returns a `GdalSourceError` if the process returns an error, or if the response cannot be converted to a `RasterTile2D`.
    ///
    /// # Panics
    /// Panics if the response from the process is not in the expected format, or if the grid blitting fails (which should not happen if the bounds are correct).
    pub async fn load_tile_data_process<T: Pixel + GdalType + FromPrimitive>(
        dataset_params: GdalDatasetParameters,
        local_read_advise: GdalReadAdvise,
        gdal_worker: GdalPoolWorkerInstance,
    ) -> Result<Option<GridAndProperties<T, GridBoundingBox2D>>, GdalSourceError> {
        let file_not_found_as_no_data =
            dataset_params.file_not_found_handling == FileNotFoundHandling::NoData;

        let (start_x, start_y) = local_read_advise.gdal_read_widow.gdal_window_start();
        let (size_x, size_y) = local_read_advise.gdal_read_widow.gdal_window_size();
        let read_advise = PoolGdalReadAdvise {
            gdal_read_widow: PoolGdalReadWindow::new(
                GridIdx2D::new_y_x(start_y, start_x),
                GridShape2D::new_2d(size_y, size_x),
            ),
            read_window_bounds: local_read_advise.read_window_bounds,
            bounds_of_target: local_read_advise.bounds_of_target,
            flip_y: local_read_advise.flip_y,
        };

        let message = IpcChannelMessage::new_request_tile_message(IpcChannelMessagePayload {
            dataset_params,
            read_advise,
            data_type: T::TYPE,
            span_context: Default::default(),
        });

        let res: Result<_, _> = gdal_worker.read_data(message).await;

        let res = match res {
            Ok(t) => {
                // Here we need to handle edges!
                // First, convert response to GridAndProperties
                let super::gdal_source::reader::GridAndProperties { grid, properties }: super::gdal_source::reader::GridAndProperties<
                    T,
                    GridBoundingBox2D,
                > = t.into();
                // Second, flip y-axis if necessary
                let grid = if read_advise.flip_y {
                    match grid {
                        GridOrEmpty::Grid(MaskedGrid {
                            inner_grid,
                            validity_mask,
                        }) => GridOrEmpty::new_grid(
                            MaskedGrid::new(
                                inner_grid.reversed_y_axis_grid(),
                                validity_mask.reversed_y_axis_grid(),
                            )
                            .expect("The bounds of the input grid should be the same after reversing the y axis, so this should never fail"),
                        ),
                        GridOrEmpty::Empty(e) => GridOrEmpty::new_empty(e),
                    }
                } else {
                    grid
                };

                Ok(Some(GridAndProperties { grid, properties }))
            }
            Err(GdalProcessPoolError::IpcProcessError {
                source:
                    IpcProcessError::GdalError {
                        kind: GdalErrorKind::FileNotFound,
                        details: _details,
                    },
            }) if file_not_found_as_no_data => Ok(None),
            Err(other_err) => Err(other_err),
        }?;

        Ok(res)
    }

    async fn load_tile_grid_props<T: Pixel + GdalType + FromPrimitive>(
        dataset_params: GdalDatasetParameters,
        reader_mode: GdalReaderMode,
        tile_information: TileInformation,
        gdal_worker: GdalPoolWorkerInstance,
    ) -> Result<Option<GridAndProperties<T>>> {
        let ds_spatial_grid = dataset_params.spatial_grid_definition();
        let tile_spatial_grid = tile_information.spatial_grid_definition();
        let Some(local_read_advise) =
            reader_mode.tiling_to_dataset_read_advise(&ds_spatial_grid, &tile_spatial_grid)
        else {
            trace!(
                "no read advise returned for tile {:?}, skipping file.",
                tile_information.global_tile_position,
            );
            return Ok(None);
        };

        let file_tile =
            Self::load_tile_data_process::<T>(dataset_params, local_read_advise, gdal_worker)
                .await?;

        Ok(file_tile)
    }

    async fn load_tile_from_files_async<T: Pixel + GdalType + FromPrimitive>(
        loading_info: MultiBandGdalLoadingInfo,
        reader_mode: GdalReaderMode,
        tile_information: TileInformation,
        time: TimeInterval,
        band: u32,
        gdal_worker: GdalPoolWorkerInstance,
    ) -> Result<RasterTile2D<T>> {
        debug!(
            "loading tile {:?} for time: {}, band: {band}",
            tile_information.global_tile_position.inner(),
            time.to_string()
        );
        let tile_files = loading_info.tile_files(time, tile_information, band);

        debug!(
            "tile_files: {}",
            tile_files
                .iter()
                .map(|tf| tf.file_path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );

        let mut tile_raster: GridOrEmpty<GridBoundingBox2D, T> =
            GridOrEmpty::from(EmptyGrid::new(tile_information.global_pixel_bounds()));

        let mut properties = RasterProperties::default();
        let cache_hint = loading_info.cache_hint();

        for dataset_params in tile_files {
            if let Some(file_tile) = Self::load_tile_grid_props(
                dataset_params,
                reader_mode,
                tile_information,
                gdal_worker.clone(),
            )
            .await?
            {
                tile_raster.grid_blit_from(&file_tile.grid);
                properties = file_tile.properties;
            }
        }

        Ok(RasterTile2D::new_with_properties(
            time,
            tile_information.global_tile_position,
            band,
            tile_information.global_geo_transform,
            tile_raster.unbounded(),
            properties,
            cache_hint,
        ))
    }
}

impl<T> GdalSourceProcessor<T> where T: gdal::raster::GdalType + Pixel {}

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
        // TODO: check all bands exist

        tracing::debug!(
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

        let loading_info_start = Instant::now();
        let loading_info = self
            .meta_data
            .loading_info(raster_query_rectangle_to_loading_info_query_rectangle(
                &query,
                produced_tiling_grid,
                true,
            ))
            .await?;
        let loading_info_ms = loading_info_start.elapsed().as_millis();
        debug!(loading_info_ms, "loading_info timing");

        let gdal_worker = ctx.get_gdal_worker();
        let time_steps = loading_info.time_steps().to_vec();
        let bands = query.attributes().clone().as_vec();
        let spatial_tiles = tiling_strategy
            .tile_information_iterator_from_pixel_bounds(query.spatial_bounds())
            .collect::<Vec<_>>();

        debug!(
            "num timesteps: {}, num bands: {}, num spatial tiles: {}",
            time_steps.len(),
            bands.len(),
            spatial_tiles.len()
        );

        // create a stream with a tile for each band of each spatial tile for each time step
        let time_tile_band_iter = itertools::iproduct!(
            time_steps.into_iter(),
            spatial_tiles.into_iter(),
            bands.into_iter(),
        );

        let stream = stream::iter(time_tile_band_iter)
            .map(move |(time_interval, tile_info, band_idx)| {
                GdalRasterLoader::load_tile_from_files_async::<P>(
                    loading_info.clone(),
                    reader_mode,
                    tile_info,
                    time_interval,
                    band_idx,
                    gdal_worker.clone(),
                )
            })
            .buffered(16) // TODO: make configurable
            .boxed();

        return Ok(stream);
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.produced_result_descriptor
    }
}

#[async_trait]
impl<P> RasterQueryProcessor for GdalSourceProcessor<P>
where
    P: Pixel + gdal::raster::GdalType + FromPrimitive,
{
    type RasterType = P;

    async fn _time_query<'a>(
        &'a self,
        query: TimeInterval,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<TimeInterval>>> {
        let result_descriptor = self.result_descriptor();

        let grid_produced_by_source_desc = result_descriptor.spatial_grid;

        let produced_tiling_grid =
            grid_produced_by_source_desc.tiling_grid_definition(self.tiling_specification);

        let q_bounds = self
            .raster_result_descriptor()
            .tiling_grid_definition(ctx.tiling_specification())
            .tiling_grid_bounds();
        let query = RasterQueryRectangle::new(q_bounds, query, BandSelection::first());

        let qrect = raster_query_rectangle_to_loading_info_query_rectangle(
            &query,
            produced_tiling_grid,
            false,
        );

        let loading_info = self.meta_data.loading_info(qrect).await?;

        let time_steps = loading_info.time_steps().to_vec();

        let stream = stream::iter(time_steps).map(Result::Ok).boxed();

        Ok(stream)
    }
}

pub type MultiBandGdalSource = SourceOperator<GdalSourceParameters>;

impl OperatorName for MultiBandGdalSource {
    const TYPE_NAME: &'static str = "MultiBandGdalSource";
}

fn overview_level_spatial_grid(
    source_spatial_grid: SpatialGridDefinition,
    overview_level: u32,
) -> Option<SpatialGridDefinition> {
    if overview_level > 0 {
        debug!("Using overview level {overview_level}");
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
            div_ceil(
                source_spatial_grid.grid_bounds.y_max(),
                overview_level as isize,
            ),
            div_floor(
                source_spatial_grid.grid_bounds.x_min(),
                overview_level as isize,
            ),
            div_ceil(
                source_spatial_grid.grid_bounds.x_max(),
                overview_level as isize,
            ),
        )
        .expect("overview level must be a positive integer");

        Some(SpatialGridDefinition::new(geo_transform, grid_bounds))
    } else {
        debug!("Using original resolution (ov = 0)");
        None
    }
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for MultiBandGdalSource {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let data_id = context.resolve_named_data(&self.params.data).await?;
        let meta_data: MultiBandGdalMetaData = context.meta_data(&data_id).await?;

        debug!(
            "Initializing MultiBandGdalSource for {:?}.",
            &self.params.data
        );
        debug!("GdalSource path: {:?}", path);

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

    span_fn!(MultiBandGdalSource);
}

pub struct InitializedGdalSourceOperator {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    data_name: NamedData,
    pub meta_data: MultiBandGdalMetaData,
    pub produced_result_descriptor: RasterResultDescriptor,
    pub tiling_specification: TilingSpecification,
    // the overview level to use. 0/1 means the highest resolution
    pub overview_level: u32,
    pub original_resolution_spatial_grid: Option<SpatialGridDefinition>,
}

impl InitializedGdalSourceOperator {
    pub fn initialize_original_resolution(
        name: CanonicOperatorName,
        path: WorkflowOperatorPath,
        data_name: NamedData,
        meta_data: MultiBandGdalMetaData,
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

    pub fn initialize_with_overview_level(
        name: CanonicOperatorName,
        path: WorkflowOperatorPath,
        data_name: NamedData,
        meta_data: MultiBandGdalMetaData,
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
            data_name,
            produced_result_descriptor: result_descriptor,
            meta_data,
            tiling_specification,
            overview_level,
            original_resolution_spatial_grid: original_grid,
        }
    }
}

impl InitializedRasterOperator for InitializedGdalSourceOperator {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.produced_result_descriptor
    }

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
        MultiBandGdalSource::TYPE_NAME
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

        Ok(MultiBandGdalSource {
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
    use std::str::FromStr;

    use super::*;
    use crate::engine::{
        ExecutionContext, MockExecutionContext, RasterBandDescriptor, StaticMetaData,
        TimeDescriptor,
    };
    use crate::source::{FileNotFoundHandling, GdalDatasetGeoTransform, GdalMetadataMapping};
    use crate::test_data;
    use crate::util::test::raster_tile_from_file;
    use futures::TryStreamExt;
    use geoengine_datatypes::dataset::{DataId, DatasetId};
    use geoengine_datatypes::primitives::{
        CacheHint, Measurement, SpatialPartition2D, TimeInstance,
    };
    use geoengine_datatypes::raster::{GridBounds, GridIdx2D, GridSize};
    use geoengine_datatypes::raster::{RasterPropertiesEntryType, RasterPropertiesKey};
    use geoengine_datatypes::raster::{TileInformation, TilingStrategy};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::Identifier;
    use geoengine_datatypes::util::test::{TestDefault, assert_eq_two_list_of_tiles};

    #[test]
    fn it_deserializes() {
        let json_string = r#"
            {
                "type": "MultiBandGdalSource",
                "params": {
                    "data": "ns:dataset"
                }
            }"#;

        let operator: MultiBandGdalSource = serde_json::from_str(json_string).unwrap();

        assert_eq!(
            operator,
            MultiBandGdalSource {
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

    fn add_multi_tile_dataset(
        ctx: &mut MockExecutionContext,
        mut files: Vec<TileFile>,
        time_steps: Vec<TimeInterval>,
    ) -> NamedData {
        let id: DataId = DatasetId::new().into();
        let name = NamedData::with_system_name("multi_tiles");

        // fix the file path because the test runs with a different working directory than the server
        for tile in &mut files {
            tile.params.file_path = test_data!(
                tile.params
                    .file_path
                    .to_string_lossy()
                    .replace("test_data/", "")
            )
            .to_path_buf();
        }

        let time = TimeDescriptor::new_irregular(Some(TimeInterval::new_unchecked(
            time_steps.first().unwrap().start(),
            time_steps.last().unwrap().end(),
        )));

        let meta: MultiBandGdalMetaData = Box::new(StaticMetaData {
            loading_info: MultiBandGdalLoadingInfo::new(time_steps, files, CacheHint::default()),
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::U16,
                spatial_reference: SpatialReference::epsg_4326().into(),
                time,
                spatial_grid: SpatialGridDescriptor::source_from_parts(
                    GeoTransform::new((-180.0, 90.0).into(), 0.2, -0.2),
                    GridBoundingBox2D::new([0, 0], [899, 1799]).unwrap(),
                ),
                bands: vec![
                    RasterBandDescriptor::new("band 0".to_string(), Measurement::Unitless),
                    RasterBandDescriptor::new("band 1".to_string(), Measurement::Unitless),
                ]
                .try_into()
                .unwrap(),
            },
            phantom: Default::default(),
        });

        ctx.add_meta_data(id, name.clone(), meta);

        name
    }

    fn tile_files() -> Vec<TileFile> {
        serde_json::from_str(
            &std::fs::read_to_string(test_data!("raster/multi_tile/metadata/loading_info.json"))
                .unwrap(),
        )
        .unwrap()
    }

    fn tile_files_rev() -> Vec<TileFile> {
        serde_json::from_str(
            &std::fs::read_to_string(test_data!(
                "raster/multi_tile/metadata/loading_info_rev.json"
            ))
            .unwrap(),
        )
        .unwrap()
    }

    fn tiles_by_file_name(file_names: &[&str]) -> Vec<TileFile> {
        let tile_files = tile_files();

        file_names
            .iter()
            .map(|file_name| {
                tile_files
                    .iter()
                    .find(|tile_file| tile_file.params.file_path.ends_with(*file_name))
                    .cloned()
                    .unwrap()
            })
            .collect()
    }

    fn tiles_by_file_name_rev(file_names: &[&str]) -> Vec<TileFile> {
        let tile_files = tile_files_rev();

        file_names
            .iter()
            .map(|file_name| {
                tile_files
                    .iter()
                    .find(|tile_file| tile_file.params.file_path.ends_with(*file_name))
                    .cloned()
                    .unwrap()
            })
            .collect()
    }

    #[tokio::test]
    async fn it_loads_multi_band_multi_file_mosaics() -> Result<()> {
        let mut execution_context = MockExecutionContext::test_default();
        let query_ctx = execution_context.mock_query_context_test_default();

        let time_steps = vec![TimeInterval::new_unchecked(
            TimeInstance::from_str("2025-01-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2025-02-01T00:00:00Z").unwrap(),
        )];

        let files = tiles_by_file_name(&[
            "2025-01-01_tile_x0_y0_b0.tif",
            "2025-01-01_tile_x0_y1_b0.tif",
            "2025-01-01_tile_x1_y0_b0.tif",
            "2025-01-01_tile_x1_y1_b0.tif",
        ]);

        let dataset_name = add_multi_tile_dataset(&mut execution_context, files, time_steps);

        let operator = MultiBandGdalSource {
            params: GdalSourceParameters::new(dataset_name),
        }
        .boxed();

        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized = operator
            .clone()
            .initialize(workflow_operator_path_root, &execution_context)
            .await?;

        let processor = initialized.query_processor()?;

        let tiling_spec = execution_context.tiling_specification();

        let tiling_spatial_grid_definition = processor
            .result_descriptor()
            .spatial_grid_descriptor()
            .tiling_grid_definition(tiling_spec);

        let query_tiling_pixel_grid = tiling_spatial_grid_definition
            .tiling_spatial_grid_definition()
            .spatial_bounds_to_compatible_spatial_grid(SpatialPartition2D::new_unchecked(
                (-180., 90.).into(),
                (180.0, -90.).into(),
            ));

        let query_rect = RasterQueryRectangle::new(
            query_tiling_pixel_grid.grid_bounds(),
            TimeInterval::new_instant(
                geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                    .unwrap(),
            )
            .unwrap(),
            BandSelection::first(),
        );

        let tiles = processor
            .get_u16()
            .unwrap()
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?;

        let expected_tiles = [
            "2025-01-01_global_b0_tile_0.tif",
            "2025-01-01_global_b0_tile_1.tif",
            "2025-01-01_global_b0_tile_2.tif",
            "2025-01-01_global_b0_tile_3.tif",
            "2025-01-01_global_b0_tile_4.tif",
            "2025-01-01_global_b0_tile_5.tif",
            "2025-01-01_global_b0_tile_6.tif",
            "2025-01-01_global_b0_tile_7.tif",
        ];

        let expected_time = TimeInterval::new_unchecked(
            TimeInstance::from_str("2025-01-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2025-02-01T00:00:00Z").unwrap(),
        );

        let expected_tiles: Vec<_> = expected_tiles
            .iter()
            .map(|f| {
                raster_tile_from_file::<u16>(
                    test_data!(format!("raster/multi_tile/results/z_index/tiles/{f}")),
                    tiling_spatial_grid_definition,
                    expected_time,
                    0,
                )
                .unwrap()
            })
            .collect();

        assert_eq_two_list_of_tiles(&tiles, &expected_tiles, false);

        Ok(())
    }

    #[tokio::test]
    async fn it_loads_multi_band_multi_file_mosaics_2_bands() -> Result<()> {
        let mut execution_context = MockExecutionContext::test_default();
        let query_ctx = execution_context.mock_query_context_test_default();

        let time_steps = vec![TimeInterval::new_unchecked(
            TimeInstance::from_str("2025-01-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2025-02-01T00:00:00Z").unwrap(),
        )];

        let files = tiles_by_file_name(&[
            "2025-01-01_tile_x0_y0_b0.tif",
            "2025-01-01_tile_x0_y0_b1.tif",
            "2025-01-01_tile_x0_y1_b0.tif",
            "2025-01-01_tile_x0_y1_b1.tif",
            "2025-01-01_tile_x1_y0_b0.tif",
            "2025-01-01_tile_x1_y0_b1.tif",
            "2025-01-01_tile_x1_y1_b0.tif",
            "2025-01-01_tile_x1_y1_b1.tif",
        ]);

        let dataset_name = add_multi_tile_dataset(&mut execution_context, files, time_steps);

        let operator = MultiBandGdalSource {
            params: GdalSourceParameters::new(dataset_name),
        }
        .boxed();

        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized = operator
            .clone()
            .initialize(workflow_operator_path_root, &execution_context)
            .await?;

        let processor = initialized.query_processor()?;

        let tiling_spec = execution_context.tiling_specification();

        let tiling_spatial_grid_definition = processor
            .result_descriptor()
            .spatial_grid_descriptor()
            .tiling_grid_definition(tiling_spec);

        let query_tiling_pixel_grid = tiling_spatial_grid_definition
            .tiling_spatial_grid_definition()
            .spatial_bounds_to_compatible_spatial_grid(SpatialPartition2D::new_unchecked(
                (-180., 90.).into(),
                (180.0, -90.).into(),
            ));

        let query_rect = RasterQueryRectangle::new(
            query_tiling_pixel_grid.grid_bounds(),
            TimeInterval::new_instant(
                geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                    .unwrap(),
            )
            .unwrap(),
            BandSelection::first_n(2),
        );

        let tiles = processor
            .get_u16()
            .unwrap()
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?;

        let expected_tiles = [
            ("2025-01-01_global_b0_tile_0.tif", 0u32),
            ("2025-01-01_global_b1_tile_0.tif", 1),
            ("2025-01-01_global_b0_tile_1.tif", 0),
            ("2025-01-01_global_b1_tile_1.tif", 1),
            ("2025-01-01_global_b0_tile_2.tif", 0),
            ("2025-01-01_global_b1_tile_2.tif", 1),
            ("2025-01-01_global_b0_tile_3.tif", 0),
            ("2025-01-01_global_b1_tile_3.tif", 1),
            ("2025-01-01_global_b0_tile_4.tif", 0),
            ("2025-01-01_global_b1_tile_4.tif", 1),
            ("2025-01-01_global_b0_tile_5.tif", 0),
            ("2025-01-01_global_b1_tile_5.tif", 1),
            ("2025-01-01_global_b0_tile_6.tif", 0),
            ("2025-01-01_global_b1_tile_6.tif", 1),
            ("2025-01-01_global_b0_tile_7.tif", 0),
            ("2025-01-01_global_b1_tile_7.tif", 1),
        ];

        let expected_time = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-02-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_tiles: Vec<_> = expected_tiles
            .iter()
            .map(|(f, b)| {
                raster_tile_from_file::<u16>(
                    test_data!(format!("raster/multi_tile/results/z_index/tiles/{f}")),
                    tiling_spatial_grid_definition,
                    expected_time,
                    *b,
                )
                .unwrap()
            })
            .collect();

        assert_eq_two_list_of_tiles(&tiles, &expected_tiles, false);

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_loads_multi_band_multi_file_mosaics_2_bands_2_timesteps() -> Result<()> {
        let mut execution_context = MockExecutionContext::test_default();
        let query_ctx = execution_context.mock_query_context_test_default();

        let time_steps = vec![
            TimeInterval::new_unchecked(
                TimeInstance::from_str("2025-01-01T00:00:00Z").unwrap(),
                TimeInstance::from_str("2025-02-01T00:00:00Z").unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_str("2025-02-01T00:00:00Z").unwrap(),
                TimeInstance::from_str("2025-03-01T00:00:00Z").unwrap(),
            ),
        ];

        let files = tiles_by_file_name(&[
            "2025-01-01_tile_x0_y0_b0.tif",
            "2025-01-01_tile_x0_y0_b1.tif",
            "2025-01-01_tile_x0_y1_b0.tif",
            "2025-01-01_tile_x0_y1_b1.tif",
            "2025-01-01_tile_x1_y0_b0.tif",
            "2025-01-01_tile_x1_y0_b1.tif",
            "2025-01-01_tile_x1_y1_b0.tif",
            "2025-01-01_tile_x1_y1_b1.tif",
            "2025-02-01_tile_x0_y0_b0.tif",
            "2025-02-01_tile_x0_y0_b1.tif",
            "2025-02-01_tile_x0_y1_b0.tif",
            "2025-02-01_tile_x0_y1_b1.tif",
            "2025-02-01_tile_x1_y0_b0.tif",
            "2025-02-01_tile_x1_y0_b1.tif",
            "2025-02-01_tile_x1_y1_b0.tif",
            "2025-02-01_tile_x1_y1_b1.tif",
        ]);

        let dataset_name = add_multi_tile_dataset(&mut execution_context, files, time_steps);

        let operator = MultiBandGdalSource {
            params: GdalSourceParameters::new(dataset_name),
        }
        .boxed();

        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized = operator
            .clone()
            .initialize(workflow_operator_path_root, &execution_context)
            .await?;

        let processor = initialized.query_processor()?;

        let tiling_spec = execution_context.tiling_specification();

        let tiling_spatial_grid_definition = processor
            .result_descriptor()
            .spatial_grid_descriptor()
            .tiling_grid_definition(tiling_spec);

        let query_tiling_pixel_grid = tiling_spatial_grid_definition
            .tiling_spatial_grid_definition()
            .spatial_bounds_to_compatible_spatial_grid(SpatialPartition2D::new_unchecked(
                (-180., 90.).into(),
                (180.0, -90.).into(),
            ));

        let query_rect = RasterQueryRectangle::new(
            query_tiling_pixel_grid.grid_bounds(),
            TimeInterval::new(
                geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                    .unwrap(),
                geoengine_datatypes::primitives::TimeInstance::from_str("2025-03-01T00:00:00Z")
                    .unwrap(),
            )
            .unwrap(),
            BandSelection::first_n(2),
        );

        let tiles = processor
            .get_u16()
            .unwrap()
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?;

        let expected_time1 = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-02-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_time2 = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-02-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-03-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_tiles = [
            ("2025-01-01_global_b0_tile_0.tif", 0u32, expected_time1),
            ("2025-01-01_global_b1_tile_0.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_1.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_1.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_2.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_2.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_3.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_3.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_4.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_4.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_5.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_5.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_6.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_6.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_7.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_7.tif", 1, expected_time1),
            ("2025-02-01_global_b0_tile_0.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_0.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_1.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_1.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_2.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_2.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_3.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_3.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_4.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_4.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_5.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_5.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_6.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_6.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_7.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_7.tif", 1, expected_time2),
        ];

        let expected_tiles: Vec<_> = expected_tiles
            .iter()
            .map(|(f, b, t)| {
                raster_tile_from_file::<u16>(
                    test_data!(format!("raster/multi_tile/results/z_index/tiles/{f}")),
                    tiling_spatial_grid_definition,
                    *t,
                    *b,
                )
                .unwrap()
            })
            .collect();

        assert_eq_two_list_of_tiles(&tiles, &expected_tiles, false);

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_loads_multi_band_multi_file_mosaics_with_time_gaps() -> Result<()> {
        let mut execution_context = MockExecutionContext::test_default();
        let query_ctx = execution_context.mock_query_context_test_default();

        let time_steps = vec![
            TimeInterval::new_unchecked(
                TimeInstance::MIN,
                TimeInstance::from_str("2025-01-01T00:00:00Z").unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_str("2025-01-01T00:00:00Z").unwrap(),
                TimeInstance::from_str("2025-02-01T00:00:00Z").unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_str("2025-02-01T00:00:00Z").unwrap(),
                TimeInstance::from_str("2025-03-01T00:00:00Z").unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_str("2025-03-01T00:00:00Z").unwrap(),
                TimeInstance::from_str("2025-04-01T00:00:00Z").unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_str("2025-04-01T00:00:00Z").unwrap(),
                TimeInstance::from_str("2025-05-01T00:00:00Z").unwrap(),
            ),
            TimeInterval::new_unchecked(
                TimeInstance::from_str("2025-05-01T00:00:00Z").unwrap(),
                TimeInstance::MAX,
            ),
        ];

        let files = tiles_by_file_name(&[
            "2025-01-01_tile_x0_y0_b0.tif",
            "2025-01-01_tile_x0_y0_b1.tif",
            "2025-01-01_tile_x0_y1_b0.tif",
            "2025-01-01_tile_x0_y1_b1.tif",
            "2025-01-01_tile_x1_y0_b0.tif",
            "2025-01-01_tile_x1_y0_b1.tif",
            "2025-01-01_tile_x1_y1_b0.tif",
            "2025-01-01_tile_x1_y1_b1.tif",
            "2025-02-01_tile_x0_y0_b0.tif",
            "2025-02-01_tile_x0_y0_b1.tif",
            "2025-02-01_tile_x0_y1_b0.tif",
            "2025-02-01_tile_x0_y1_b1.tif",
            "2025-02-01_tile_x1_y0_b0.tif",
            "2025-02-01_tile_x1_y0_b1.tif",
            "2025-02-01_tile_x1_y1_b0.tif",
            "2025-02-01_tile_x1_y1_b1.tif",
            "2025-04-01_tile_x0_y0_b0.tif",
            "2025-04-01_tile_x0_y0_b1.tif",
            "2025-04-01_tile_x0_y1_b0.tif",
            "2025-04-01_tile_x0_y1_b1.tif",
            "2025-04-01_tile_x1_y0_b0.tif",
            "2025-04-01_tile_x1_y0_b1.tif",
            "2025-04-01_tile_x1_y1_b0.tif",
            "2025-04-01_tile_x1_y1_b1.tif",
        ]);

        let dataset_name = add_multi_tile_dataset(&mut execution_context, files, time_steps);

        let operator = MultiBandGdalSource {
            params: GdalSourceParameters::new(dataset_name),
        }
        .boxed();

        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized = operator
            .clone()
            .initialize(workflow_operator_path_root, &execution_context)
            .await?;

        let processor = initialized.query_processor()?;

        let tiling_spec = execution_context.tiling_specification();

        let tiling_spatial_grid_definition = processor
            .result_descriptor()
            .spatial_grid_descriptor()
            .tiling_grid_definition(tiling_spec);

        let query_tiling_pixel_grid = tiling_spatial_grid_definition
            .tiling_spatial_grid_definition()
            .spatial_bounds_to_compatible_spatial_grid(SpatialPartition2D::new_unchecked(
                (-180., 90.).into(),
                (180.0, -90.).into(),
            ));

        // query a time interval that is greater than the time interval of the tiles and covers a region with a temporal gap
        let query_rect = RasterQueryRectangle::new(
            query_tiling_pixel_grid.grid_bounds(),
            TimeInterval::new(
                geoengine_datatypes::primitives::TimeInstance::from_str("2024-12-01T00:00:00Z")
                    .unwrap(),
                geoengine_datatypes::primitives::TimeInstance::from_str("2025-05-15T00:00:00Z")
                    .unwrap(),
            )
            .unwrap(),
            BandSelection::first_n(2),
        );

        let mut tiles = processor
            .get_u16()
            .unwrap()
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?;

        // first 8 (spatial) x 2 (bands) tiles must be no data
        for tile in tiles.drain(..16) {
            assert_eq!(
                tile.time,
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::MIN,
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                        .unwrap(),
                )
                .unwrap()
            );
            assert!(tile.is_empty());
        }

        // next comes data
        let expected_time1 = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-02-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_time2 = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-02-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-03-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_tiles = [
            ("2025-01-01_global_b0_tile_0.tif", 0u32, expected_time1),
            ("2025-01-01_global_b1_tile_0.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_1.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_1.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_2.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_2.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_3.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_3.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_4.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_4.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_5.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_5.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_6.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_6.tif", 1, expected_time1),
            ("2025-01-01_global_b0_tile_7.tif", 0, expected_time1),
            ("2025-01-01_global_b1_tile_7.tif", 1, expected_time1),
            ("2025-02-01_global_b0_tile_0.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_0.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_1.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_1.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_2.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_2.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_3.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_3.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_4.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_4.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_5.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_5.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_6.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_6.tif", 1, expected_time2),
            ("2025-02-01_global_b0_tile_7.tif", 0, expected_time2),
            ("2025-02-01_global_b1_tile_7.tif", 1, expected_time2),
        ];

        let expected_tiles: Vec<_> = expected_tiles
            .iter()
            .map(|(f, b, t)| {
                raster_tile_from_file::<u16>(
                    test_data!(format!("raster/multi_tile/results/z_index/tiles/{f}")),
                    tiling_spatial_grid_definition,
                    *t,
                    *b,
                )
                .unwrap()
            })
            .collect();

        assert_eq_two_list_of_tiles(
            &tiles.drain(..expected_tiles.len()).collect::<Vec<_>>(),
            &expected_tiles,
            false,
        );

        // next comes a gap of no data
        for tile in tiles.drain(..16) {
            assert_eq!(
                tile.time,
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-03-01T00:00:00Z")
                        .unwrap(),
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-04-01T00:00:00Z")
                        .unwrap(),
                )
                .unwrap()
            );
            assert!(tile.is_empty());
        }

        // next comes data again
        let expected_time = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-04-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-05-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_tiles = [
            ("2025-04-01_global_b0_tile_0.tif", 0u32, expected_time),
            ("2025-04-01_global_b1_tile_0.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_1.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_1.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_2.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_2.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_3.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_3.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_4.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_4.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_5.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_5.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_6.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_6.tif", 1, expected_time),
            ("2025-04-01_global_b0_tile_7.tif", 0, expected_time),
            ("2025-04-01_global_b1_tile_7.tif", 1, expected_time),
        ];

        let expected_tiles: Vec<_> = expected_tiles
            .iter()
            .map(|(f, b, t)| {
                raster_tile_from_file::<u16>(
                    test_data!(format!("raster/multi_tile/results/z_index/tiles/{f}")),
                    tiling_spatial_grid_definition,
                    *t,
                    *b,
                )
                .unwrap()
            })
            .collect();

        assert_eq_two_list_of_tiles(
            &tiles.drain(..expected_tiles.len()).collect::<Vec<_>>(),
            &expected_tiles,
            false,
        );

        // last 8 (spatial) x 2 (bands) tiles must be no data
        for tile in &tiles[..16] {
            assert_eq!(
                tile.time,
                TimeInterval::new(
                    geoengine_datatypes::primitives::TimeInstance::from_str("2025-05-01T00:00:00Z")
                        .unwrap(),
                    geoengine_datatypes::primitives::TimeInstance::MAX
                )
                .unwrap()
            );
            assert!(tile.is_empty());
        }

        Ok(())
    }

    #[tokio::test]
    async fn it_loads_multi_band_multi_file_mosaics_reverse_z_index() -> Result<()> {
        let mut execution_context = MockExecutionContext::test_default();
        let query_ctx = execution_context.mock_query_context_test_default();

        let time_steps = vec![TimeInterval::new_unchecked(
            TimeInstance::from_str("2025-01-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2025-02-01T00:00:00Z").unwrap(),
        )];

        let files = tiles_by_file_name_rev(&[
            "2025-01-01_tile_x1_y1_b0.tif",
            "2025-01-01_tile_x1_y0_b0.tif",
            "2025-01-01_tile_x0_y1_b0.tif",
            "2025-01-01_tile_x0_y0_b0.tif",
        ]);

        let dataset_name = add_multi_tile_dataset(&mut execution_context, files, time_steps);

        let operator = MultiBandGdalSource {
            params: GdalSourceParameters::new(dataset_name),
        }
        .boxed();

        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized = operator
            .clone()
            .initialize(workflow_operator_path_root, &execution_context)
            .await?;

        let processor = initialized.query_processor()?;

        let tiling_spec = execution_context.tiling_specification();

        let tiling_spatial_grid_definition = processor
            .result_descriptor()
            .spatial_grid_descriptor()
            .tiling_grid_definition(tiling_spec);

        let query_tiling_pixel_grid = tiling_spatial_grid_definition
            .tiling_spatial_grid_definition()
            .spatial_bounds_to_compatible_spatial_grid(SpatialPartition2D::new_unchecked(
                (-180., 90.).into(),
                (180.0, -90.).into(),
            ));

        let query_rect = RasterQueryRectangle::new(
            query_tiling_pixel_grid.grid_bounds(),
            TimeInterval::new_instant(
                geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                    .unwrap(),
            )
            .unwrap(),
            BandSelection::first(),
        );

        let tiles = processor
            .get_u16()
            .unwrap()
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?;

        let expected_tiles = [
            "2025-01-01_global_b0_tile_0.tif",
            "2025-01-01_global_b0_tile_1.tif",
            "2025-01-01_global_b0_tile_2.tif",
            "2025-01-01_global_b0_tile_3.tif",
            "2025-01-01_global_b0_tile_4.tif",
            "2025-01-01_global_b0_tile_5.tif",
            "2025-01-01_global_b0_tile_6.tif",
            "2025-01-01_global_b0_tile_7.tif",
        ];

        let expected_time = TimeInterval::new(
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                .unwrap(),
            geoengine_datatypes::primitives::TimeInstance::from_str("2025-02-01T00:00:00Z")
                .unwrap(),
        )
        .unwrap();

        let expected_tiles: Vec<_> = expected_tiles
            .iter()
            .map(|f| {
                raster_tile_from_file::<u16>(
                    test_data!(format!(
                        "raster/multi_tile/results/z_index_reversed/tiles/{f}"
                    )),
                    tiling_spatial_grid_definition,
                    expected_time,
                    0,
                )
                .unwrap()
            })
            .collect();

        assert_eq_two_list_of_tiles(&tiles, &expected_tiles, false);

        Ok(())
    }

    #[tokio::test]
    async fn it_loads_overview_level() -> Result<()> {
        let mut execution_context = MockExecutionContext::test_default();
        let query_ctx = execution_context.mock_query_context_test_default();

        let time_steps = vec![TimeInterval::new_unchecked(
            TimeInstance::from_str("2025-01-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2025-02-01T00:00:00Z").unwrap(),
        )];

        let files = tiles_by_file_name(&[
            "2025-01-01_tile_x0_y0_b0.tif",
            "2025-01-01_tile_x0_y1_b0.tif",
            "2025-01-01_tile_x1_y0_b0.tif",
            "2025-01-01_tile_x1_y1_b0.tif",
        ]);

        let dataset_name = add_multi_tile_dataset(&mut execution_context, files, time_steps);

        let operator = MultiBandGdalSource {
            params: GdalSourceParameters::new_with_overview_level(dataset_name, 2),
        }
        .boxed();

        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized = operator
            .clone()
            .initialize(workflow_operator_path_root, &execution_context)
            .await?;

        let processor = initialized.query_processor()?.get_u16().unwrap();

        let tiling_spec = execution_context.tiling_specification();

        let tiling_spatial_grid_definition = processor
            .result_descriptor()
            .spatial_grid_descriptor()
            .tiling_grid_definition(tiling_spec);

        let query_tiling_pixel_grid = tiling_spatial_grid_definition
            .tiling_spatial_grid_definition()
            .spatial_bounds_to_compatible_spatial_grid(SpatialPartition2D::new_unchecked(
                (-180., 90.).into(),
                (180.0, -90.).into(),
            ));

        let query_rect = RasterQueryRectangle::new(
            query_tiling_pixel_grid.grid_bounds(),
            TimeInterval::new_instant(
                geoengine_datatypes::primitives::TimeInstance::from_str("2025-01-01T00:00:00Z")
                    .unwrap(),
            )
            .unwrap(),
            BandSelection::first(),
        );

        let tiles = processor
            .query(query_rect.clone(), &query_ctx)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?;

        // // Write geotiff_bytes to disk
        // let mut geotiff_bytes = raster_stream_to_geotiff_bytes(
        //     processor,
        //     query_rect,
        //     query_ctx,
        //     GdalGeoTiffDatasetMetadata {
        //         no_data_value: Some(0.),
        //         spatial_reference: SpatialReference::epsg_4326(),
        //     },
        //     GdalGeoTiffOptions {
        //         as_cog: false,
        //         compression_num_threads: GdalCompressionNumThreads::AllCpus,
        //         force_big_tiff: false,
        //     },
        //     None,
        //     Box::pin(futures::future::pending()),
        // )
        // .await?;

        // std::fs::write("overview_level_2.tif", &geotiff_bytes[0])
        //     .expect("Failed to write GeoTIFF file");

        let expected_tiles = [
            "2025-01-01_global_b0_tile_0.tif",
            "2025-01-01_global_b0_tile_1.tif",
            "2025-01-01_global_b0_tile_2.tif",
            "2025-01-01_global_b0_tile_3.tif",
        ];

        let expected_time = TimeInterval::new_unchecked(
            TimeInstance::from_str("2025-01-01T00:00:00Z").unwrap(),
            TimeInstance::from_str("2025-02-01T00:00:00Z").unwrap(),
        );

        let expected_tiles: Vec<_> = expected_tiles
            .iter()
            .map(|f| {
                raster_tile_from_file::<u16>(
                    test_data!(format!(
                        "raster/multi_tile/results/overview_level_2/tiles/{f}"
                    )),
                    tiling_spatial_grid_definition,
                    expected_time,
                    0,
                )
                .unwrap()
            })
            .collect();

        assert_eq_two_list_of_tiles(&tiles, &expected_tiles, false);

        Ok(())
    }
}
