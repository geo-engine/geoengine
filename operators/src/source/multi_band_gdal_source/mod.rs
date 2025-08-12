use crate::engine::{
    CanonicOperatorName, MetaData, OperatorData, OperatorName, QueryProcessor,
    SpatialGridDescriptor, WorkflowOperatorPath,
};
use crate::source::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalMetadataMapping,
};
use crate::util::TemporaryGdalThreadLocalConfigOptions;
use crate::util::gdal::gdal_open_dataset_ex;
use crate::util::retry::retry;
use crate::{
    engine::{
        InitializedRasterOperator, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
        SourceOperator, TypedRasterQueryProcessor,
    },
    error::Error,
    util::Result,
};
use async_trait::async_trait;
pub use error::GdalSourceError;
use float_cmp::approx_eq;
use futures::stream::{self, BoxStream, StreamExt};
use gdal::errors::GdalError;
use gdal::raster::{GdalType, RasterBand as GdalRasterBand};
use gdal::{Dataset as GdalDataset, DatasetOptions, GdalOpenFlags, Metadata as GdalMetadata};
use gdal_sys::VSICurlPartialClearCache;
use geoengine_datatypes::primitives::{QueryRectangle, SpatialPartition2D, SpatialPartitioned};
use geoengine_datatypes::raster::TilingSpatialGridDefinition;
use geoengine_datatypes::{
    dataset::NamedData,
    primitives::{BandSelection, Coordinate2D, RasterQueryRectangle, TimeInterval},
    raster::{
        ChangeGridBounds, EmptyGrid, GeoTransform, Grid, GridBlit, GridBoundingBox2D, GridOrEmpty,
        GridSize, MapElements, MaskedGrid, NoDataValueGrid, Pixel, RasterDataType,
        RasterProperties, RasterPropertiesEntry, RasterPropertiesEntryType, RasterTile2D,
        SpatialGridDefinition, TileInformation, TilingSpecification,
    },
};
pub use loading_info::{GdalMultiBand, MultiBandGdalLoadingInfo, TileFile};
use num::{FromPrimitive, integer::div_ceil, integer::div_floor};
use reader::{
    GdalReadAdvise, GdalReadWindow, GdalReaderMode, GridAndProperties, OverviewReaderState,
    ReaderState,
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::ffi::CString;
use std::marker::PhantomData;
use std::path::Path;
use tracing::{debug, trace};

mod error;
mod loading_info;
mod reader;

static GDAL_RETRY_INITIAL_BACKOFF_MS: u64 = 1000;
static GDAL_RETRY_MAX_BACKOFF_MS: u64 = 60 * 60 * 1000;
static GDAL_RETRY_EXPONENTIAL_BACKOFF_FACTOR: f64 = 2.;

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

pub type MultiBandGdalLoadingInfoQueryRectangle = QueryRectangle<SpatialPartition2D, BandSelection>;

fn raster_query_rectangle_to_loading_info_query_rectangle(
    raster_query_rectangle: &RasterQueryRectangle,
    tiling_spatial_grid: TilingSpatialGridDefinition,
) -> MultiBandGdalLoadingInfoQueryRectangle {
    MultiBandGdalLoadingInfoQueryRectangle::new(
        tiling_spatial_grid
            .tiling_geo_transform()
            .grid_to_spatial_bounds(&raster_query_rectangle.spatial_bounds()),
        raster_query_rectangle.time_interval(),
        raster_query_rectangle.attributes().clone(),
    )
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct TilingInformation {
    pub x_axis_tiles: usize,
    pub y_axis_tiles: usize,
    pub x_axis_tile_size: usize,
    pub y_axis_tile_size: usize,
}

pub struct GdalSourceProcessor<T>
where
    T: Pixel,
{
    pub produced_result_descriptor: RasterResultDescriptor,
    pub tiling_specification: TilingSpecification,
    pub meta_data: MultiBandGdalMetaData,
    pub _overview_level: u32, // TODO: is it correct that this is never needed?
    pub original_resolution_spatial_grid: Option<SpatialGridDefinition>,
    pub _phantom_data: PhantomData<T>,
}

struct GdalRasterLoader {}

impl GdalRasterLoader {
    async fn load_tile_from_files_async<T: Pixel + GdalType + FromPrimitive>(
        loading_info: MultiBandGdalLoadingInfo,
        reader_mode: GdalReaderMode,
        tile_information: TileInformation,
        time: TimeInterval,
        band: u32,
    ) -> Result<RasterTile2D<T>> {
        debug!(
            "loading tile {:?} for time: {}, band: {band}",
            tile_information.global_tile_position.inner(),
            time.to_string()
        );
        let tile_files = loading_info.tile_files(time, tile_information, band);

        for tile_file in &tile_files {
            trace!(
                "tile_file: {:?}, {:?}",
                tile_file.file_path,
                tile_file
                    .spatial_grid_definition()
                    .geo_transform
                    .origin_coordinate
            );
        }

        let mut tile_raster: GridOrEmpty<GridBoundingBox2D, T> =
            GridOrEmpty::from(EmptyGrid::new(tile_information.global_pixel_bounds()));

        let mut properties = RasterProperties::default();
        let cache_hint = loading_info.cache_hint();

        for dataset_params in tile_files {
            let Some(file_tile) = Self::retrying_load_raster_tile_from_file::<T>(
                &dataset_params,
                reader_mode,
                tile_information,
            )
            .await?
            else {
                debug!("didn't load from file: {:?}", dataset_params.file_path);
                continue;
            };
            tile_raster.grid_blit_from(&file_tile.grid);

            properties = file_tile.properties;
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

    async fn retrying_load_raster_tile_from_file<T: Pixel + GdalType + FromPrimitive>(
        dataset_params: &GdalDatasetParameters,
        reader_mode: GdalReaderMode,
        tile_information: TileInformation,
    ) -> Result<Option<GridAndProperties<T>>> {
        // TODO: detect usage of vsi curl properly, e.g. also check for `/vsicurl_streaming` and combinations with `/vsizip`
        let is_vsi_curl = dataset_params.file_path.starts_with("/vsicurl/");

        retry(
            dataset_params
                .retry
                .map(|r| r.max_retries)
                .unwrap_or_default(),
            GDAL_RETRY_INITIAL_BACKOFF_MS,
            GDAL_RETRY_EXPONENTIAL_BACKOFF_FACTOR,
            Some(GDAL_RETRY_MAX_BACKOFF_MS),
            move || {
                let ds = dataset_params.clone();
                let file_path = ds.file_path.clone();

                async move {
                    let load_tile_result = crate::util::spawn_blocking(move || {
                        Self::load_raster_tile_from_file(&ds, reader_mode, tile_information)
                    })
                    .await
                    .context(crate::error::TokioJoin);

                    match load_tile_result {
                        Ok(Ok(r)) => Ok(r),
                        Ok(Err(e)) | Err(e) => {
                            if is_vsi_curl {
                                // clear the VSICurl cache, to force GDAL to try to re-download the file
                                // otherwise it will assume any observed error will happen again
                                clear_gdal_vsi_cache_for_path(file_path.as_path());
                            }

                            Err(e)
                        }
                    }
                }
            },
        )
        .await
    }

    fn load_raster_tile_from_file<T: Pixel + GdalType + FromPrimitive>(
        dataset_params: &GdalDatasetParameters,
        reader_mode: GdalReaderMode,
        tile_information: TileInformation,
    ) -> Result<Option<GridAndProperties<T>>> {
        debug!(
            "Loading raster tile from file: {:?}",
            dataset_params.file_path.file_name().unwrap_or_default()
        );
        let gdal_read_advise: Option<GdalReadAdvise> = reader_mode.tiling_to_dataset_read_advise(
            &dataset_params.spatial_grid_definition(),
            &tile_information.spatial_grid_definition(),
        );

        let Some(gdal_read_advise) = gdal_read_advise else {
            return Ok(None);
        };

        let options = dataset_params
            .gdal_open_options
            .as_ref()
            .map(|o| o.iter().map(String::as_str).collect::<Vec<_>>());

        // reverts the thread local configs on drop
        let _thread_local_configs = dataset_params
            .gdal_config_options
            .as_ref()
            .map(|config_options| TemporaryGdalThreadLocalConfigOptions::new(config_options));

        let dataset_result = gdal_open_dataset_ex(
            &dataset_params.file_path,
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_RASTER,
                open_options: options.as_deref(),
                ..DatasetOptions::default()
            },
        );

        let dataset = match dataset_result {
            Ok(dataset) => dataset,
            Err(error) => {
                let is_file_not_found = error_is_gdal_file_not_found(&error);

                match dataset_params.file_not_found_handling {
                    FileNotFoundHandling::NoData if is_file_not_found => return Ok(None),
                    _ => {
                        return Err(crate::error::Error::CouldNotOpenGdalDataset {
                            file_path: dataset_params.file_path.to_string_lossy().to_string(),
                        });
                    }
                };
            }
        };

        let rasterband = dataset.rasterband(dataset_params.rasterband_channel)?;

        // overwrite old properties with the properties of the current dataset (z-index)
        let properties = read_raster_properties(&dataset, dataset_params, &rasterband);

        let gdal_dataset_geotransform = GdalDatasetGeoTransform::from(dataset.geo_transform()?);
        // check that the dataset geo transform is the same as the one we get from GDAL
        debug_assert!(approx_eq!(
            Coordinate2D,
            gdal_dataset_geotransform.origin_coordinate,
            dataset_params.geo_transform.origin_coordinate
        ));

        debug_assert!(approx_eq!(
            f64,
            gdal_dataset_geotransform.x_pixel_size,
            dataset_params.geo_transform.x_pixel_size
        ));

        debug_assert!(approx_eq!(
            f64,
            gdal_dataset_geotransform.y_pixel_size,
            dataset_params.geo_transform.y_pixel_size
        ));

        let (gdal_dataset_pixels_x, gdal_dataset_pixels_y) = dataset.raster_size();
        // check that the dataset pixel size is the same as the one we get from GDAL
        debug_assert_eq!(gdal_dataset_pixels_x, dataset_params.width);
        debug_assert_eq!(gdal_dataset_pixels_y, dataset_params.height);

        Ok(Some(GridAndProperties {
            grid: read_grid_from_raster::<T, GridBoundingBox2D>(
                &rasterband,
                &gdal_read_advise.gdal_read_widow,
                gdal_read_advise.read_window_bounds,
                dataset_params,
                gdal_read_advise.flip_y,
            )?,
            properties,
        }))
    }
}

fn error_is_gdal_file_not_found(error: &Error) -> bool {
    matches!(
        error,
        Error::Gdal {
            source: GdalError::NullPointer {
                method_name,
                msg
            },
        } if *method_name == "GDALOpenEx" && (*msg == "HTTP response code: 404" || msg.ends_with("No such file or directory"))
    )
}

fn clear_gdal_vsi_cache_for_path(file_path: &Path) {
    unsafe {
        if let Some(Some(c_string)) = file_path.to_str().map(|s| CString::new(s).ok()) {
            VSICurlPartialClearCache(c_string.as_ptr());
        }
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
        _ctx: &'a dyn crate::engine::QueryContext,
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
                })
            }
        };

        let loading_info = self
            .meta_data
            .loading_info(raster_query_rectangle_to_loading_info_query_rectangle(
                &query,
                produced_tiling_grid,
            ))
            .await?;

        let time_steps = loading_info.time_steps().to_vec();
        let bands = query.attributes().clone().as_vec();
        let spatial_tiles = tiling_strategy
            .tile_information_iterator_from_grid_bounds(query.spatial_bounds())
            .collect::<Vec<_>>();

        for tile_info in &spatial_tiles {
            debug!(
                "Output Tile: pixel: {:?}, crs: {:?}",
                tile_info.global_pixel_bounds(),
                tile_info.spatial_partition()
            );
        }

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
                self.params.data.to_string(),
                meta_data,
                meta_data_result_descriptor,
                context.tiling_specification(),
            )
        } else {
            // generate a result descriptor with the overview level
            InitializedGdalSourceOperator::initialize_with_overview_level(
                op_name,
                path,
                self.params.data.to_string(),
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
    data: String,
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
        data: String,
        meta_data: MultiBandGdalMetaData,
        result_descriptor: RasterResultDescriptor,
        tiling_specification: TilingSpecification,
    ) -> Self {
        InitializedGdalSourceOperator {
            name,
            path,
            data,
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
        data: String,
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
            data,
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
                    _overview_level: self.overview_level,
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
                    _overview_level: self.overview_level,
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
                    _overview_level: self.overview_level,
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
                    _overview_level: self.overview_level,
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
                    _overview_level: self.overview_level,
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
                    _overview_level: self.overview_level,
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
                    _overview_level: self.overview_level,
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
        Some(self.data.clone())
    }
}

/// This method reads the data for a single grid with a specified size from the GDAL dataset.
/// It fails if the tile is not within the dataset.
#[allow(clippy::float_cmp)]
fn read_grid_from_raster<T, D>(
    rasterband: &GdalRasterBand,
    read_window: &GdalReadWindow,
    out_shape: D,
    dataset_params: &GdalDatasetParameters,
    flip_y_axis: bool,
) -> Result<GridOrEmpty<D, T>>
where
    T: Pixel + GdalType + Default + FromPrimitive,
    D: Clone + GridSize + PartialEq,
{
    let gdal_out_shape = (out_shape.axis_size_x(), out_shape.axis_size_y());

    let buffer = rasterband.read_as::<T>(
        read_window.gdal_window_start(), // pixelspace origin
        read_window.gdal_window_size(),  // pixelspace size
        gdal_out_shape,                  // requested raster size
        None,                            // sampling mode
    )?;
    let (_, buffer_data) = buffer.into_shape_and_vec();
    let data_grid = Grid::new(out_shape.clone(), buffer_data)?;

    let data_grid = if flip_y_axis {
        data_grid.reversed_y_axis_grid()
    } else {
        data_grid
    };

    let dataset_mask_flags = rasterband.mask_flags()?;

    if dataset_mask_flags.is_all_valid() {
        debug!("all pixels are valid --> skip no-data and mask handling.");
        return Ok(MaskedGrid::new_with_data(data_grid).into());
    }

    if dataset_mask_flags.is_nodata() {
        debug!("raster uses a no-data value --> use no-data handling.");
        let no_data_value = dataset_params
            .no_data_value
            .or_else(|| rasterband.no_data_value())
            .and_then(FromPrimitive::from_f64);
        let no_data_value_grid = NoDataValueGrid::new(data_grid, no_data_value);
        let grid_or_empty = GridOrEmpty::from(no_data_value_grid);
        return Ok(grid_or_empty);
    }

    if dataset_mask_flags.is_alpha() {
        debug!("raster uses alpha band to mask pixels.");
        if !dataset_params.allow_alphaband_as_mask {
            return Err(Error::AlphaBandAsMaskNotAllowed);
        }
    }

    debug!("use mask based no-data handling.");

    let mask_band = rasterband.open_mask_band()?;
    let mask_buffer = mask_band.read_as::<u8>(
        read_window.gdal_window_start(), // pixelspace origin
        read_window.gdal_window_size(),  // pixelspace size
        gdal_out_shape,                  // requested raster size
        None,                            // sampling mode
    )?;
    let (_, mask_buffer_data) = mask_buffer.into_shape_and_vec();
    let mask_grid = Grid::new(out_shape, mask_buffer_data)?.map_elements(|p: u8| p > 0);

    let mask_grid = if flip_y_axis {
        mask_grid.reversed_y_axis_grid()
    } else {
        mask_grid
    };

    let masked_grid = MaskedGrid::new(data_grid, mask_grid)?;
    Ok(GridOrEmpty::from(masked_grid))
}

/// This method reads the data for a single tile with a specified size from the GDAL dataset and adds the requested metadata as properties to the tile.
fn read_raster_properties(
    dataset: &GdalDataset,
    dataset_params: &GdalDatasetParameters,
    rasterband: &GdalRasterBand,
) -> RasterProperties {
    let mut properties = RasterProperties::default();

    // always read the scale and offset values from the rasterband
    properties_from_band(&mut properties, rasterband);

    // read the properties from the dataset and rasterband metadata
    if let Some(properties_mapping) = dataset_params.properties_mapping.as_ref() {
        properties_from_gdal_metadata(&mut properties, dataset, properties_mapping);
        properties_from_gdal_metadata(&mut properties, rasterband, properties_mapping);
    }

    properties
}

fn properties_from_gdal_metadata<'a, I, M>(
    properties: &mut RasterProperties,
    gdal_dataset: &M,
    properties_mapping: I,
) where
    I: IntoIterator<Item = &'a GdalMetadataMapping>,
    M: GdalMetadata,
{
    let mapping_iter = properties_mapping.into_iter();

    for m in mapping_iter {
        let data = if let Some(domain) = &m.source_key.domain {
            gdal_dataset.metadata_item(&m.source_key.key, domain)
        } else {
            gdal_dataset.metadata_item(&m.source_key.key, "")
        };

        if let Some(d) = data {
            let entry = match m.target_type {
                RasterPropertiesEntryType::Number => d.parse::<f64>().map_or_else(
                    |_| RasterPropertiesEntry::String(d),
                    RasterPropertiesEntry::Number,
                ),
                RasterPropertiesEntryType::String => RasterPropertiesEntry::String(d),
            };

            debug!(
                "gdal properties key \"{:?}\" => target key \"{:?}\". Value: {:?} ",
                &m.source_key, &m.target_key, &entry
            );

            properties.insert_property(m.target_key.clone(), entry);
        }
    }
}

fn properties_from_band(properties: &mut RasterProperties, gdal_dataset: &GdalRasterBand) {
    if let Some(scale) = gdal_dataset.scale() {
        properties.set_scale(scale);
    }
    if let Some(offset) = gdal_dataset.offset() {
        properties.set_offset(offset);
    }

    // ignore if there is no description
    if let Ok(description) = gdal_dataset.description() {
        properties.set_description(description);
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::str::FromStr;

    use super::*;
    use crate::engine::{
        ExecutionContext, MockExecutionContext, RasterBandDescriptor, StaticMetaData,
    };
    use crate::test_data;
    use crate::util::test::raster_tile_from_file;
    use futures::TryStreamExt;
    use geoengine_datatypes::dataset::{DataId, DatasetId};
    use geoengine_datatypes::primitives::{
        CacheHint, Measurement, SpatialPartition2D, TimeInstance,
    };
    use geoengine_datatypes::raster::{GridBounds, GridIdx2D};
    use geoengine_datatypes::raster::{RasterPropertiesKey, SpatialGridDefinition};
    use geoengine_datatypes::raster::{TileInformation, TilingStrategy};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::Identifier;
    use geoengine_datatypes::util::gdal::hide_gdal_errors;
    use geoengine_datatypes::util::test::{TestDefault, assert_eq_two_list_of_tiles};
    use httptest::matchers::request;
    use httptest::{Expectation, Server, responders};

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
            .tile_information_iterator_from_grid_bounds(grid_bounds)
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

    #[test]
    fn read_raster_and_offset_scale() {
        let up_side_down_params = GdalDatasetParameters {
            file_path: test_data!("raster/modis_ndvi/cropped/MOD13A2_M_NDVI_2014-04-01_30x30.tif")
                .into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (8.0, 57.4).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 30,
            height: 30,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(255.),
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        };

        let tile_information = TileInformation::new(
            [0, 0].into(),
            [8, 8].into(),
            up_side_down_params.geo_transform.try_into().unwrap(),
        );
        let reader_mode = GdalReaderMode::OriginalResolution(ReaderState {
            dataset_spatial_grid: SpatialGridDefinition::new(
                up_side_down_params.geo_transform.try_into().unwrap(),
                GridBoundingBox2D::new([0, 0], [29, 29]).unwrap(),
            ),
        });

        let GridAndProperties { grid, properties } =
            GdalRasterLoader::load_raster_tile_from_file::<u8>(
                &up_side_down_params,
                reader_mode,
                tile_information,
            )
            .unwrap()
            .unwrap();

        assert!(!grid.is_empty());

        let grid = grid.into_materialized_masked_grid();

        assert_eq!(grid.inner_grid.data.len(), 64);
        // pixel value are the top left 8x8 block from MOD13A2_M_NDVI_2014-04-01_27x27_bytes.txt
        assert_eq!(
            grid.inner_grid.data,
            &[
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 255, 255, 127, 107, 255, 255, 255, 255, 255, 164, 185, 182,
                255, 255, 255, 175, 186, 190, 167, 140, 255, 255, 161, 175, 184, 173, 170, 188,
                255, 255, 128, 177, 165, 145, 191, 174, 255, 117, 100, 174, 159, 147, 99, 135
            ]
        );

        assert_eq!(grid.validity_mask.data.len(), 64);
        // pixel mask is pixel > 0 from the top left 8x8 block from MOD13A2_M_NDVI_2014-04-01_27x27_bytes.txt
        assert_eq!(
            grid.validity_mask.data,
            &[
                false, false, false, false, false, false, false, false, false, false, false, false,
                false, false, false, false, false, false, false, false, false, false, true, true,
                false, false, false, false, false, true, true, true, false, false, false, true,
                true, true, true, true, false, false, true, true, true, true, true, true, false,
                false, true, true, true, true, true, true, false, true, true, true, true, true,
                true, true
            ]
        );

        assert_eq!(properties.offset_option(), Some(1.));
        assert_eq!(properties.scale_option(), Some(2.));

        assert!(approx_eq!(f64, properties.offset(), 1.));
        assert!(approx_eq!(f64, properties.scale(), 2.));
    }

    #[test]
    fn it_creates_no_data_only_for_missing_files() {
        hide_gdal_errors();

        let ds = GdalDatasetParameters {
            file_path: "nonexisting_file.tif".into(),
            rasterband_channel: 1,
            geo_transform: TestDefault::test_default(),
            width: 100,
            height: 100,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: None,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        };

        let tile_information = TileInformation::new(
            [0, 0].into(),
            [8, 8].into(),
            ds.geo_transform.try_into().unwrap(),
        );
        let reader_mode = GdalReaderMode::OriginalResolution(ReaderState {
            dataset_spatial_grid: SpatialGridDefinition::new(
                ds.geo_transform.try_into().unwrap(),
                GridBoundingBox2D::new([0, 0], [29, 29]).unwrap(),
            ),
        });

        let res =
            GdalRasterLoader::load_raster_tile_from_file::<u8>(&ds, reader_mode, tile_information);

        assert!(res.is_ok());

        let res = res.unwrap();

        assert!(res.is_none());

        let ds = GdalDatasetParameters {
            file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF").into(),
            rasterband_channel: 100, // invalid channel
            geo_transform: TestDefault::test_default(),
            width: 100,
            height: 100,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: None,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        };

        // invalid channel => error
        let result =
            GdalRasterLoader::load_raster_tile_from_file::<u8>(&ds, reader_mode, tile_information);
        assert!(result.is_err());
    }

    #[test]
    fn it_creates_no_data_only_for_http_404() {
        let server = Server::run();

        server.expect(
            Expectation::matching(request::method_path("HEAD", "/non_existing.tif"))
                .times(1)
                .respond_with(responders::cycle![responders::status_code(404),]),
        );

        server.expect(
            Expectation::matching(request::method_path("HEAD", "/internal_error.tif"))
                .times(1)
                .respond_with(responders::cycle![responders::status_code(500),]),
        );

        let ds = GdalDatasetParameters {
            file_path: format!("/vsicurl/{}", server.url_str("/non_existing.tif")).into(),
            rasterband_channel: 1,
            geo_transform: TestDefault::test_default(),
            width: 100,
            height: 100,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: None,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: Some(vec![
                (
                    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS".to_owned(),
                    ".tif".to_owned(),
                ),
                (
                    "GDAL_DISABLE_READDIR_ON_OPEN".to_owned(),
                    "EMPTY_DIR".to_owned(),
                ),
                ("GDAL_HTTP_NETRC".to_owned(), "NO".to_owned()),
                ("GDAL_HTTP_MAX_RETRY".to_owned(), "0".to_string()),
            ]),
            allow_alphaband_as_mask: true,
            retry: None,
        };

        // 404 => no data
        let tile_information = TileInformation::new(
            [0, 0].into(),
            [8, 8].into(),
            ds.geo_transform.try_into().unwrap(),
        );
        let reader_mode = GdalReaderMode::OriginalResolution(ReaderState {
            dataset_spatial_grid: SpatialGridDefinition::new(
                ds.geo_transform.try_into().unwrap(),
                GridBoundingBox2D::new([0, 0], [29, 29]).unwrap(),
            ),
        });

        let res =
            GdalRasterLoader::load_raster_tile_from_file::<u8>(&ds, reader_mode, tile_information);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.is_none());

        let ds = GdalDatasetParameters {
            file_path: format!("/vsicurl/{}", server.url_str("/internal_error.tif")).into(),
            rasterband_channel: 1,
            geo_transform: TestDefault::test_default(),
            width: 100,
            height: 100,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: None,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: Some(vec![
                (
                    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS".to_owned(),
                    ".tif".to_owned(),
                ),
                (
                    "GDAL_DISABLE_READDIR_ON_OPEN".to_owned(),
                    "EMPTY_DIR".to_owned(),
                ),
                ("GDAL_HTTP_NETRC".to_owned(), "NO".to_owned()),
                ("GDAL_HTTP_MAX_RETRY".to_owned(), "0".to_string()),
            ]),
            allow_alphaband_as_mask: true,
            retry: None,
        };

        // 500 => error
        let res =
            GdalRasterLoader::load_raster_tile_from_file::<u8>(&ds, reader_mode, tile_information);
        assert!(res.is_err());
    }

    #[test]
    fn it_retries_only_after_clearing_vsi_cache() {
        hide_gdal_errors();

        let server = Server::run();

        server.expect(
            Expectation::matching(request::method_path("HEAD", "/foo.tif"))
                .times(2)
                .respond_with(responders::cycle![
                    // first generic error
                    responders::status_code(500),
                    // then 404 file not found
                    responders::status_code(404)
                ]),
        );

        let file_path: PathBuf = format!("/vsicurl/{}", server.url_str("/foo.tif")).into();

        let options = Some(vec![
            (
                "CPL_VSIL_CURL_ALLOWED_EXTENSIONS".to_owned(),
                ".tif".to_owned(),
            ),
            (
                "GDAL_DISABLE_READDIR_ON_OPEN".to_owned(),
                "EMPTY_DIR".to_owned(),
            ),
            ("GDAL_HTTP_NETRC".to_owned(), "NO".to_owned()),
            ("GDAL_HTTP_MAX_RETRY".to_owned(), "0".to_string()),
        ]);

        let _thread_local_configs = options
            .as_ref()
            .map(|config_options| TemporaryGdalThreadLocalConfigOptions::new(config_options));

        // first fail
        let result = gdal_open_dataset_ex(
            file_path.as_path(),
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_RASTER,
                ..DatasetOptions::default()
            },
        );

        // it failed, but not with file not found
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(!error_is_gdal_file_not_found(&error));
        }

        // second fail doesn't even try, so still not "file not found", even though it should be now
        let result = gdal_open_dataset_ex(
            file_path.as_path(),
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_RASTER,
                ..DatasetOptions::default()
            },
        );

        assert!(result.is_err());
        if let Err(error) = result {
            assert!(!error_is_gdal_file_not_found(&error));
        }

        clear_gdal_vsi_cache_for_path(file_path.as_path());

        // after clearing the cache, it tries again
        let result = gdal_open_dataset_ex(
            file_path.as_path(),
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_RASTER,
                ..DatasetOptions::default()
            },
        );

        // now we get the file not found error
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(error_is_gdal_file_not_found(&error));
        }
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

        let meta: MultiBandGdalMetaData = Box::new(StaticMetaData {
            loading_info: MultiBandGdalLoadingInfo::new(time_steps, files, CacheHint::default()),
            result_descriptor: RasterResultDescriptor {
                data_type: RasterDataType::U16,
                spatial_reference: SpatialReference::epsg_4326().into(),
                time: None,
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
}
