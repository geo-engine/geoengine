use crate::engine::{
    CanonicOperatorName, MetaData, OperatorData, OperatorName, QueryProcessor,
    SpatialGridDescriptor, WorkflowOperatorPath,
};
use crate::optimization::{OptimizableOperator, OptimizationError, SourcesMustNotUseOverviews};
use crate::source::gdal_source::process::{
    GdalDataVariant, GdalErrorKind, GdalIpcPayload, IpcChannelMessage, IpcChannelMessagePayload,
};
use crate::source::{GdalDatasetCache, IpcProcessError};

pub use crate::source::gdal_source::process_pool_7::{GdalProcessPool, LazyGdalWorkerInstance};
use crate::util::input::float_option_with_nan;
use crate::util::retry::retry_sync;
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
use float_cmp::{ApproxEq, approx_eq};
use futures::{Future, TryFutureExt, TryStreamExt};
use futures::{
    Stream,
    stream::{self, BoxStream, StreamExt},
};
use gdal::errors::GdalError;
use gdal::raster::{GdalType, RasterBand as GdalRasterBand};
use gdal::{Dataset as GdalDataset, Metadata as GdalMetadata};
use gdal_sys::VSICurlPartialClearCache;
use geoengine_datatypes::dataset::NamedData;
use geoengine_datatypes::primitives::{
    BandSelection, CacheHint, Coordinate2D, DateTimeParseFormat, RasterQueryRectangle,
    SpatialResolution, TimeInterval, TryIrregularTimeFillIterExt, TryRegularTimeFillIterExt,
    find_next_best_overview_level,
};
use reader::{GdalReadAdvise, GridAndProperties, ReaderState};

use geoengine_datatypes::raster::{
    ChangeGridBounds, EmptyGrid, GeoTransform, GridBlit, GridBoundingBox2D, GridOrEmpty,
    GridShape2D, GridShapeAccess, GridSize, MaskedGrid, Pixel, RasterDataType, RasterProperties,
    RasterPropertiesEntry, RasterPropertiesEntryType, RasterPropertiesKey, RasterTile2D,
    SpatialGridDefinition, TileInformation, TilingSpecification, TilingStrategy,
};
use geoengine_datatypes::util::test::TestDefault;
use itertools::Itertools;
pub use loading_info::{
    GdalLoadingInfo, GdalLoadingInfoTemporalSlice, GdalLoadingInfoTemporalSliceIterator,
    GdalMetaDataList, GdalMetaDataRegular, GdalMetaDataStatic, GdalMetadataNetCdfCf,
};
use num::FromPrimitive;
use num::integer::{div_ceil, div_floor};
use postgres_types::{FromSql, ToSql};
use reader::{GdalReadWindow, GdalReaderMode, OverviewReaderState};
use serde::{Deserialize, Serialize};
use snafu::{Snafu, ensure};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::ffi::CString;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tracing::{debug, warn};

mod db_types;
pub mod error;
pub mod loading_info;
pub mod process;
pub mod process_pool_7;
pub mod reader;

static GDAL_RETRY_INITIAL_BACKOFF_MS: u64 = 1000;
static GDAL_RETRY_MAX_BACKOFF_MS: u64 = 60 * 60 * 1000;
static GDAL_RETRY_EXPONENTIAL_BACKOFF_FACTOR: f64 = 2.;

pub trait GdalProcessPoolAccess {
    fn get_gdal_pool(&self) -> &std::sync::Arc<GdalProcessPool>;
    fn get_gdal_worker(&self) -> LazyGdalWorkerInstance {
        LazyGdalWorkerInstance::new(self.get_gdal_pool().clone())
    }
}

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub struct GdalSourceTimePlaceholder {
    pub format: DateTimeParseFormat,
    pub reference: TimeReference,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub enum TimeReference {
    Start,
    End,
}

/// Parameters for loading data using Gdal
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GdalDatasetParameters {
    pub file_path: PathBuf,
    pub rasterband_channel: usize,
    pub geo_transform: GdalDatasetGeoTransform, // TODO: discuss if we need this at all
    pub width: usize,
    pub height: usize,
    pub file_not_found_handling: FileNotFoundHandling,
    #[serde(default)]
    #[serde(with = "float_option_with_nan")]
    pub no_data_value: Option<f64>,
    pub properties_mapping: Option<Vec<GdalMetadataMapping>>,
    // Dataset open option as strings, e.g. `vec!["UserPwd=geoengine:pwd".to_owned(), "HttpAuth=BASIC".to_owned()]`
    pub gdal_open_options: Option<Vec<String>>,
    // Configs as key, value pairs that will be set as thread local config options, e.g.
    // `vec!["AWS_REGION".to_owned(), "eu-central-1".to_owned()]` and unset afterwards
    // TODO: validate the config options: only allow specific keys and specific values
    pub gdal_config_options: Option<Vec<(String, String)>>,
    #[serde(default)]
    pub allow_alphaband_as_mask: bool,
    pub retry: Option<GdalRetryOptions>,
}

impl GdalDatasetParameters {
    pub fn dataset_bounds(&self) -> GridBoundingBox2D {
        GridBoundingBox2D::new_unchecked(
            [0, 0],
            [self.height as isize - 1, self.width as isize - 1],
        )
    }

    pub fn gdal_geo_transform(&self) -> GdalDatasetGeoTransform {
        self.geo_transform
    }

    /// Returns the `SpatialGridDefinition` of the Gdal dataset.
    ///
    /// Note: This allows upside down datasets (where `GeoTransform` `y_pixel_size` is positive)!
    ///
    /// # Panics
    /// Panics if the `GdalDatasetParameters` are faulty.
    pub fn spatial_grid_definition(&self) -> SpatialGridDefinition {
        let gdal_geo_transform = GeoTransform::new(
            self.gdal_geo_transform().origin_coordinate,
            self.gdal_geo_transform().x_pixel_size,
            self.gdal_geo_transform().y_pixel_size,
        );

        SpatialGridDefinition::new(gdal_geo_transform, self.dataset_bounds())
    }

    pub fn max_retries(&self) -> Option<usize> {
        self.retry.map(|r| r.max_retries)
    }

    pub fn partial_hash<H: Hasher>(&self, state: &mut H) {
        // 1. Hash the file path
        self.file_path.hash(state);

        // 2. Hash gdal_open_options
        self.gdal_open_options.hash(state);

        // 3. Hash gdal_config_options
        self.gdal_config_options.hash(state);
    }
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub struct GdalRetryOptions {
    pub max_retries: usize,
}

/// A user friendly representation of Gdal's geo transform. In contrast to [`GeoTransform`] this
/// geo transform allows arbitrary pixel sizes and can thus also represent rasters where the origin is not located
/// in the upper left corner. It should only be used for loading rasters with Gdal and not internally.
/// The GDAL pixel space is usually anchored at the "top-left" corner of the data spatial bounds. Therefore the raster data is stored with spatial coordinate y-values decreasing with the rasters rows. This is represented by a negative pixel size.
/// However, there are datasets where the data is stored "upside-down". If this is the case, the pixel size is positive.
#[derive(Copy, Clone, PartialEq, Debug, Serialize, Deserialize, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub struct GdalDatasetGeoTransform {
    pub origin_coordinate: Coordinate2D,
    pub x_pixel_size: f64,
    pub y_pixel_size: f64,
}

/// Default implementation for testing purposes where geo transform doesn't matter
impl TestDefault for GdalDatasetGeoTransform {
    fn test_default() -> Self {
        Self {
            origin_coordinate: (0.0, 0.0).into(),
            x_pixel_size: 1.0,
            y_pixel_size: -1.0,
        }
    }
}

impl ApproxEq for GdalDatasetGeoTransform {
    type Margin = float_cmp::F64Margin;

    fn approx_eq<M>(self, other: Self, margin: M) -> bool
    where
        M: Into<Self::Margin>,
    {
        let m = margin.into();
        self.origin_coordinate.approx_eq(other.origin_coordinate, m)
            && self.x_pixel_size.approx_eq(other.x_pixel_size, m)
            && self.y_pixel_size.approx_eq(other.y_pixel_size, m)
    }
}

/// Direct conversion from `GdalDatasetGeoTransform` to [`GeoTransform`] only works if origin is located in the upper left corner.
impl TryFrom<GdalDatasetGeoTransform> for GeoTransform {
    type Error = Error;

    fn try_from(dataset_geo_transform: GdalDatasetGeoTransform) -> Result<Self> {
        ensure!(
            dataset_geo_transform.x_pixel_size != 0.0 && dataset_geo_transform.y_pixel_size != 0.0,
            crate::error::GeoTransformOrigin // TODO new name?
        );

        Ok(GeoTransform::new(
            dataset_geo_transform.origin_coordinate,
            dataset_geo_transform.x_pixel_size,
            dataset_geo_transform.y_pixel_size,
        ))
    }
}

impl From<gdal::GeoTransform> for GdalDatasetGeoTransform {
    fn from(gdal_geo_transform: gdal::GeoTransform) -> Self {
        Self {
            origin_coordinate: (gdal_geo_transform[0], gdal_geo_transform[3]).into(),
            x_pixel_size: gdal_geo_transform[1],
            y_pixel_size: gdal_geo_transform[5],
        }
    }
}

impl GridShapeAccess for GdalDatasetParameters {
    type ShapeArray = [usize; 2];

    fn grid_shape_array(&self) -> Self::ShapeArray {
        [self.height, self.width]
    }
}

/// How to handle file not found errors
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, FromSql, ToSql)]
pub enum FileNotFoundHandling {
    NoData, // output tiles filled with nodata
    Error,  // return error tile
}

impl GdalDatasetParameters {
    /// Placeholders are replaced by formatted time value.
    /// E.g. `%my_placeholder%` could be replaced by `2014-04-01` depending on the format and time input.
    pub fn replace_time_placeholders(
        &self,
        placeholders: &HashMap<String, GdalSourceTimePlaceholder>,
        time: TimeInterval,
    ) -> Result<Self> {
        let mut file_path: String = self.file_path.to_string_lossy().into();

        for (placeholder, time_placeholder) in placeholders {
            let time = match time_placeholder.reference {
                TimeReference::Start => time.start(),
                TimeReference::End => time.end(),
            };
            let time_string = time
                .as_date_time()
                .ok_or(Error::TimeInstanceNotDisplayable)?
                .format(&time_placeholder.format);

            // TODO: use more efficient algorithm for replacing multiple placeholders, e.g. aho-corasick
            file_path = file_path.replace(placeholder, &time_string);
        }

        Ok(Self {
            file_not_found_handling: self.file_not_found_handling,
            file_path: file_path.into(),
            properties_mapping: self.properties_mapping.clone(),
            gdal_open_options: self.gdal_open_options.clone(),
            gdal_config_options: self.gdal_config_options.clone(),
            ..*self
        })
    }
}

#[derive(Snafu, Debug)]
pub enum GdalRasterLoaderError {
    GdalError {
        source: GdalError,
    },

    GdalFileNotFound {
        source: GdalError,
    },

    ShapeBufferMissmatch {
        shape: GridShape2D,
        buffer_len: usize,
    },

    AlphaBandAsMaskNotAllowed,
}

impl From<GdalError> for GdalRasterLoaderError {
    fn from(source: GdalError) -> Self {
        GdalRasterLoaderError::GdalError { source }
    }
}

pub struct GdalRasterLoader {}

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
        tile_info: TileInformation,
        time_interval: TimeInterval,
        read_advise: GdalReadAdvise,
        cache_hint: CacheHint,
        gdal_worker: LazyGdalWorkerInstance,
    ) -> Result<RasterTile2D<T>, GdalSourceError> {
        let file_not_found_as_no_data =
            dataset_params.file_not_found_handling == FileNotFoundHandling::NoData;

        let message = IpcChannelMessage::new_request_tile_message(IpcChannelMessagePayload {
            dataset_params,
            read_advise,
            data_type: T::TYPE,
        });

        let res = gdal_worker.read_data(message).await;

        let res = match res {
            Ok(t) => {
                // Here we need to handle edges!
                // First, convert response to GridAndProperties
                let GridAndProperties { grid, properties }: GridAndProperties<
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
                // Third, blit the grid to the tile bounds if necessary
                let grid = if read_advise.direct_read() {
                    grid
                } else {
                    let mut tile_raster =
                        GridOrEmpty::from(EmptyGrid::new(read_advise.bounds_of_target));
                    tile_raster.grid_blit_from(&grid);
                    tile_raster
                };
                Ok(GridAndProperties { grid, properties })
            }
            Err(GdalSourceError::IpcProcessError {
                source:
                    IpcProcessError::GdalError {
                        kind: GdalErrorKind::FileNotFound,
                        details: _details,
                    },
            }) if file_not_found_as_no_data => Ok(GridAndProperties {
                grid: GridOrEmpty::new_empty_shape(read_advise.bounds_of_target),
                properties: RasterProperties::default(),
            }),
            Err(other_err) => Err(other_err),
        }?;

        let tile = RasterTile2D::new_with_tile_info_and_properties(
            time_interval,
            tile_info,
            0,
            res.grid.unbounded(),
            res.properties,
            cache_hint,
        );

        Ok(tile)
    }

    ///
    /// A method to load single tiles from a GDAL dataset.
    ///
    pub fn load_tile_data_with_dataset_retry<T: Pixel + GdalType + FromPrimitive>(
        cache: &mut GdalDatasetCache,
        dataset_params: &GdalDatasetParameters,
        read_advise: GdalReadAdvise,
    ) -> Result<GdalIpcPayload<T>, GdalRasterLoaderError> {
        let is_vsi_curl = dataset_params.file_path.starts_with("/vsicurl/");
        let max_retries = dataset_params.max_retries().unwrap_or(0);
        let dp = &dataset_params;

        // Wrap both OPEN and READ actions inside the retry loop
        let result = retry_sync(
            max_retries,
            GDAL_RETRY_INITIAL_BACKOFF_MS,
            GDAL_RETRY_EXPONENTIAL_BACKOFF_FACTOR,
            Some(GDAL_RETRY_MAX_BACKOFF_MS),
            || {
                // 1. Attempt to grab or open the dataset
                let ds = match cache.get_or_open(dp) {
                    Ok(dataset) => dataset,
                    Err(gdal_error) => {
                        // If the file explicitly does not exist, do not waste time retrying
                        if error_is_gdal_file_not_found(&gdal_error) {
                            return Err(GdalRasterLoaderError::GdalFileNotFound {
                                source: gdal_error,
                            });
                        }
                        if is_vsi_curl {
                            clear_gdal_vsi_cache_for_path(dp.file_path.as_path());
                        }
                        return Err(GdalRasterLoaderError::GdalError { source: gdal_error });
                    }
                };

                // 2. Perform the actual data read inside the retry boundary
                Self::load_tile_data(ds, dataset_params, read_advise).inspect_err(|_e| {
                    if is_vsi_curl {
                        clear_gdal_vsi_cache_for_path(dp.file_path.as_path());
                    }
                })
            },
        );

        // 3. Handle results without crashing the process
        match result {
            Ok(grid_and_properties) => Ok(grid_and_properties),

            Err(other_error) => {
                // Bubble error cleanly back to the parent pool via the IPC channel
                Err(other_error)
            }
        }
    }

    fn read_advise_for_tile(
        reader_mode: GdalReaderMode,
        ds_spatial_grid: &SpatialGridDefinition,
        tile_spatial_grid: &SpatialGridDefinition,
    ) -> Option<GdalReadAdvise> {
        tracing::trace!(
            "ds_spatial_grid: {:?}, tile_spatial_grid {:?}",
            ds_spatial_grid,
            tile_spatial_grid
        );

        reader_mode.tiling_to_dataset_read_advise(ds_spatial_grid, tile_spatial_grid)
    }

    /// Load a single tile using the process manager.
    async fn load_tile_async<T: Pixel + GdalType + FromPrimitive>(
        dataset_params: Option<GdalDatasetParameters>,
        reader_mode: GdalReaderMode,
        tile_information: TileInformation,
        tile_time: TimeInterval,
        cache_hint: CacheHint,
        gdal_worker: LazyGdalWorkerInstance,
    ) -> Result<RasterTile2D<T>, GdalSourceError> {
        let tile_spatial_grid = tile_information.spatial_grid_definition();

        match dataset_params {
            // TODO: discuss if we need this check here. The metadata provider should only pass on loading infos if the query intersects the datasets bounds! And the tiling strategy should only generate tiles that intersect the querys bbox.
            Some(ds) => {
                tracing::trace!(
                    "Loading tile {:?}, from {}, band: {}",
                    &tile_information,
                    ds.file_path.display(),
                    ds.rasterband_channel
                );

                let ds_spatial_grid = ds.spatial_grid_definition();
                let gdal_read_advise =
                    Self::read_advise_for_tile(reader_mode, &ds_spatial_grid, &tile_spatial_grid);

                let Some(gdal_read_advise) = gdal_read_advise else {
                    debug!(
                        "Tile {:?} not intersecting dataset grid or gdal grid {:?}",
                        &tile_information, ds.file_path
                    );
                    return Ok(Self::create_no_data_tile(
                        tile_information,
                        tile_time,
                        cache_hint,
                    ));
                };

                Self::load_tile_data_process(
                    ds,
                    tile_information,
                    tile_time,
                    gdal_read_advise,
                    cache_hint,
                    gdal_worker,
                )
                .await
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

    ///
    /// A method to load single tiles from a GDAL dataset.
    ///
    fn load_tile_data<T: Pixel + GdalType + FromPrimitive>(
        dataset: &mut GdalDataset,
        dataset_params: &GdalDatasetParameters,
        read_advise: GdalReadAdvise,
    ) -> Result<GdalIpcPayload<T>, GdalRasterLoaderError> {
        let start = Instant::now();

        debug!(
            "GridOrEmpty2D<{:?}> requested for {:?}.",
            T::TYPE,
            &read_advise.bounds_of_target,
        );

        let gdal_dataset_geotransform = GdalDatasetGeoTransform::from(dataset.geo_transform()?);
        // check that the dataset geo transform is the same as the one we get from GDAL
        debug_assert!(
            approx_eq!(
                Coordinate2D,
                gdal_dataset_geotransform.origin_coordinate,
                dataset_params.geo_transform.origin_coordinate
            ),
            "expected dataset geo transform origin coordinate {:?} to be approximately equal to GDAL dataset geo transform origin coordinate {:?}",
            dataset_params.geo_transform.origin_coordinate,
            gdal_dataset_geotransform.origin_coordinate
        );

        debug_assert!(
            approx_eq!(
                f64,
                gdal_dataset_geotransform.x_pixel_size,
                dataset_params.geo_transform.x_pixel_size
            ),
            "expected dataset geo transform x pixel size {:?} to be approximately equal to GDAL dataset geo transform x pixel size {:?}",
            dataset_params.geo_transform.x_pixel_size,
            gdal_dataset_geotransform.x_pixel_size
        );

        debug_assert!(
            approx_eq!(
                f64,
                gdal_dataset_geotransform.y_pixel_size,
                dataset_params.geo_transform.y_pixel_size
            ),
            "expected dataset geo transform y pixel size {:?} to be approximately equal to GDAL dataset geo transform y pixel size {:?}",
            dataset_params.geo_transform.y_pixel_size,
            gdal_dataset_geotransform.y_pixel_size
        );

        let (gdal_dataset_pixels_x, gdal_dataset_pixels_y) = dataset.raster_size();
        // check that the dataset pixel size is the same as the one we get from GDAL
        debug_assert_eq!(gdal_dataset_pixels_x, dataset_params.width);
        debug_assert_eq!(gdal_dataset_pixels_y, dataset_params.height);

        let rasterband = dataset.rasterband(dataset_params.rasterband_channel)?;

        let result_gdal_raster = Self::read_grid_from_raster(
            &rasterband,
            &read_advise.gdal_read_widow,
            &read_advise.read_window_bounds,
            dataset_params,
        )?;

        let properties = Self::read_raster_properties(dataset, dataset_params, &rasterband);

        let elapsed = start.elapsed();
        debug!("data loaded -> returning data grid, took {elapsed:?}");

        Ok(GdalIpcPayload {
            dimensions: read_advise.read_window_bounds,
            properties,
            data_variant: result_gdal_raster,
        })
    }

    // This is where the source attaches!
    /// A stream of futures producing `RasterTile2D` for a single slice in time
    fn temporal_slice_tile_future_stream<T: Pixel + GdalType + FromPrimitive>(
        spatial_bounds: GridBoundingBox2D,
        info: GdalLoadingInfoTemporalSlice,
        tiling_strategy: TilingStrategy,
        reader_mode: GdalReaderMode,
        gdal_worker: LazyGdalWorkerInstance,
    ) -> impl Stream<Item = impl Future<Output = Result<RasterTile2D<T>>>> + use<T> {
        stream::iter(tiling_strategy.tile_information_iterator_from_pixel_bounds(spatial_bounds))
            .map(move |tile| {
                GdalRasterLoader::load_tile_async(
                    info.params.clone(),
                    reader_mode,
                    tile,
                    info.time,
                    info.cache_ttl.into(),
                    gdal_worker.clone(),
                )
                .map_err(Into::into)
            })
    }

    // Creates a seperate process which handles the loading info to tile stream
    pub(crate) fn loading_info_to_tile_stream<
        T: Pixel + GdalType + FromPrimitive,
        S: Stream<Item = Result<GdalLoadingInfoTemporalSlice>>,
    >(
        loading_info_stream: S,
        spatial_query: GridBoundingBox2D,
        tiling_strategy: TilingStrategy,
        gdal_worker: LazyGdalWorkerInstance,
        reader_mode: GdalReaderMode,
    ) -> impl Stream<Item = Result<RasterTile2D<T>>> + use<S, T> {
        loading_info_stream
            .map_ok(move |info| {
                GdalRasterLoader::temporal_slice_tile_future_stream(
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

    /// This method reads the data for a single grid with a specified size from the GDAL dataset.
    /// It fails if the tile is not within the dataset.
    #[allow(clippy::float_cmp)]
    pub fn read_grid_from_raster<T, D>(
        rasterband: &GdalRasterBand,
        read_window: &GdalReadWindow,
        out_shape: &D,
        dataset_params: &GdalDatasetParameters,
    ) -> Result<GdalDataVariant<T>, GdalRasterLoaderError>
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

        let dataset_mask_flags = rasterband.mask_flags()?;

        if dataset_mask_flags.is_all_valid() {
            debug!("all pixels are valid --> skip no-data and mask handling.");
            return Ok(GdalDataVariant::AllValid { data: buffer_data });
        }

        if dataset_mask_flags.is_nodata() {
            debug!("raster uses a no-data value --> use no-data handling.");
            let no_data_value = dataset_params
                .no_data_value
                .or_else(|| rasterband.no_data_value());

            if let Some(no_data_value) = no_data_value {
                return Ok(GdalDataVariant::WithNoData {
                    data: buffer_data,
                    no_data_value,
                });
            }
            return Ok(GdalDataVariant::AllValid { data: buffer_data });
        }

        if dataset_mask_flags.is_alpha() {
            debug!("raster uses alpha band to mask pixels.");
            if !dataset_params.allow_alphaband_as_mask {
                return Err(GdalRasterLoaderError::AlphaBandAsMaskNotAllowed);
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
        Ok(GdalDataVariant::WithExplicitMask {
            data: buffer_data,
            validity_mask: mask_buffer_data,
        })
    }

    /*
    /// This method reads the data for a single tile with a specified size from the GDAL dataset.
    /// It handles conversion to grid coordinates.
    /// If the tile is inside the dataset it uses the `read_grid_from_raster` method.
    /// If the tile overlaps the borders of the dataset it uses the `read_partial_grid_from_raster` method.
    pub fn read_grid_and_handle_edges<T>(
        rasterband: &GdalRasterBand,
        dataset_params: &GdalDatasetParameters,
        gdal_read_advice: GdalReadAdvise,
    ) -> Result<GdalIpcPayload<T>, GdalRasterLoaderError>
    where
        T: Pixel + GdalType + Default + FromPrimitive,
    {
        let result_grid = if gdal_read_advice.direct_read() {
            let r: GridOrEmpty<GridBoundingBox2D, T> = Self::read_grid_from_raster(
                rasterband,
                &gdal_read_advice.gdal_read_widow,
                gdal_read_advice.read_window_bounds,
                dataset_params,
                gdal_read_advice.flip_y,
            )?;
            r
        } else {
            let r: GridOrEmpty<GridBoundingBox2D, T> = Self::read_grid_from_raster(
                rasterband,
                &gdal_read_advice.gdal_read_widow,
                gdal_read_advice.read_window_bounds,
                dataset_params,
                gdal_read_advice.flip_y,
            )?;
            let mut tile_raster =
                GridOrEmpty::from(EmptyGrid::new(gdal_read_advice.bounds_of_target)); // TODO: move blit!
            tile_raster.grid_blit_from(&r);
            tile_raster
        };

        Ok(result_grid)
    }
    */

    /// This method reads the data for a single tile with a specified size from the GDAL dataset and adds the requested metadata as properties to the tile.
    pub fn read_raster_properties(
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

    fn create_no_data_tile<T: Pixel>(
        tile_info: TileInformation,
        tile_time: TimeInterval,
        cache_hint: CacheHint,
    ) -> RasterTile2D<T> {
        // TODO: add cache_hint
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

pub fn error_is_gdal_file_not_found(error: &GdalError) -> bool {
    matches!(
        error,
        GdalError::NullPointer {
                method_name,
                msg
            }
         if *method_name == "GDALOpenEx" && (*msg == "HTTP response code: 404" || msg.ends_with("No such file or directory"))
    )
}

fn clear_gdal_vsi_cache_for_path(file_path: &Path) {
    unsafe {
        if let Some(Some(c_string)) = file_path.to_str().map(|s| CString::new(s).ok()) {
            VSICurlPartialClearCache(c_string.as_ptr());
        }
    }
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
            Some(original_resolution_spatial_grid) => GdalReaderMode::OverviewLevel(
                OverviewReaderState::new(original_resolution_spatial_grid),
            ),
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
            &query,
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
                tracing::trace!("GdalSource time_query producing time interval: {:?}", ti)
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
    query: &RasterQueryRectangle,
    tiling_strategy: TilingStrategy,
    reader_mode: GdalReaderMode,
    gdal_worker: LazyGdalWorkerInstance,
) -> impl Stream<Item = Result<RasterTile2D<P>>> + use<P, S>
where
    P: Pixel + GdalType + FromPrimitive,
    S: Stream<Item = Result<GdalLoadingInfoTemporalSlice>>,
{
    GdalRasterLoader::loading_info_to_tile_stream(
        source_stream,
        query.spatial_bounds(),
        tiling_strategy,
        gdal_worker,
        reader_mode,
    )
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
impl RasterOperator for GdalSource {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let data_id = context.resolve_named_data(&self.params.data).await?;
        let meta_data: GdalMetaData = context.meta_data(&data_id).await?;

        debug!("Initializing GdalSource for {:?}.", &self.params.data);
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromSql, ToSql)]
pub struct GdalMetadataMapping {
    pub source_key: RasterPropertiesKey,
    pub target_key: RasterPropertiesKey,
    pub target_type: RasterPropertiesEntryType,
}

impl GdalMetadataMapping {
    pub fn identity(
        key: RasterPropertiesKey,
        target_type: RasterPropertiesEntryType,
    ) -> GdalMetadataMapping {
        GdalMetadataMapping {
            source_key: key.clone(),
            target_key: key,
            target_type,
        }
    }
}

pub fn properties_from_gdal_metadata<'a, I, M>(
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

pub fn properties_from_band(properties: &mut RasterProperties, gdal_dataset: &GdalRasterBand) {
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
    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use crate::source::IpcProcessRasterResult;
    use crate::source::gdal_source::GdalProcessPool;
    use crate::source::gdal_source::process::GdalIpcPayload;
    use crate::test_data;
    use crate::util::gdal::{add_ndvi_dataset, gdal_open_ex_gdal_error};
    use crate::util::{Result, TemporaryGdalThreadLocalConfigOptions};
    use float_cmp::assert_approx_eq;
    use gdal::{DatasetOptions, GdalOpenFlags};
    use geoengine_datatypes::hashmap;
    use geoengine_datatypes::primitives::{AxisAlignedRectangle, SpatialPartition2D, TimeInstance};
    use geoengine_datatypes::raster::GridShape2D;

    use geoengine_datatypes::raster::{BoundedGrid, SpatialGridDefinition};
    use geoengine_datatypes::raster::{EmptyGrid2D, TilesEqualIgnoringCacheHint};

    use geoengine_datatypes::raster::{GridBounds, GridIdx2D};
    use geoengine_datatypes::raster::{TileInformation, TilingStrategy};
    use geoengine_datatypes::util::gdal::hide_gdal_errors;
    use httptest::matchers::request;
    use httptest::{Expectation, Server, responders};
    use reader::{GdalReadAdvise, GdalReadWindow};

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

    fn get_params() -> GdalDatasetParameters {
        GdalDatasetParameters {
            file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF").into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (-180., 90.).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 3600,
            height: 1800,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(0.),
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
        }
    }

    #[test]
    fn test_sending_gdal_dataset_parameters_via_string() {
        let msg = get_params();

        let (sender, receiver) = ipc_channel::ipc::channel::<String>().unwrap();

        sender.send(serde_json::to_string(&msg).unwrap()).unwrap();
        let recv = receiver.recv().unwrap();
        let recv = serde_json::from_str::<GdalDatasetParameters>(&recv).unwrap();
        assert_eq!(msg.properties_mapping, recv.properties_mapping);
        assert_eq!(msg, recv);
    }

    #[test]
    fn test_sending_gdal_dataset_parameters() {
        let msg = get_params();

        let (sender, receiver) = ipc_channel::ipc::channel().unwrap();

        sender.send(msg.clone()).unwrap();
        let recv = receiver.recv().unwrap();
        assert_eq!(msg.properties_mapping, recv.properties_mapping);

        assert_eq!(msg, recv);
    }

    #[test]
    fn test_sending_tile_information() {
        let output_shape: GridShape2D = [8, 8].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());

        let msg = tile_information_with_partition_and_shape(output_bounds, output_shape);

        let (sender, receiver) = ipc_channel::ipc::channel().unwrap();

        sender.send(msg).unwrap();
        let recv = receiver.recv().unwrap();
        assert_eq!(msg, recv);
    }

    #[test]
    fn test_sending_time() {
        let msg = TimeInstance::from_millis(10).unwrap();

        let (sender, receiver) = ipc_channel::ipc::channel().unwrap();

        sender.send(msg.clone()).unwrap();
        let recv = receiver.recv().unwrap();
        assert_eq!(msg, recv);
    }

    #[test]
    fn test_sending_time_interval() {
        let msg = TimeInterval::default();

        let (sender, receiver) = ipc_channel::ipc::channel().unwrap();

        sender.send(msg.clone()).unwrap();
        let recv = receiver.recv().unwrap();
        assert_eq!(msg, recv);
    }

    #[test]
    fn test_sending_request_tile_data() {
        use process::{IpcChannelMessage, IpcChannelMessagePayload};

        let output_shape: GridShape2D = [8, 8].into();

        let read_advise = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), output_shape),
            read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            flip_y: false,
        };

        let payload = IpcChannelMessagePayload {
            data_type: RasterDataType::U8,
            dataset_params: GdalDatasetParameters {
                file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF").into(),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: (-180., 90.).into(),
                    x_pixel_size: 0.1,
                    y_pixel_size: -0.1,
                },
                width: 3600,
                height: 1800,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: Some(0.),
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
            },
            read_advise,
        };

        let msg = IpcChannelMessage::new_request_tile_message(payload);

        let (sender, receiver) = ipc_channel::ipc::channel().unwrap();

        sender.send(msg.clone()).unwrap();
        let recv = receiver.recv().unwrap();

        assert_eq!(msg, recv);
    }

    #[test]
    fn test_ipc_channel_roundtrip_tile() {
        use process::{IpcChannelMessage, IpcChannelMessagePayload, spawn_ipc_server_process};

        let output_shape: GridShape2D = [8, 8].into();

        let read_advise = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), output_shape),
            read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            flip_y: false,
        };

        let payload = IpcChannelMessagePayload {
            data_type: RasterDataType::U8,
            dataset_params: GdalDatasetParameters {
                file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF").into(),
                rasterband_channel: 1,
                geo_transform: GdalDatasetGeoTransform {
                    origin_coordinate: (-180., 90.).into(),
                    x_pixel_size: 0.1,
                    y_pixel_size: -0.1,
                },
                width: 3600,
                height: 1800,
                file_not_found_handling: FileNotFoundHandling::NoData,
                no_data_value: Some(0.),
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
            },
            read_advise,
        };

        let msg = IpcChannelMessage::new_request_tile_message(payload);

        let (_child_guard, sender, receiver) =
            spawn_ipc_server_process::<IpcChannelMessage, IpcProcessRasterResult>().unwrap();

        sender.send(msg).unwrap();
        let rx_result = receiver
            .recv()
            .inspect_err(|e| panic!("IPC receive: {:?}", e))
            .unwrap();

        let payload = match rx_result {
            Ok(r) => r,
            Err(e) => panic!("Error receiving from IPC process: {:?}", e),
        };

        let result_2: GdalIpcPayload<u8> = payload.into();

        let grid_and_props: GridAndProperties<u8, GridBoundingBox2D> = result_2.into();

        assert!(!grid_and_props.grid.is_empty());

        let grid = grid_and_props.grid.into_materialized_masked_grid();

        assert_eq!(grid.inner_grid.data.len(), 64);
        assert_eq!(grid.validity_mask.data.len(), 64);
    }

    // TODO (low): name / test
    async fn load_ndvi_jan_2014_by_process(
        gdal_read_advice: GdalReadAdvise,
        tile_information: TileInformation,
        gdal_worker: LazyGdalWorkerInstance,
    ) -> Result<RasterTile2D<u8>> {
        let dataset_params = GdalDatasetParameters {
            file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF").into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (-180., 90.).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 3600,
            height: 1800,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(0.),
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

        GdalRasterLoader::load_tile_data_process::<u8>(
            dataset_params,
            tile_information,
            TimeInterval::default(),
            gdal_read_advice,
            CacheHint::default(),
            gdal_worker,
        )
        .await
        .map_err(Into::into)
    }

    // This method loads raster data from a cropped MODIS NDVI raster.
    // To inspect the byte values first convert the file to XYZ with GDAL:
    // 'gdal_translate -of xyz MOD13A2_M_NDVI_2014-04-01_30X30.tif MOD13A2_M_NDVI_2014-04-01_30x30.xyz'
    // Then you can convert them to gruped bytes:
    // 'cut -d ' ' -f 1,2 --complement MOD13A2_M_NDVI_2014-04-01_30x30.xyz | xargs -n 30 > MOD13A2_M_NDVI_2014-04-01_30x30_bytes.txt'.
    fn load_ndvi_apr_2014_cropped(
        gdal_read_advice: GdalReadAdvise,
    ) -> Result<GridAndProperties<u8>> {
        let dataset_params = GdalDatasetParameters {
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
            properties_mapping: Some(vec![GdalMetadataMapping {
                source_key: RasterPropertiesKey {
                    domain: None,
                    key: "AREA_OR_POINT".to_string(),
                },
                target_type: RasterPropertiesEntryType::String,
                target_key: RasterPropertiesKey {
                    domain: None,
                    key: "AREA_OR_POINT".to_string(),
                },
            }]),
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        };

        let mut gdc = GdalDatasetCache::new();
        let dataset = gdc.get_or_open(&dataset_params).unwrap();

        let reader_payload =
            GdalRasterLoader::load_tile_data::<u8>(dataset, &dataset_params, gdal_read_advice)
                .map_err(IpcProcessError::from)
                .map_err(|e| GdalSourceError::IpcProcessError { source: e })?;

        let grid_and_props: GridAndProperties<u8, GridBoundingBox2D> = reader_payload.into();
        Ok(grid_and_props.into())
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
    async fn test_load_tile_data_process() {
        let output_shape: GridShape2D = [8, 8].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (-179.2, 89.2).into());

        let gdal_read_advice = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), output_shape),
            read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            flip_y: false,
        };

        let tile_information =
            tile_information_with_partition_and_shape(output_bounds, output_shape);

        let gpp = GdalProcessPool::new(4, 2);
        let gw = gpp.get_gdal_worker();

        let RasterTile2D {
            global_geo_transform: _,
            grid_array: grid,
            tile_position: _,
            band: _,
            time: _,
            properties,
            cache_hint: _,
        } = load_ndvi_jan_2014_by_process(gdal_read_advice, tile_information, gw)
            .await
            .unwrap();

        assert!(!grid.is_empty());

        let grid = grid.into_materialized_masked_grid();

        assert_eq!(grid.inner_grid.data.len(), 64);
        assert_eq!(
            grid.inner_grid.data,
            &[
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255
            ]
        );

        assert_eq!(grid.validity_mask.data.len(), 64);
        assert_eq!(grid.validity_mask.data, &[true; 64]);

        assert!((properties.scale_option()).is_none());
        assert!(properties.offset_option().is_none());
        assert_eq!(
            properties.get_property(&RasterPropertiesKey {
                domain: None,
                key: "AREA_OR_POINT".to_string(),
            }),
            Some(&RasterPropertiesEntry::String("Area".to_string()))
        );
        assert_eq!(
            properties.get_property(&RasterPropertiesKey {
                domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                key: "COMPRESSION".to_string(),
            }),
            Some(&RasterPropertiesEntry::String("LZW".to_string()))
        );
    }

    #[tokio::test]
    async fn test_load_tile_data_top_left() {
        let gdal_read_advice = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), [8, 8].into()),
            read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            flip_y: false,
        };

        let GridAndProperties { grid, properties } =
            load_ndvi_apr_2014_cropped(gdal_read_advice).unwrap();

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

        assert_eq!(
            properties.get_property(&RasterPropertiesKey {
                key: "AREA_OR_POINT".to_string(),
                domain: None,
            }),
            Some(&RasterPropertiesEntry::String("Area".to_string()))
        );
    }

    #[test]
    fn test_load_tile_data_overlaps_dataset_bounds_top_left_out1() {
        // shift world bbox one pixel up and to the left
        let gdal_read_advice = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), [7, 7].into()), // this is the data we can read
            read_window_bounds: GridBoundingBox2D::new([1, 1], [7, 7]).unwrap(), // this is the area we can fill in target
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(), // this is the area of the target
            flip_y: false,
        };

        let GridAndProperties {
            grid,
            properties: _properties,
        } = load_ndvi_apr_2014_cropped(gdal_read_advice).unwrap();

        assert!(!grid.is_empty());

        let x = grid.into_materialized_masked_grid();

        assert_eq!(x.inner_grid.data.len(), 49);
        assert_eq!(
            x.inner_grid.data,
            &[
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 127, 255, 255, 255, 255, 255, 164, 185, 255, 255, 255, 175,
                186, 190, 167, 255, 255, 161, 175, 184, 173, 170, 255, 255, 128, 177, 165, 145,
                191,
            ]
        );

        assert_eq!(x.validity_mask.data.len(), 49);
        // pixel mask is pixel == 255 from the top left 8x8 block from MOD13A2_M_NDVI_2014-04-01_27x27_bytes.txt
        assert_eq!(
            x.validity_mask.data,
            &[
                false, false, false, false, false, false, false, false, false, false, false, false,
                false, false, false, false, false, false, false, false, true, false, false, false,
                false, false, true, true, false, false, false, true, true, true, true, false,
                false, true, true, true, true, true, false, false, true, true, true, true, true,
            ]
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

        let gpp = GdalProcessPool::new(4, 2);
        let gw: LazyGdalWorkerInstance = gpp.get_gdal_worker();

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

    #[test]
    #[allow(clippy::too_many_lines)]
    fn read_up_side_down_raster() {
        let up_side_down_params = GdalDatasetParameters {
            file_path: test_data!(
                "raster/modis_ndvi/flipped_axis_y/MOD13A2_M_NDVI_2014-01-01_flipped_y.tiff"
            )
            .into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (-180., -90.).into(),
                x_pixel_size: 0.1,
                y_pixel_size: 0.1,
            },
            width: 3600,
            height: 1800,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(0.),
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

        let ge_global_dataset_grid = SpatialGridDefinition::new(
            GeoTransform::new(Coordinate2D::new(-180., 90.), 0.1, -0.1),
            GridBoundingBox2D::new_min_max(0, 1799, 0, 3599).unwrap(),
        );

        let gdal_dataset_grid = ge_global_dataset_grid.flip_axis_y(); // first, flip axis
        assert_approx_eq!(
            Coordinate2D,
            gdal_dataset_grid.geo_transform().origin_coordinate,
            Coordinate2D::new(-180., 90.)
        );
        assert_approx_eq!(f64, gdal_dataset_grid.geo_transform.y_pixel_size(), 0.1);
        assert_approx_eq!(f64, gdal_dataset_grid.geo_transform.x_pixel_size(), 0.1);
        assert_eq!(
            gdal_dataset_grid.grid_bounds,
            GridBoundingBox2D::new_min_max(-1800, -1, 0, 3599).unwrap()
        );

        let gdal_dataset_grid = gdal_dataset_grid
            .with_moved_origin_exact_grid(Coordinate2D::new(-180., -90.))
            .unwrap(); // second, move origin (to other side of axis)
        assert_approx_eq!(
            Coordinate2D,
            gdal_dataset_grid.geo_transform().origin_coordinate,
            Coordinate2D::new(-180., -90.)
        );
        assert_approx_eq!(f64, gdal_dataset_grid.geo_transform.y_pixel_size(), 0.1);
        assert_approx_eq!(f64, gdal_dataset_grid.geo_transform.x_pixel_size(), 0.1);
        assert_eq!(
            gdal_dataset_grid.grid_bounds,
            GridBoundingBox2D::new_min_max(0, 1799, 0, 3599).unwrap()
        );

        let ovr = OverviewReaderState::new(ge_global_dataset_grid);

        let tile = SpatialGridDefinition::new(
            ge_global_dataset_grid.geo_transform,
            GridBoundingBox2D::new_min_max(326, 326 + 7, 1880, 1880 + 7).unwrap(),
        );

        let gdal_read_advice = ovr
            .tiling_to_dataset_read_advise(&gdal_dataset_grid, &tile)
            .unwrap();

        let exp_gdal_read_advice = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([1466, 1880].into(), [8, 8].into()),
            read_window_bounds: GridBoundingBox2D::new([326, 1880], [326 + 7, 1880 + 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([326, 1880], [326 + 7, 1880 + 7]).unwrap(),
            flip_y: true,
        };

        assert_eq!(gdal_read_advice, exp_gdal_read_advice);

        let mut gdc = GdalDatasetCache::new();
        let dataset = gdc.get_or_open(&up_side_down_params).unwrap();

        let reader_payload =
            GdalRasterLoader::load_tile_data::<u8>(dataset, &up_side_down_params, gdal_read_advice)
                .unwrap();

        let GridAndProperties { grid, properties } = reader_payload.into();
        assert!(!grid.is_empty());
        let grid = grid.into_materialized_masked_grid();

        assert_eq!(grid.inner_grid.data.len(), 64);

        assert_eq!(
            grid.inner_grid.data,
            &[
                // this is not yet flipped!
                255, 47, 42, 82, 81, 76, 73, 98, 255, 255, 59, 95, 85, 66, 105, 104, 255, 255, 91,
                97, 100, 86, 78, 106, 255, 255, 255, 97, 102, 91, 73, 72, 255, 255, 255, 255, 255,
                68, 81, 93, 255, 255, 255, 255, 255, 255, 53, 47, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            ]
        );

        assert_eq!(grid.validity_mask.data.len(), 64);
        assert_eq!(grid.validity_mask.data, &[true; 64]);

        assert!(properties.offset_option().is_none());
        assert!(properties.scale_option().is_none());
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

        let gdal_read_advice = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), [8, 8].into()),
            read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            flip_y: false,
        };

        let mut gdc = GdalDatasetCache::new();
        let dataset = gdc.get_or_open(&up_side_down_params).unwrap();

        let GridAndProperties { grid, properties } =
            GdalRasterLoader::load_tile_data::<u8>(dataset, &up_side_down_params, gdal_read_advice)
                .unwrap()
                .into();

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
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (-180., 90.).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 3600,
            height: 1800,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: None,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        };

        let gdal_read_advice = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), [8, 8].into()),
            read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            flip_y: false,
        };

        let mut gdc = GdalDatasetCache::new();

        let result = GdalRasterLoader::load_tile_data_with_dataset_retry::<u8>(
            &mut gdc,
            &ds,
            gdal_read_advice,
        );

        // file not found => specific error
        match result {
            Err(GdalRasterLoaderError::GdalFileNotFound { .. }) => {}
            _ => panic!("expected FileNotFound error"),
        }

        let ds = GdalDatasetParameters {
            file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF").into(),
            rasterband_channel: 100, // invalid channel
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (-180., 90.).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 3600,
            height: 1800,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: None,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        };

        // invalid channel => error
        let result = GdalRasterLoader::load_tile_data_with_dataset_retry::<u8>(
            &mut gdc,
            &ds,
            gdal_read_advice,
        );
        match result {
            Err(GdalRasterLoaderError::GdalError { .. }) => {}
            _ => panic!("expected GdalError error"),
        }
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
        let gdal_read_advice = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), [8, 8].into()),
            read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            flip_y: false,
        };

        let mut gdc = GdalDatasetCache::new();

        let result = GdalRasterLoader::load_tile_data_with_dataset_retry::<u8>(
            &mut gdc,
            &ds,
            gdal_read_advice,
        );

        // file not found => specific error
        match result {
            Err(GdalRasterLoaderError::GdalFileNotFound { .. }) => {}
            _ => panic!("expected FileNotFound error"),
        }

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
        let result = GdalRasterLoader::load_tile_data_with_dataset_retry::<u8>(
            &mut gdc,
            &ds,
            gdal_read_advice,
        );
        match result {
            Err(GdalRasterLoaderError::GdalError { .. }) => {}
            _ => panic!("expected GdalError error"),
        }
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
        let result = gdal_open_ex_gdal_error(
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
        let result = gdal_open_ex_gdal_error(
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
        let result = gdal_open_ex_gdal_error(
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

    #[tokio::test]
    async fn it_attaches_cache_hint() {
        let output_bounds =
            SpatialPartition2D::new_unchecked((-90., 90.).into(), (90., -90.).into());
        let output_shape: GridShape2D = [256, 256].into();

        let tile_info = tile_information_with_partition_and_shape(output_bounds, output_shape);
        let time_interval = TimeInterval::new_unchecked(1_388_534_400_000, 1_391_212_800_000); // 2014-01-01 - 2014-01-15
        let params = None;

        let gpp = GdalProcessPool::new(4, 2);
        let gw: LazyGdalWorkerInstance = gpp.get_gdal_worker();

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
}
