use self::reader::{GdalReadAdvise, GdalReadWindow, GdalReaderMode, GridAndProperties};
use crate::adapters::{FillerTileCacheExpirationStrategy, SparseTilesFillAdapter};
use crate::engine::{
    CanonicOperatorName, MetaData, OperatorData, OperatorName, QueryProcessor, WorkflowOperatorPath,
};
use crate::source::gdal_source::reader::ReaderState;
use crate::util::gdal::gdal_open_dataset_ex;
use crate::util::input::float_option_with_nan;
use crate::util::retry::retry;
use crate::util::TemporaryGdalThreadLocalConfigOptions;
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
use float_cmp::{approx_eq, ApproxEq};
use futures::{
    stream::{self, BoxStream, StreamExt},
    Stream,
};
use futures::{Future, TryStreamExt};
use gdal::errors::GdalError;
use gdal::raster::{GdalType, RasterBand as GdalRasterBand};
use gdal::{Dataset as GdalDataset, DatasetOptions, GdalOpenFlags, Metadata as GdalMetadata};
use gdal_sys::VSICurlPartialClearCache;
use geoengine_datatypes::dataset::NamedData;
use geoengine_datatypes::primitives::{BandSelection, CacheHint};
use geoengine_datatypes::primitives::{
    Coordinate2D, DateTimeParseFormat, RasterQueryRectangle, RasterSpatialQueryRectangle,
};
use geoengine_datatypes::raster::TileInformation;
use geoengine_datatypes::raster::{
    ChangeGridBounds, EmptyGrid, GeoTransform, GridOrEmpty, GridOrEmpty2D, GridShapeAccess,
    MapElements, MaskedGrid, NoDataValueGrid, Pixel, RasterDataType, RasterProperties,
    RasterPropertiesEntry, RasterPropertiesEntryType, RasterPropertiesKey, RasterTile2D,
    TilingStrategy,
};
use geoengine_datatypes::raster::{GridBounds, GridIntersection};
use geoengine_datatypes::util::test::TestDefault;
use geoengine_datatypes::{
    primitives::TimeInterval,
    raster::{Grid, GridBlit, GridBoundingBox2D, GridSize, TilingSpecification},
};
pub use loading_info::{
    GdalLoadingInfo, GdalLoadingInfoTemporalSlice, GdalLoadingInfoTemporalSliceIterator,
    GdalMetaDataList, GdalMetaDataRegular, GdalMetaDataStatic, GdalMetadataNetCdfCf,
};
use log::debug;
use num::{integer::div_ceil, integer::div_floor, FromPrimitive};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::ffi::CString;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::time::Instant;
mod db_types;
mod error;
mod loading_info;
mod reader;

static GDAL_RETRY_INITIAL_BACKOFF_MS: u64 = 1000;
static GDAL_RETRY_MAX_BACKOFF_MS: u64 = 60 * 60 * 1000;
static GDAL_RETRY_EXPONENTIAL_BACKOFF_FACTOR: f64 = 2.;

/// Parameters for the GDAL Source Operator
///
/// # Examples
///
/// ```rust
/// use serde_json::{Result, Value};
/// use geoengine_operators::source::{GdalSource, GdalSourceParameters};
/// use geoengine_datatypes::dataset::{NamedData};
/// use geoengine_datatypes::util::Identifier;
///
/// let json_string = r#"
///     {
///         "type": "GdalSource",
///         "params": {
///             "data": "ns:dataset"
///         }
///     }"#;
///
/// let operator: GdalSource = serde_json::from_str(json_string).unwrap();
///
/// assert_eq!(operator, GdalSource {
///     params: GdalSourceParameters {
///         data: NamedData::with_namespaced_name("ns", "dataset"),
///     },
/// });
/// ```
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GdalSourceParameters {
    pub data: NamedData,
    #[serde(default)]
    pub overview_level: Option<u32>, // TODO: should also allow a resolution? Add resample method?
}

impl GdalSourceParameters {
    pub fn new(data: NamedData) -> Self {
        Self {
            data,
            overview_level: None,
        }
    }

    pub fn new_with_overview_level(data: NamedData, overview_level: u32) -> Self {
        Self {
            data,
            overview_level: Some(overview_level),
        }
    }

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
            dataset_geo_transform.x_pixel_size > 0.0 && dataset_geo_transform.y_pixel_size < 0.0,
            crate::error::GeoTransformOrigin
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
    pub result_descriptor: RasterResultDescriptor,
    pub tiling_specification: TilingSpecification,
    pub meta_data: GdalMetaData,
    pub overview_level: u32,
    pub _phantom_data: PhantomData<T>,
}

struct GdalRasterLoader {}

impl GdalRasterLoader {
    ///
    /// A method to async load single tiles from a GDAL dataset.
    ///
    async fn load_tile_data_async<T: Pixel + GdalType + FromPrimitive>(
        dataset_params: GdalDatasetParameters,
        read_advise: GdalReadAdvise,
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
                    let load_tile_result =
                        crate::util::spawn_blocking(move || Self::load_tile_data(&ds, read_advise))
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

    async fn load_tile_async<T: Pixel + GdalType + FromPrimitive>(
        dataset_params: Option<GdalDatasetParameters>,
        reader_mode: GdalReaderMode,
        tile_information: TileInformation,
        tile_time: TimeInterval,
        cache_hint: CacheHint,
    ) -> Result<RasterTile2D<T>> {
        match dataset_params {
            // TODO: discuss if we need this check here. The metadata provider should only pass on loading infos if the query intersects the datasets bounds! And the tiling strategy should only generate tiles that intersect the querys bbox.
            Some(ds)
                if reader_mode
                    .dataset_intersects_bounds(&tile_information.global_pixel_bounds()) =>
            {
                debug!(
                    "Loading tile {:?}, from {:?}, band: {}",
                    &tile_information, ds.file_path, ds.rasterband_channel
                );
                // TODO: maybe move this further up the call stack
                let gdal_read_advise = reader_mode
                    .tiling_to_dataset_read_advise(tile_information.global_pixel_bounds())
                    .expect("intersection was checked before");

                let grid = Self::load_tile_data_async(ds, gdal_read_advise).await?;

                match grid {
                    Some(grid) => Ok(RasterTile2D::new_with_properties(
                        tile_time,
                        tile_information.global_tile_position,
                        0,
                        tile_information.global_geo_transform,
                        grid.grid,
                        grid.properties,
                        cache_hint,
                    )),
                    None => Ok(create_no_data_tile(tile_information, tile_time, cache_hint)),
                }
            }
            Some(_) => {
                debug!("Skipping tile not in query rect {:?}", &tile_information);

                Ok(create_no_data_tile(tile_information, tile_time, cache_hint))
            }

            _ => {
                debug!(
                    "Skipping tile without GdalDatasetParameters {:?}",
                    &tile_information
                );

                Ok(create_no_data_tile(tile_information, tile_time, cache_hint))
            }
        }
    }

    ///
    /// A method to load single tiles from a GDAL dataset.
    ///
    fn load_tile_data<T: Pixel + GdalType + FromPrimitive>(
        dataset_params: &GdalDatasetParameters,
        read_advise: GdalReadAdvise,
    ) -> Result<Option<GridAndProperties<T>>> {
        let start = Instant::now();

        debug!(
            "GridOrEmpty2D<{:?}> requested for {:?}.",
            T::TYPE,
            &read_advise.bounds_of_target,
        );

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

        if let Err(error) = &dataset_result {
            let is_file_not_found = error_is_gdal_file_not_found(error);

            let err_result = match dataset_params.file_not_found_handling {
                FileNotFoundHandling::NoData if is_file_not_found => Ok(None),
                _ => Err(crate::error::Error::CouldNotOpenGdalDataset {
                    file_path: dataset_params.file_path.to_string_lossy().to_string(),
                }),
            };
            let elapsed = start.elapsed();
            debug!(
                "error opening dataset: {:?} -> returning error = {}, took: {:?}, file: {:?}",
                error,
                err_result.is_err(),
                elapsed,
                dataset_params.file_path
            );
            return err_result;
        };

        let dataset = dataset_result.expect("checked");

        let rasterband = dataset.rasterband(dataset_params.rasterband_channel as isize)?;

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

        let result_grid =
            read_grid_and_handle_edges(&dataset, &rasterband, dataset_params, read_advise)?;

        let properties = read_raster_properties::<T>(&dataset, dataset_params, &rasterband);

        let elapsed = start.elapsed();
        debug!("data loaded -> returning data grid, took {:?}", elapsed);

        Ok(Some(GridAndProperties {
            grid: result_grid,
            properties,
        }))
    }

    ///
    /// A stream of futures producing `RasterTile2D` for a single slice in time
    ///
    fn temporal_slice_tile_future_stream<T: Pixel + GdalType + FromPrimitive>(
        query: &RasterQueryRectangle,
        info: GdalLoadingInfoTemporalSlice,
        tiling_strategy: TilingStrategy,
        reader_mode: GdalReaderMode,
    ) -> impl Stream<Item = impl Future<Output = Result<RasterTile2D<T>>>> {
        stream::iter(
            tiling_strategy
                .tile_information_iterator_from_grid_bounds(query.spatial_query().grid_bounds())
                .map(move |tile| {
                    GdalRasterLoader::load_tile_async(
                        info.params.clone(),
                        reader_mode,
                        tile,
                        info.time,
                        info.cache_ttl.into(),
                    )
                }),
        )
    }

    fn loading_info_to_tile_stream<
        T: Pixel + GdalType + FromPrimitive,
        S: Stream<Item = Result<GdalLoadingInfoTemporalSlice>>,
    >(
        loading_info_stream: S,
        query: RasterQueryRectangle,
        tiling_strategy: TilingStrategy,
        reader_mode: GdalReaderMode,
    ) -> impl Stream<Item = Result<RasterTile2D<T>>> {
        loading_info_stream
            .map_ok(move |info| {
                GdalRasterLoader::temporal_slice_tile_future_stream(
                    &query,
                    info,
                    tiling_strategy,
                    reader_mode,
                )
                .map(Result::Ok)
            })
            .try_flatten()
            .try_buffered(16) // TODO: make this configurable
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
    type SpatialQuery = RasterSpatialQueryRectangle;
    type Selection = BandSelection;
    type ResultDescription = RasterResultDescriptor;

    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        _ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<BoxStream<Result<Self::Output>>> {
        ensure!(
            query.attributes.as_slice() == [0],
            crate::error::GdalSourceDoesNotSupportQueryingOtherBandsThanTheFirstOneYet
        );

        tracing::debug!(
            "Querying GdalSourceProcessor<{:?}> with: {:?}.",
            P::TYPE,
            &query
        );

        // this is the result descriptor of the operator. It already incorporates the overview level AND shifts the origin to the tiling origin
        let result_descriptor = self.result_descriptor();

        // A `GeoTransform` maps pixel space to world space.
        // Usually a SRS has axis directions pointing "up" (y-axis) and "up" (y-axis).
        // We are not aware of spatial reference systems where the x-axis points to the right.
        // However, there are spatial reference systems where the y-axis points downwards.
        // The standard "pixel-space" starts at the top-left corner of a `GeoTransform` and points down-right.
        // Therefore, the pixel size on the x-axis is always increasing
        let pixel_size_x = result_descriptor.geo_transform_x.x_pixel_size();
        debug_assert!(pixel_size_x.is_sign_positive());
        // and the y-axis should only be positive if the y-axis of the spatial reference system also "points down".
        // NOTE: at the moment we do not allow "down pointing" y-axis.
        let pixel_size_y = result_descriptor.geo_transform_x.y_pixel_size();
        debug_assert!(pixel_size_y.is_sign_negative());

        // The data origin is not neccessarily the origin of the tileing we want to use.
        // TODO: maybe derive tilling origin reference from the data projection
        let tiling_geo_transform = result_descriptor.tiling_geo_transform();
        let tiling_based_pixel_bounds = result_descriptor.tiling_pixel_bounds();

        // TODO: Not really sure if we want to support the case where the query is not based on tiling origin...
        let tiling_strategy = TilingStrategy::new(
            self.tiling_specification.tile_size_in_pixels,
            tiling_geo_transform,
        );

        let query_pixel_bounds = query.spatial_query().grid_bounds();

        let mut empty = false;

        if !tiling_based_pixel_bounds.intersects(&query_pixel_bounds) {
            debug!("query does not intersect spatial data bounds");
            empty = true;
        }

        // TODO: use the time bounds to early return.
        /*
        if let Some(data_time_bounds) = result_descriptor.time {
            if !data_time_bounds.intersects(&query.time_interval) {
                debug!("query does not intersect temporal data bounds");
                empty = true;
            }
        }
        */

        // Here we reverse the tiling geo transform to get the dataset geo transform.
        // FIXME: we need both, the tiling and the original geo transform! However, there is a logical gap in the logic. We want the RasterResultDescriptor to provide the tiling ge transform since this is the one we are querying. However, we need the original geo transform to load the data. If we store the original geo transform in the stored result descrptor this will propaply cause someone to use it directly.
        let dataset_geo_tansform = result_descriptor
            .tiling_geo_transform()
            .shift_by_pixel_offset(tiling_based_pixel_bounds.min_index());

        let reader_mode = if self.overview_level == 0 {
            GdalReaderMode::OriginalResolution(ReaderState {
                dataset_shape: result_descriptor.pixel_bounds_x.grid_shape(),
                dataset_geo_transform: dataset_geo_tansform,
            })
        } else {
            unimplemented!("GdalReaderMode::OverviewResolution");
        };

        let loading_iter = if empty {
            GdalLoadingInfoTemporalSliceIterator::Static {
                parts: vec![].into_iter(),
            }
        } else {
            let loading_info = self.meta_data.loading_info(query.clone()).await?;
            loading_info.info
        };

        let source_stream = stream::iter(loading_iter);

        let source_stream = GdalRasterLoader::loading_info_to_tile_stream(
            source_stream,
            query.clone(),
            tiling_strategy,
            reader_mode,
        );

        // use SparseTilesFillAdapter to fill all the gaps
        let filled_stream = SparseTilesFillAdapter::new(
            source_stream,
            tiling_strategy.global_pixel_grid_bounds_to_tile_grid_bounds(query_pixel_bounds),
            query.attributes.count(),
            tiling_strategy.geo_transform,
            tiling_strategy.tile_size_in_pixels,
            FillerTileCacheExpirationStrategy::DerivedFromSurroundingTiles,
        );
        Ok(filled_stream.boxed())
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

pub type GdalSource = SourceOperator<GdalSourceParameters>;

impl OperatorName for GdalSource {
    const TYPE_NAME: &'static str = "GdalSource";
}

fn overview_result_descriptor(
    // FIXME: other mechanism for original geo transform
    result_descriptor: RasterResultDescriptor,
    overview_level: u32,
) -> RasterResultDescriptor {
    if overview_level > 0 {
        debug!("Using overview level {}", overview_level);
        RasterResultDescriptor {
            geo_transform_x: GeoTransform::new(
                result_descriptor.geo_transform_x.origin_coordinate,
                result_descriptor.geo_transform_x.x_pixel_size() * overview_level as f64,
                result_descriptor.geo_transform_x.y_pixel_size() * overview_level as f64,
            ),
            pixel_bounds_x: GridBoundingBox2D::new_min_max(
                div_floor(
                    result_descriptor.pixel_bounds_x.y_min(),
                    overview_level as isize,
                ),
                div_ceil(
                    result_descriptor.pixel_bounds_x.y_max(),
                    overview_level as isize,
                ),
                div_floor(
                    result_descriptor.pixel_bounds_x.x_min(),
                    overview_level as isize,
                ),
                div_ceil(
                    result_descriptor.pixel_bounds_x.x_max(),
                    overview_level as isize,
                ),
            )
            .expect("overview level must be a positive integer"),
            ..result_descriptor
        }
    } else {
        debug!("Using original resolution (ov = 0)");
        result_descriptor
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

        // generate a result descriptor with the overview level
        let res = overview_result_descriptor(
            meta_data_result_descriptor,
            self.params.overview_level.unwrap_or(0),
        );

        let op = InitializedGdalSourceOperator {
            name: CanonicOperatorName::from(&self),
            result_descriptor: res,
            meta_data,
            overview_level: self.params.overview_level.unwrap_or(0),
            tiling_specification: context.tiling_specification(),
        };

        Ok(op.boxed())
    }

    span_fn!(GdalSource);
}

pub struct InitializedGdalSourceOperator {
    name: CanonicOperatorName,
    pub meta_data: GdalMetaData,
    pub result_descriptor: RasterResultDescriptor,
    pub tiling_specification: TilingSpecification,
    // the overview level to use. 0 means the highest resolution
    pub overview_level: u32,
}

impl InitializedRasterOperator for InitializedGdalSourceOperator {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        Ok(match self.result_descriptor().data_type {
            RasterDataType::U8 => TypedRasterQueryProcessor::U8(
                GdalSourceProcessor {
                    result_descriptor: self.result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    overview_level: self.overview_level,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::U16 => TypedRasterQueryProcessor::U16(
                GdalSourceProcessor {
                    result_descriptor: self.result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    overview_level: self.overview_level,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::U32 => TypedRasterQueryProcessor::U32(
                GdalSourceProcessor {
                    result_descriptor: self.result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    overview_level: self.overview_level,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::U64 => {
                return Err(GdalSourceError::UnsupportedRasterType {
                    raster_type: RasterDataType::U64,
                })?
            }
            RasterDataType::I8 => {
                return Err(GdalSourceError::UnsupportedRasterType {
                    raster_type: RasterDataType::I8,
                })?
            }
            RasterDataType::I16 => TypedRasterQueryProcessor::I16(
                GdalSourceProcessor {
                    result_descriptor: self.result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    overview_level: self.overview_level,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::I32 => TypedRasterQueryProcessor::I32(
                GdalSourceProcessor {
                    result_descriptor: self.result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    overview_level: self.overview_level,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::I64 => {
                return Err(GdalSourceError::UnsupportedRasterType {
                    raster_type: RasterDataType::I64,
                })?
            }
            RasterDataType::F32 => TypedRasterQueryProcessor::F32(
                GdalSourceProcessor {
                    result_descriptor: self.result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    overview_level: self.overview_level,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::F64 => TypedRasterQueryProcessor::F64(
                GdalSourceProcessor {
                    result_descriptor: self.result_descriptor.clone(),
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    overview_level: self.overview_level,
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
        })
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
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
    let data_grid = Grid::new(out_shape.clone(), buffer.data)?;

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
    let mask_grid = Grid::new(out_shape, mask_buffer.data)?.map_elements(|p: u8| p > 0);

    let mask_grid = if flip_y_axis {
        mask_grid.reversed_y_axis_grid()
    } else {
        mask_grid
    };

    let masked_grid = MaskedGrid::new(data_grid, mask_grid)?;
    Ok(GridOrEmpty::from(masked_grid))
}

/// This method reads the data for a single tile with a specified size from the GDAL dataset.
/// It handles conversion to grid coordinates.
/// If the tile is inside the dataset it uses the `read_grid_from_raster` method.
/// If the tile overlaps the borders of the dataset it uses the `read_partial_grid_from_raster` method.  
fn read_grid_and_handle_edges<T>(
    _dataset: &GdalDataset,
    rasterband: &GdalRasterBand,
    dataset_params: &GdalDatasetParameters,
    gdal_read_advice: GdalReadAdvise,
) -> Result<GridOrEmpty2D<T>>
where
    T: Pixel + GdalType + Default + FromPrimitive,
{
    let result_grid = if gdal_read_advice.direct_read() {
        read_grid_from_raster(
            rasterband,
            &gdal_read_advice.gdal_read_widow,
            gdal_read_advice.read_window_bounds.grid_shape(),
            dataset_params,
            gdal_read_advice.flip_y,
        )?
    } else {
        let r: GridOrEmpty<GridBoundingBox2D, T> = read_grid_from_raster(
            rasterband,
            &gdal_read_advice.gdal_read_widow,
            gdal_read_advice.read_window_bounds,
            dataset_params,
            gdal_read_advice.flip_y,
        )?;
        let mut tile_raster = GridOrEmpty::from(EmptyGrid::new(gdal_read_advice.bounds_of_target));
        tile_raster.grid_blit_from(&r);
        tile_raster.unbounded()
    };

    Ok(result_grid)
}

/// This method reads the data for a single tile with a specified size from the GDAL dataset and adds the requested metadata as properties to the tile.
fn read_raster_properties<T: Pixel + gdal::raster::GdalType + FromPrimitive>(
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
    };
    if let Some(offset) = gdal_dataset.offset() {
        properties.set_offset(offset);
    };

    // ignore if there is no description
    if let Ok(description) = gdal_dataset.description() {
        properties.set_description(description);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{MockExecutionContext, MockQueryContext};
    use crate::test_data;
    use crate::util::gdal::add_ndvi_dataset;
    use crate::util::Result;
    use geoengine_datatypes::hashmap;
    use geoengine_datatypes::primitives::{
        SpatialGridQueryRectangle, SpatialPartition2D, TimeInstance,
    };
    use geoengine_datatypes::raster::GridShape2D;
    use geoengine_datatypes::raster::{
        EmptyGrid2D, GridBounds, GridIdx2D, TilesEqualIgnoringCacheHint,
    };
    use geoengine_datatypes::raster::{TileInformation, TilingStrategy};
    use geoengine_datatypes::util::gdal::hide_gdal_errors;
    use httptest::matchers::request;
    use httptest::{responders, Expectation, Server};

    async fn query_gdal_source(
        exe_ctx: &MockExecutionContext,
        query_ctx: &MockQueryContext,
        name: NamedData,
        spatial_query: SpatialGridQueryRectangle,
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

    // This method loads raster data from a cropped MODIS NDVI raster.
    // To inspect the byte values first convert the file to XYZ with GDAL:
    // 'gdal_translate -of RST MOD13A2_M_NDVI_2014-04-01_27x27_compress.tif MOD13A2_M_NDVI_2014-04-01_27x27.xyz'
    // Then you can convert them to gruped bytes:
    // 'cut -d ' ' -f 1,2 --complement MOD13A2_M_NDVI_2014-04-01_27x27.xyz | xargs -n 27 > MOD13A2_M_NDVI_2014-04-01_27x27_bytes.txt'.
    fn load_ndvi_apr_2014_cropped(
        gdal_read_advice: GdalReadAdvise,
    ) -> Result<Option<GridAndProperties<u8>>> {
        let dataset_params = GdalDatasetParameters {
            file_path: test_data!(
                "raster/modis_ndvi/cropped/MOD13A2_M_NDVI_2014-04-01_27x27_compress.tif"
            )
            .into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (30.9, 61.7).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 27,
            height: 27,
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

        GdalRasterLoader::load_tile_data::<u8>(&dataset_params, gdal_read_advice)
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

    #[test]
    fn test_load_tile_data_top_left() {
        let gdal_read_advice = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), [8, 8].into()),
            read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            flip_y: false,
        };

        let GridAndProperties { grid, properties } = load_ndvi_apr_2014_cropped(gdal_read_advice)
            .unwrap()
            .unwrap();

        assert!(!grid.is_empty());

        let grid = grid.into_materialized_masked_grid();

        assert_eq!(grid.inner_grid.data.len(), 64);
        // pixel value are the top left 8x8 block from MOD13A2_M_NDVI_2014-04-01_27x27_bytes.txt
        assert_eq!(
            grid.inner_grid.data,
            &[
                147, 153, 164, 164, 163, 180, 191, 184, 101, 123, 135, 132, 145, 154, 175, 188,
                108, 112, 148, 164, 170, 166, 157, 164, 148, 126, 101, 116, 143, 145, 137, 140,
                104, 53, 0, 255, 90, 103, 102, 81, 255, 255, 255, 141, 85, 97, 92, 95, 255, 255,
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            ]
        );

        assert_eq!(grid.validity_mask.data.len(), 64);
        // pixel mask is pixel > 0 from the top left 8x8 block from MOD13A2_M_NDVI_2014-04-01_27x27_bytes.txt
        assert_eq!(
            grid.validity_mask.data,
            &[
                true, true, true, true, true, true, true, true, true, true, true, true, true, true,
                true, true, true, true, true, true, true, true, true, true, true, true, true, true,
                true, true, true, true, true, true, false, true, true, true, true, true, true,
                true, true, true, true, true, true, true, true, true, true, true, true, true, true,
                true, true, true, true, true, true, true, true, true,
            ]
        );

        assert!((properties.scale_option()).is_none());
        assert!(properties.offset_option().is_none());
        assert_eq!(
            properties.get_property(&RasterPropertiesKey {
                key: "AREA_OR_POINT".to_string(),
                domain: None,
            }),
            Some(&RasterPropertiesEntry::String("Area".to_string()))
        );
        assert_eq!(
            properties.get_property(&RasterPropertiesKey {
                domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                key: "COMPRESSION".to_string(),
            }),
            Some(&RasterPropertiesEntry::String("DEFLATE".to_string()))
        );

        assert_eq!(properties.offset_option(), None);
        assert_eq!(properties.scale_option(), None);
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

        let GridAndProperties { grid, properties } = load_ndvi_apr_2014_cropped(gdal_read_advice)
            .unwrap()
            .unwrap();

        assert!(!grid.is_empty());

        let x = grid.into_materialized_masked_grid();

        assert_eq!(x.inner_grid.data.len(), 64);
        assert_eq!(
            x.inner_grid.data,
            &[
                0, 0, 0, 0, 0, 0, 0, 0, 0, 147, 153, 164, 164, 163, 180, 191, 0, 101, 123, 135,
                132, 145, 154, 175, 0, 108, 112, 148, 164, 170, 166, 157, 0, 148, 126, 101, 116,
                143, 145, 137, 0, 104, 53, 0, 255, 90, 103, 102, 0, 255, 255, 255, 141, 85, 97, 92,
                0, 255, 255, 255, 255, 255, 255, 255
            ]
        );

        assert_eq!(x.validity_mask.data.len(), 64);
        // pixel mask is pixel > 0 from the top left 8x8 block from MOD13A2_M_NDVI_2014-04-01_27x27_bytes.txt
        assert_eq!(
            x.validity_mask.data,
            &[
                false, false, false, false, false, false, false, false, false, true, true, true,
                true, true, true, true, false, true, true, true, true, true, true, true, false,
                true, true, true, true, true, true, true, false, true, true, true, true, true,
                true, true, false, true, true, false, true, true, true, true, false, true, true,
                true, true, true, true, true, false, true, true, true, true, true, true, true,
            ]
        );

        assert_eq!(properties.offset_option(), None);
        assert_eq!(properties.scale_option(), None);
    }

    /* This test no longer works since we now employ a clipping strategy and this makes us read a lot more data?
    #[test]
    fn test_load_tile_data_is_inside_single_pixel() {
        let output_shape: GridShape2D = [8, 8].into();
        // shift world bbox one pixel up and to the left
        let (x_size, y_size) = (0.001, 0.001);
        let output_bounds = SpatialPartition2D::new(
            (-116.22222, 66.66666).into(),
            (-116.22222 + x_size, 66.66666 - y_size).into(),
        )
        .unwrap();

        let RasterTile2D {
            global_geo_transform: _,
            grid_array: grid,
            tile_position: _,
            band: _,
            time: _,
            properties: _,
            cache_hint: _,
        } = load_ndvi_jan_2014(output_shape, output_bounds).unwrap();

        assert!(!grid.is_empty());

        let x = grid.into_materialized_masked_grid();

        assert_eq!(x.inner_grid.data.len(), 64);
        assert_eq!(x.inner_grid.data, &[1; 64]);
    }
    */

    #[tokio::test]
    async fn test_query_single_time_slice() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = MockQueryContext::test_default();
        let id = add_ndvi_dataset(&mut exe_ctx);
        let spatial_query = SpatialGridQueryRectangle::new(
            GridBoundingBox2D::new([-256, -256], [255, 255]).unwrap(),
        );

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
        let query_ctx = MockQueryContext::test_default();
        let id = add_ndvi_dataset(&mut exe_ctx);

        let spatial_query = SpatialGridQueryRectangle::new(
            GridBoundingBox2D::new([-256, -256], [255, 255]).unwrap(),
        );

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
        let query_ctx = MockQueryContext::test_default();
        let id = add_ndvi_dataset(&mut exe_ctx);

        let spatial_query = SpatialGridQueryRectangle::new(
            GridBoundingBox2D::new([-256, -256], [255, 255]).unwrap(),
        );
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
        let query_ctx = MockQueryContext::test_default();
        let id = add_ndvi_dataset(&mut exe_ctx);

        let spatial_query = SpatialGridQueryRectangle::new(
            GridBoundingBox2D::new([-256, -256], [255, 255]).unwrap(),
        );
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
        let query_ctx = MockQueryContext::test_default();
        let id = add_ndvi_dataset(&mut exe_ctx);

        let spatial_query = SpatialGridQueryRectangle::new(
            GridBoundingBox2D::new([-256, -256], [255, 255]).unwrap(),
        );
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

        let tile_info = TileInformation::with_partition_and_shape(output_bounds, output_shape);
        let time_interval = TimeInterval::new_unchecked(1_388_534_400_000, 1_391_212_800_000); // 2014-01-01 - 2014-01-15
        let params = None;
        let reader_mode = GdalReaderMode::OriginalResolution(ReaderState {
            dataset_shape: GridShape2D::new([3600, 1800]),
            dataset_geo_transform: tile_info.global_geo_transform,
        });

        let tile = GdalRasterLoader::load_tile_async::<f64>(
            params,
            reader_mode,
            tile_info,
            time_interval,
            CacheHint::default(),
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
    fn it_reverts_config_options() {
        let config_options = vec![("foo".to_owned(), "bar".to_owned())];

        {
            let _config =
                TemporaryGdalThreadLocalConfigOptions::new(config_options.as_slice()).unwrap();

            assert_eq!(
                gdal::config::get_config_option("foo", "default").unwrap(),
                "bar".to_owned()
            );
        }

        assert_eq!(
            gdal::config::get_config_option("foo", "").unwrap(),
            String::new()
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

    /* FIXME: add upside down support back
    #[test]
    fn read_up_side_down_raster() {
        let output_shape: GridShape2D = [8, 8].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());

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

        let tile_information =
            TileInformation::with_partition_and_shape(output_bounds, output_shape);

        let RasterTile2D {
            global_geo_transform: _,
            grid_array: grid,
            tile_position: _,
            band: _,
            time: _,
            properties,
            cache_hint: _,
        } = GdalRasterLoader::load_tile_data::<u8>(
            &up_side_down_params,
            tile_information,
            TimeInterval::default(),
            CacheHint::default(),
        )
        .unwrap();

        assert!(!grid.is_empty());

        let grid = grid.into_materialized_masked_grid();

        assert_eq!(grid.inner_grid.data.len(), 64);
        assert_eq!(
            grid.inner_grid.data,
            &[
                255, 255, 255, 255, 255, 255, 255, 255, 255, 75, 37, 255, 44, 34, 39, 32, 255, 86,
                255, 255, 255, 30, 96, 255, 255, 255, 255, 255, 90, 255, 255, 255, 255, 255, 202,
                255, 193, 255, 255, 255, 255, 255, 89, 255, 111, 255, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255
            ]
        );

        assert_eq!(grid.validity_mask.data.len(), 64);
        assert_eq!(grid.validity_mask.data, &[true; 64]);

        assert!(properties.offset_option().is_none());
        assert!(properties.scale_option().is_none());
    }

    */

    #[test]
    fn read_raster_and_offset_scale() {
        let up_side_down_params = GdalDatasetParameters {
            file_path: test_data!(
                "raster/modis_ndvi/cropped/MOD13A2_M_NDVI_2014-04-01_27x27_compress_scale2_offset1.tif"
            )
            .into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (30.9, 61.7).into(),
                x_pixel_size: 0.1,
                y_pixel_size: -0.1,
            },
            width: 27,
            height: 27,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(0.),
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

        let GridAndProperties { grid, properties } =
            GdalRasterLoader::load_tile_data::<u8>(&up_side_down_params, gdal_read_advice)
                .unwrap()
                .unwrap();

        assert!(!grid.is_empty());

        let grid = grid.into_materialized_masked_grid();

        assert_eq!(grid.inner_grid.data.len(), 64);
        // pixel value are the top left 8x8 block from MOD13A2_M_NDVI_2014-04-01_27x27_bytes.txt
        assert_eq!(
            grid.inner_grid.data,
            &[
                147, 153, 164, 164, 163, 180, 191, 184, 101, 123, 135, 132, 145, 154, 175, 188,
                108, 112, 148, 164, 170, 166, 157, 164, 148, 126, 101, 116, 143, 145, 137, 140,
                104, 53, 0, 255, 90, 103, 102, 81, 255, 255, 255, 141, 85, 97, 92, 95, 255, 255,
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            ]
        );

        assert_eq!(grid.validity_mask.data.len(), 64);
        // pixel mask is pixel > 0 from the top left 8x8 block from MOD13A2_M_NDVI_2014-04-01_27x27_bytes.txt
        assert_eq!(
            grid.validity_mask.data,
            &[
                true, true, true, true, true, true, true, true, true, true, true, true, true, true,
                true, true, true, true, true, true, true, true, true, true, true, true, true, true,
                true, true, true, true, true, true, false, true, true, true, true, true, true,
                true, true, true, true, true, true, true, true, true, true, true, true, true, true,
                true, true, true, true, true, true, true, true, true,
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

        let gdal_read_advice = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), [8, 8].into()),
            read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            flip_y: false,
        };

        let res = GdalRasterLoader::load_tile_data::<u8>(&ds, gdal_read_advice);

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
        let result = GdalRasterLoader::load_tile_data::<u8>(&ds, gdal_read_advice);
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
        let gdal_read_advice = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new([0, 0].into(), [8, 8].into()),
            read_window_bounds: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            bounds_of_target: GridBoundingBox2D::new([0, 0], [7, 7]).unwrap(),
            flip_y: false,
        };

        let res = GdalRasterLoader::load_tile_data::<u8>(&ds, gdal_read_advice);
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
        let res = GdalRasterLoader::load_tile_data::<u8>(&ds, gdal_read_advice);
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

    #[tokio::test]
    async fn it_attaches_cache_hint() {
        let output_bounds =
            SpatialPartition2D::new_unchecked((-90., 90.).into(), (90., -90.).into());
        let output_shape: GridShape2D = [256, 256].into();

        let tile_info = TileInformation::with_partition_and_shape(output_bounds, output_shape);
        let time_interval = TimeInterval::new_unchecked(1_388_534_400_000, 1_391_212_800_000); // 2014-01-01 - 2014-01-15
        let params = None;

        let tile = GdalRasterLoader::load_tile_async::<f64>(
            params,
            GdalReaderMode::OriginalResolution(ReaderState {
                dataset_shape: GridShape2D::new([3600, 1800]),
                dataset_geo_transform: tile_info.global_geo_transform,
            }),
            tile_info,
            time_interval,
            CacheHint::seconds(1234),
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
