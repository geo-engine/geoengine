use crate::adapters::SparseTilesFillAdapter;
use crate::engine::{CreateSpan, MetaData, OperatorData, OperatorName, QueryProcessor};
use crate::util::gdal::gdal_open_dataset_ex;
use crate::util::input::float_option_with_nan;
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
use gdal::raster::{GdalType, RasterBand as GdalRasterBand};
use gdal::{Dataset as GdalDataset, DatasetOptions, GdalOpenFlags, Metadata as GdalMetadata};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, Coordinate2D, DateTimeParseFormat, RasterQueryRectangle,
    SpatialPartition2D, SpatialPartitioned,
};
use geoengine_datatypes::raster::{
    EmptyGrid, GeoTransform, GridIdx2D, GridOrEmpty, GridOrEmpty2D, GridShape2D, GridShapeAccess,
    MapElements, MaskedGrid, NoDataValueGrid, Pixel, RasterDataType, RasterProperties,
    RasterPropertiesEntry, RasterPropertiesEntryType, RasterPropertiesKey, RasterTile2D,
    TilingStrategy,
};
use geoengine_datatypes::util::test::TestDefault;
use geoengine_datatypes::{dataset::DataId, raster::TileInformation};
use geoengine_datatypes::{
    primitives::TimeInterval,
    raster::{Grid, GridBlit, GridBoundingBox2D, GridIdx, GridSize, TilingSpecification},
};
pub use loading_info::{
    GdalLoadingInfo, GdalLoadingInfoTemporalSlice, GdalLoadingInfoTemporalSliceIterator,
    GdalMetaDataList, GdalMetaDataRegular, GdalMetaDataStatic, GdalMetadataNetCdfCf,
};
use log::debug;
use num::FromPrimitive;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::time::Instant;
use tracing::{span, Level};

mod error;
mod loading_info;

/// Parameters for the GDAL Source Operator
///
/// # Examples
///
/// ```rust
/// use serde_json::{Result, Value};
/// use geoengine_operators::source::{GdalSource, GdalSourceParameters};
/// use geoengine_datatypes::dataset::{DatasetId, DataId};
/// use geoengine_datatypes::util::Identifier;
/// use std::str::FromStr;
///
/// let json_string = r#"
///     {
///         "type": "GdalSource",
///         "params": {
///             "data": {
///                 "type": "internal",
///                 "datasetId": "a626c880-1c41-489b-9e19-9596d129859c"
///             }
///         }
///     }"#;
///
/// let operator: GdalSource = serde_json::from_str(json_string).unwrap();
///
/// assert_eq!(operator, GdalSource {
///     params: GdalSourceParameters {
///         data: DatasetId::from_str("a626c880-1c41-489b-9e19-9596d129859c").unwrap().into()
///     },
/// });
/// ```
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct GdalSourceParameters {
    pub data: DataId,
}

impl OperatorData for GdalSourceParameters {
    fn data_ids_collect(&self, data_ids: &mut Vec<DataId>) {
        data_ids.push(self.data.clone());
    }
}

type GdalMetaData =
    Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GdalSourceTimePlaceholder {
    pub format: DateTimeParseFormat,
    pub reference: TimeReference,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
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
}

#[derive(Debug, PartialEq, Eq)]
struct GdalReadWindow {
    read_start_x: isize, // pixelspace origin
    read_start_y: isize,
    read_size_x: usize, // pixelspace size
    read_size_y: usize,
}

impl GdalReadWindow {
    fn gdal_window_start(&self) -> (isize, isize) {
        (self.read_start_x, self.read_start_y)
    }

    fn gdal_window_size(&self) -> (usize, usize) {
        (self.read_size_x, self.read_size_y)
    }
}

/// A user friendly representation of Gdal's geo transform. In contrast to [`GeoTransform`] this
/// geo transform allows arbitrary pixel sizes and can thus also represent rasters where the origin is not located
/// in the upper left corner. It should only be used for loading rasters with Gdal and not internally.
/// The GDAL pixel space is usually anchored at the "top-left" corner of the data spatial bounds. Therefore the raster data is stored with spatial coordinate y-values decreasing with the rasters rows. This is represented by a negative pixel size.
/// However, there are datasets where the data is stored "upside-down". If this is the case, the pixel size is positive.
#[derive(Copy, Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GdalDatasetGeoTransform {
    pub origin_coordinate: Coordinate2D,
    pub x_pixel_size: f64,
    pub y_pixel_size: f64,
}

impl GdalDatasetGeoTransform {
    /// Produce the `SpatialPartition` anchored at the datasets origin with a size of x * y pixels. This method handles non-standard pixel sizes.
    pub fn spatial_partition(&self, x_size: usize, y_size: usize) -> SpatialPartition2D {
        // the opposite y value (y value of the non origin edge)
        let opposite_coord_y = self.origin_coordinate.y + self.y_pixel_size * y_size as f64;

        // if the y-axis is negative then the origin is on the upper side.
        let (upper_y, lower_y) = if self.y_pixel_size.is_sign_negative() {
            (self.origin_coordinate.y, opposite_coord_y)
        } else {
            (opposite_coord_y, self.origin_coordinate.y)
        };

        let opposite_coord_x = self.origin_coordinate.x + self.x_pixel_size * x_size as f64;

        // if the y-axis is negative then the origin is on the upper side.
        let (left_x, right_x) = if self.x_pixel_size.is_sign_positive() {
            (self.origin_coordinate.x, opposite_coord_x)
        } else {
            (opposite_coord_x, self.origin_coordinate.x)
        };

        SpatialPartition2D::new_unchecked(
            Coordinate2D::new(left_x, upper_y),
            Coordinate2D::new(right_x, lower_y),
        )
    }

    /// Transform a `Coordinate2D` into a `GridIdx2D`
    #[inline]
    pub fn coordinate_to_grid_idx_2d(&self, coord: Coordinate2D) -> GridIdx2D {
        // TODO: use an epsilon here?
        let grid_x_index =
            ((coord.x - self.origin_coordinate.x) / self.x_pixel_size).floor() as isize;
        let grid_y_index =
            ((coord.y - self.origin_coordinate.y) / self.y_pixel_size).floor() as isize;

        [grid_y_index, grid_x_index].into()
    }

    fn spatial_partition_to_read_window(
        &self,
        spatial_partition: &SpatialPartition2D,
    ) -> GdalReadWindow {
        // World coordinates and pixel sizes use float values. Since the float imprecision might cause overflowing into the next pixel we use an epsilon to correct values very close the pixel borders. This logic is the same as used in [`GeoTransform::grid_idx_to_pixel_upper_left_coordinate_2d`].
        const EPSILON: f64 = 0.000_001;
        let epsilon: Coordinate2D =
            (self.x_pixel_size * EPSILON, self.y_pixel_size * EPSILON).into();

        /*
        The read window is relative to the transform of the gdal dataset. The `SpatialPartition` is oriented at axis of the spatial SRS. This usually causes this situation:

        The gdal data is stored with negative pixel size. The "ul" coordinate of the `SpatialPartition` is neareest to the origin of the gdal raster data.
        ul                      ur
        +_______________________+
        |_|_ row 1              |
        | |_|_  row 2           |
        |   |_|_  row ...       |
        |     |_|               |
        |_______________________|
        +                       *
        ll                      lr

        However, sometimes the data is stored up-side down. Like this:

        The gdal data is stored with a positive pixel size. So the "ll" coordinate is nearest to the reading the raster data needs to starts at this anchor.

        ul                      ur
        +_______________________+
        |      _                |
        |    _|_|  row ...      |
        |  _|_|  row 3          |
        | |_|  row 2            |
        |_______________________|
        +                       *
        ll                      lr

        Therefore we need to select the raster read start based on the coordinate next to the raster data origin. From there we then calculate the size of the window to read.
        */
        let (near_origin_coord, far_origin_coord) = if self.y_pixel_size.is_sign_negative() {
            (
                spatial_partition.upper_left(),
                spatial_partition.lower_right(),
            )
        } else {
            (
                spatial_partition.lower_left(),
                spatial_partition.upper_right(),
            )
        };

        // Move the coordinate near the origin a bit inside the bbox by adding an epsilon of the pixel size.
        let safe_near_coord = near_origin_coord + epsilon;
        // Move the coordinate far from the origin a bit inside the bbox by subtracting an epsilon of the pixel size
        let safe_far_coord = far_origin_coord - epsilon;

        let GridIdx([near_idx_y, near_idx_x]) = self.coordinate_to_grid_idx_2d(safe_near_coord);
        let GridIdx([far_idx_y, far_idx_x]) = self.coordinate_to_grid_idx_2d(safe_far_coord);

        debug_assert!(near_idx_x <= far_idx_x);
        debug_assert!(near_idx_y <= far_idx_y);

        let read_size_x = (far_idx_x - near_idx_x) as usize + 1;
        let read_size_y = (far_idx_y - near_idx_y) as usize + 1;

        GdalReadWindow {
            read_start_x: near_idx_x,
            read_start_y: near_idx_y,
            read_size_x: read_size_x as usize,
            read_size_y: read_size_y as usize,
        }
    }
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

impl SpatialPartitioned for GdalDatasetParameters {
    fn spatial_partition(&self) -> SpatialPartition2D {
        self.geo_transform
            .spatial_partition(self.width, self.height)
    }
}

impl GridShapeAccess for GdalDatasetParameters {
    type ShapeArray = [usize; 2];

    fn grid_shape_array(&self) -> Self::ShapeArray {
        [self.height, self.width]
    }
}

/// How to handle file not found errors
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
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
    pub tiling_specification: TilingSpecification,
    pub meta_data: GdalMetaData,
    pub _phantom_data: PhantomData<T>,
}

struct GdalRasterLoader {}

impl GdalRasterLoader {
    ///
    /// A method to async load single tiles from a GDAL dataset.
    ///
    async fn load_tile_data_async<T: Pixel + GdalType + FromPrimitive>(
        dataset_params: GdalDatasetParameters,
        tile_information: TileInformation,
        tile_time: TimeInterval,
    ) -> Result<RasterTile2D<T>> {
        crate::util::spawn_blocking(move || {
            Self::load_tile_data(&dataset_params, tile_information, tile_time)
        })
        .await
        .context(crate::error::TokioJoin)?
    }

    async fn load_tile_async<T: Pixel + GdalType + FromPrimitive>(
        dataset_params: Option<GdalDatasetParameters>,
        tile_information: TileInformation,
        tile_time: TimeInterval,
    ) -> Result<RasterTile2D<T>> {
        match dataset_params {
            // TODO: discuss if we need this check here. The metadata provider should only pass on loading infos if the query intersects the datasets bounds! And the tiling strategy should only generate tiles that intersect the querys bbox.
            Some(ds)
                if tile_information
                    .spatial_partition()
                    .intersects(&ds.spatial_partition()) =>
            {
                debug!(
                    "Loading tile {:?}, from {:?}, band: {}",
                    &tile_information, ds.file_path, ds.rasterband_channel
                );
                Self::load_tile_data_async(ds, tile_information, tile_time).await
            }
            Some(_) => {
                debug!("Skipping tile not in query rect {:?}", &tile_information);

                Ok(create_no_data_tile(tile_information, tile_time))
            }

            _ => {
                debug!(
                    "Skipping tile without GdalDatasetParameters {:?}",
                    &tile_information
                );

                Ok(create_no_data_tile(tile_information, tile_time))
            }
        }
    }

    ///
    /// A method to load single tiles from a GDAL dataset.
    ///
    fn load_tile_data<T: Pixel + GdalType + FromPrimitive>(
        dataset_params: &GdalDatasetParameters,
        tile_information: TileInformation,
        tile_time: TimeInterval,
    ) -> Result<RasterTile2D<T>> {
        let start = Instant::now();

        debug!(
            "GridOrEmpty2D<{:?}> requested for {:?}.",
            T::TYPE,
            &tile_information.spatial_partition()
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

        if dataset_result.is_err() {
            // TODO: check if Gdal error is actually file not found

            let err_result = match dataset_params.file_not_found_handling {
                FileNotFoundHandling::NoData => {
                    Ok(create_no_data_tile(tile_information, tile_time))
                }
                FileNotFoundHandling::Error => Err(crate::error::Error::CouldNotOpenGdalDataset {
                    file_path: dataset_params.file_path.to_string_lossy().to_string(),
                }),
            };
            let elapsed = start.elapsed();
            debug!(
                "file not found -> returning error = {}, took {:?}",
                err_result.is_err(),
                elapsed
            );
            return err_result;
        };

        let dataset = dataset_result.expect("checked");

        let result_tile = read_raster_tile_with_properties(
            &dataset,
            dataset_params,
            tile_information,
            tile_time,
        )?;

        let elapsed = start.elapsed();
        debug!("data loaded -> returning data grid, took {:?}", elapsed);

        Ok(result_tile)
    }

    ///
    /// A stream of futures producing `RasterTile2D` for a single slice in time
    ///
    fn temporal_slice_tile_future_stream<T: Pixel + GdalType + FromPrimitive>(
        query: RasterQueryRectangle,
        info: GdalLoadingInfoTemporalSlice,
        tiling_strategy: TilingStrategy,
    ) -> impl Stream<Item = impl Future<Output = Result<RasterTile2D<T>>>> {
        stream::iter(tiling_strategy.tile_information_iterator(query.spatial_bounds)).map(
            move |tile| GdalRasterLoader::load_tile_async(info.params.clone(), tile, info.time),
        )
    }

    fn loading_info_to_tile_stream<
        T: Pixel + GdalType + FromPrimitive,
        S: Stream<Item = Result<GdalLoadingInfoTemporalSlice>>,
    >(
        loading_info_stream: S,
        query: RasterQueryRectangle,
        tiling_strategy: TilingStrategy,
    ) -> impl Stream<Item = Result<RasterTile2D<T>>> {
        loading_info_stream
            .map_ok(move |info| {
                GdalRasterLoader::temporal_slice_tile_future_stream(query, info, tiling_strategy)
                    .map(Result::Ok)
            })
            .try_flatten()
            .try_buffered(16) // TODO: make this configurable
    }
}

impl<T> GdalSourceProcessor<T> where T: gdal::raster::GdalType + Pixel {}

#[async_trait]
impl<P> QueryProcessor for GdalSourceProcessor<P>
where
    P: Pixel + gdal::raster::GdalType + FromPrimitive,
{
    type Output = RasterTile2D<P>;
    type SpatialBounds = SpatialPartition2D;

    async fn query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        _ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<BoxStream<Result<Self::Output>>> {
        let start = Instant::now();
        debug!(
            "Querying GdalSourceProcessor<{:?}> with: {:?}.",
            P::TYPE,
            &query
        );

        debug!(
            "GdalSourceProcessor<{:?}> meta data loaded, took {:?}.",
            P::TYPE,
            start.elapsed()
        );

        let spatial_resolution = query.spatial_resolution;

        // A `GeoTransform` maps pixel space to world space.
        // Usually a SRS has axis directions pointing "up" (y-axis) and "up" (y-axis).
        // We are not aware of spatial reference systems where the x-axis points to the right.
        // However, there are spatial reference systems where the y-axis points downwards.
        // The standard "pixel-space" starts at the top-left corner of a `GeoTransform` and points down-right.
        // Therefore, the pixel size on the x-axis is always increasing
        let pixel_size_x = spatial_resolution.x;
        debug_assert!(pixel_size_x.is_sign_positive());
        // and the y-axis should only be positive if the y-axis of the spatial reference system also "points down".
        // NOTE: at the moment we do not allow "down pointing" y-axis.
        let pixel_size_y = spatial_resolution.y * -1.0;
        debug_assert!(pixel_size_y.is_sign_negative());

        let tiling_strategy = self
            .tiling_specification
            .strategy(pixel_size_x, pixel_size_y);

        let result_descriptor = self.meta_data.result_descriptor().await?;

        let mut empty = false;
        debug!("result descr bbox: {:?}", result_descriptor.bbox);
        debug!("query bbox: {:?}", query.spatial_bounds);

        if let Some(data_spatial_bounds) = result_descriptor.bbox {
            if !data_spatial_bounds.intersects(&query.spatial_bounds) {
                debug!("query does not intersect spatial data bounds");
                empty = true;
            }
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

        let loading_iter = if empty {
            GdalLoadingInfoTemporalSliceIterator::Static {
                parts: vec![].into_iter(),
            }
        } else {
            let loading_info = self.meta_data.loading_info(query).await?;
            loading_info.info
        };

        let source_stream = stream::iter(loading_iter);

        let source_stream =
            GdalRasterLoader::loading_info_to_tile_stream(source_stream, query, tiling_strategy);

        // use SparseTilesFillAdapter to fill all the gaps
        let filled_stream = SparseTilesFillAdapter::new(
            source_stream,
            tiling_strategy.tile_grid_box(query.spatial_partition()),
            tiling_strategy.geo_transform,
            tiling_strategy.tile_size_in_pixels,
        );

        Ok(filled_stream.boxed())
    }
}

pub type GdalSource = SourceOperator<GdalSourceParameters>;

impl OperatorName for GdalSource {
    const TYPE_NAME: &'static str = "GdalSource";
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for GdalSource {
    async fn _initialize(
        self: Box<Self>,
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let meta_data: GdalMetaData = context.meta_data(&self.params.data).await?;

        debug!("Initializing GdalSource for {:?}.", &self.params.data);

        let op = InitializedGdalSourceOperator {
            result_descriptor: meta_data.result_descriptor().await?,
            meta_data,
            tiling_specification: context.tiling_specification(),
        };

        Ok(op.boxed())
    }

    span_fn!(GdalSource);
}

pub struct InitializedGdalSourceOperator {
    pub meta_data: GdalMetaData,
    pub result_descriptor: RasterResultDescriptor,
    pub tiling_specification: TilingSpecification,
}

impl InitializedRasterOperator for InitializedGdalSourceOperator {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        Ok(match self.result_descriptor().data_type {
            RasterDataType::U8 => TypedRasterQueryProcessor::U8(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::U16 => TypedRasterQueryProcessor::U16(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::U32 => TypedRasterQueryProcessor::U32(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
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
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::I32 => TypedRasterQueryProcessor::I32(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
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
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
            RasterDataType::F64 => TypedRasterQueryProcessor::F64(
                GdalSourceProcessor {
                    tiling_specification: self.tiling_specification,
                    meta_data: self.meta_data.clone(),
                    _phantom_data: PhantomData,
                }
                .boxed(),
            ),
        })
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

/// This method reads the data for a single grid with a specified size from the GDAL dataset.
/// If the tile overlaps the borders of the dataset only the data in the dataset bounds is read.
/// The data read from the dataset is clipped into a grid with the requested size filled  with the `no_data_value`.
fn read_partial_grid_from_raster<T>(
    rasterband: &GdalRasterBand,
    gdal_read_window: &GdalReadWindow,
    out_tile_read_bounds: GridBoundingBox2D,
    out_tile_shape: GridShape2D,
    dataset_params: &GdalDatasetParameters,
    flip_y_axis: bool,
) -> Result<GridOrEmpty2D<T>>
where
    T: Pixel + GdalType + Default + FromPrimitive,
{
    let dataset_raster = read_grid_from_raster(
        rasterband,
        gdal_read_window,
        out_tile_read_bounds,
        dataset_params,
        flip_y_axis,
    )?;

    let mut tile_raster = GridOrEmpty::from(EmptyGrid::new(out_tile_shape));
    tile_raster.grid_blit_from(&dataset_raster);
    Ok(tile_raster)
}

/// This method reads the data for a single tile with a specified size from the GDAL dataset.
/// It handles conversion to grid coordinates.
/// If the tile is inside the dataset it uses the `read_grid_from_raster` method.
/// If the tile overlaps the borders of the dataset it uses the `read_partial_grid_from_raster` method.  
fn read_grid_and_handle_edges<T>(
    tile_info: TileInformation,
    dataset: &GdalDataset,
    rasterband: &GdalRasterBand,
    dataset_params: &GdalDatasetParameters,
) -> Result<GridOrEmpty2D<T>>
where
    T: Pixel + GdalType + Default + FromPrimitive,
{
    let gdal_dataset_geotransform = GdalDatasetGeoTransform::from(dataset.geo_transform()?);
    let (gdal_dataset_pixels_x, gdal_dataset_pixels_y) = dataset.raster_size();

    if !approx_eq!(
        GdalDatasetGeoTransform,
        gdal_dataset_geotransform,
        dataset_params.geo_transform
    ) {
        log::warn!(
            "GdalDatasetParameters geo transform is different to the one retrieved from GDAL dataset: {:?} != {:?}",
            dataset_params.geo_transform,
            gdal_dataset_geotransform,
        )
    };

    debug_assert_eq!(gdal_dataset_pixels_x, dataset_params.width);
    debug_assert_eq!(gdal_dataset_pixels_y, dataset_params.height);

    let gdal_dataset_bounds =
        gdal_dataset_geotransform.spatial_partition(gdal_dataset_pixels_x, gdal_dataset_pixels_y);

    let output_bounds = tile_info.spatial_partition();
    let dataset_intersects_tile = gdal_dataset_bounds.intersection(&output_bounds);
    let output_shape = tile_info.tile_size_in_pixels();

    let dataset_intersection_area = match dataset_intersects_tile {
        Some(i) => i,
        None => {
            return Ok(GridOrEmpty::from(EmptyGrid::new(output_shape)));
        }
    };

    let tile_geo_transform = tile_info.tile_geo_transform();

    let gdal_read_window =
        gdal_dataset_geotransform.spatial_partition_to_read_window(&dataset_intersection_area);

    let is_y_axis_flipped = tile_geo_transform.y_pixel_size().is_sign_negative()
        != gdal_dataset_geotransform.y_pixel_size.is_sign_negative();

    if is_y_axis_flipped {
        debug!("The GDAL data has a flipped y-axis. Need to unflip it!");
    }

    let result_grid = if dataset_intersection_area == output_bounds {
        read_grid_from_raster(
            rasterband,
            &gdal_read_window,
            output_shape,
            dataset_params,
            is_y_axis_flipped,
        )?
    } else {
        let partial_tile_grid_bounds =
            tile_geo_transform.spatial_to_grid_bounds(&dataset_intersection_area);

        read_partial_grid_from_raster(
            rasterband,
            &gdal_read_window,
            partial_tile_grid_bounds,
            output_shape,
            dataset_params,
            is_y_axis_flipped,
        )?
    };

    Ok(result_grid)
}

/// This method reads the data for a single tile with a specified size from the GDAL dataset and adds the requested metadata as properties to the tile.
fn read_raster_tile_with_properties<T: Pixel + gdal::raster::GdalType + FromPrimitive>(
    dataset: &GdalDataset,
    dataset_params: &GdalDatasetParameters,
    tile_info: TileInformation,
    tile_time: TimeInterval,
) -> Result<RasterTile2D<T>> {
    let rasterband = dataset.rasterband(dataset_params.rasterband_channel as isize)?;

    let result_grid = read_grid_and_handle_edges(tile_info, dataset, &rasterband, dataset_params)?;

    let mut properties = RasterProperties::default();

    if let Some(properties_mapping) = dataset_params.properties_mapping.as_ref() {
        properties_from_gdal(&mut properties, dataset, properties_mapping);
        let rasterband = dataset.rasterband(dataset_params.rasterband_channel as isize)?;
        properties_from_gdal(&mut properties, &rasterband, properties_mapping);
        properties_from_band(&mut properties, &rasterband);
    }

    Ok(RasterTile2D::new_with_tile_info_and_properties(
        tile_time,
        tile_info,
        result_grid,
        properties,
    ))
}

fn create_no_data_tile<T: Pixel>(
    tile_info: TileInformation,
    tile_time: TimeInterval,
) -> RasterTile2D<T> {
    RasterTile2D::new_with_tile_info_and_properties(
        tile_time,
        tile_info,
        EmptyGrid::new(tile_info.tile_size_in_pixels).into(),
        RasterProperties::default(),
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

fn properties_from_gdal<'a, I, M>(
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

            properties
                .properties_map
                .insert(m.target_key.clone(), entry);
        }
    }
}

fn properties_from_band(properties: &mut RasterProperties, gdal_dataset: &GdalRasterBand) {
    if let Some(scale) = gdal_dataset.metadata_item("scale", "") {
        properties.scale = scale.parse::<f64>().ok();
    };

    if let Some(offset) = gdal_dataset.metadata_item("offset", "") {
        properties.offset = offset.parse::<f64>().ok();
    };

    if let Some(band_name) = gdal_dataset.metadata_item("band_name", "") {
        properties.band_name = Some(band_name);
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
    use geoengine_datatypes::primitives::{AxisAlignedRectangle, SpatialPartition2D, TimeInstance};
    use geoengine_datatypes::raster::{EmptyGrid2D, GridBounds, GridIdx2D};
    use geoengine_datatypes::raster::{TileInformation, TilingStrategy};
    use geoengine_datatypes::util::gdal::hide_gdal_errors;
    use geoengine_datatypes::{primitives::SpatialResolution, raster::GridShape2D};

    async fn query_gdal_source(
        exe_ctx: &mut MockExecutionContext,
        query_ctx: &MockQueryContext,
        id: DataId,
        output_shape: GridShape2D,
        output_bounds: SpatialPartition2D,
        time_interval: TimeInterval,
    ) -> Vec<Result<RasterTile2D<u8>>> {
        let op = GdalSource {
            params: GdalSourceParameters { data: id.clone() },
        }
        .boxed();

        let x_query_resolution = output_bounds.size_x() / output_shape.axis_size_x() as f64;
        let y_query_resolution = output_bounds.size_y() / output_shape.axis_size_y() as f64;
        let spatial_resolution =
            SpatialResolution::new_unchecked(x_query_resolution, y_query_resolution);

        let o = op.initialize(exe_ctx).await.unwrap();

        o.query_processor()
            .unwrap()
            .get_u8()
            .unwrap()
            .raster_query(
                RasterQueryRectangle {
                    spatial_bounds: output_bounds,
                    time_interval,
                    spatial_resolution,
                },
                query_ctx,
            )
            .await
            .unwrap()
            .collect()
            .await
    }

    fn load_ndvi_jan_2014(
        output_shape: GridShape2D,
        output_bounds: SpatialPartition2D,
    ) -> Result<RasterTile2D<u8>> {
        GdalRasterLoader::load_tile_data::<u8>(
            &GdalDatasetParameters {
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
            },
            TileInformation::with_partition_and_shape(output_bounds, output_shape),
            TimeInterval::default(),
        )
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

        let partition = SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap();

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: central_geo_transform,
        };

        let vres: Vec<GridIdx2D> = origin_split_tileing_strategy
            .tile_idx_iterator(partition)
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

        let partition = SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap();

        let origin_split_tileing_strategy = TilingStrategy {
            tile_size_in_pixels: tile_size_in_pixels.into(),
            geo_transform: central_geo_transform,
        };

        let vres: Vec<TileInformation> = origin_split_tileing_strategy
            .tile_information_iterator(partition)
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
    fn test_load_tile_data() {
        let output_shape: GridShape2D = [8, 8].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());

        let RasterTile2D {
            global_geo_transform: _,
            grid_array: grid,
            tile_position: _,
            time: _,
            properties,
        } = load_ndvi_jan_2014(output_shape, output_bounds).unwrap();

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

        assert!(properties.scale.is_none());
        assert!(properties.offset.is_none());
        assert_eq!(
            properties.properties_map.get(&RasterPropertiesKey {
                key: "AREA_OR_POINT".to_string(),
                domain: None,
            }),
            Some(&RasterPropertiesEntry::String("Area".to_string()))
        );
        assert_eq!(
            properties.properties_map.get(&RasterPropertiesKey {
                domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                key: "COMPRESSION".to_string(),
            }),
            Some(&RasterPropertiesEntry::String("LZW".to_string()))
        );
    }

    #[test]
    fn test_load_tile_data_overlaps_dataset_bounds() {
        let output_shape: GridShape2D = [8, 8].into();
        // shift world bbox one pixel up and to the left
        let (x_size, y_size) = (45., 22.5);
        let output_bounds = SpatialPartition2D::new_unchecked(
            (-180. - x_size, 90. + y_size).into(),
            (180. - x_size, -90. + y_size).into(),
        );

        let RasterTile2D {
            global_geo_transform: _,
            grid_array: grid,
            tile_position: _,
            time: _,
            properties: _,
        } = load_ndvi_jan_2014(output_shape, output_bounds).unwrap();

        assert!(!grid.is_empty());

        let x = grid.into_materialized_masked_grid();

        assert_eq!(x.inner_grid.data.len(), 64);
        assert_eq!(
            x.inner_grid.data,
            &[
                0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 0, 255, 75, 37, 255,
                44, 34, 39, 0, 255, 86, 255, 255, 255, 30, 96, 0, 255, 255, 255, 255, 90, 255, 255,
                0, 255, 255, 202, 255, 193, 255, 255, 0, 255, 255, 89, 255, 111, 255, 255, 0, 255,
                255, 255, 255, 255, 255, 255
            ]
        );
    }

    #[test]
    fn test_load_tile_data_is_inside_single_pixel() {
        let output_shape: GridShape2D = [8, 8].into();
        // shift world bbox one pixel up and to the left
        let (x_size, y_size) = (0.000_000_000_01, 0.000_000_000_01);
        let output_bounds = SpatialPartition2D::new(
            (-116.22222, 66.66666).into(),
            (-116.22222 + x_size, 66.66666 - y_size).into(),
        )
        .unwrap();

        let RasterTile2D {
            global_geo_transform: _,
            grid_array: grid,
            tile_position: _,
            time: _,
            properties: _,
        } = load_ndvi_jan_2014(output_shape, output_bounds).unwrap();

        assert!(!grid.is_empty());

        let x = grid.into_materialized_masked_grid();

        assert_eq!(x.inner_grid.data.len(), 64);
        assert_eq!(x.inner_grid.data, &[1; 64]);
    }

    #[tokio::test]
    async fn test_query_single_time_slice() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let query_ctx = MockQueryContext::test_default();
        let id = add_ndvi_dataset(&mut exe_ctx);

        let output_shape: GridShape2D = [256, 256].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());
        let time_interval = TimeInterval::new_unchecked(1_388_534_400_000, 1_388_534_400_001); // 2014-01-01

        let c = query_gdal_source(
            &mut exe_ctx,
            &query_ctx,
            id,
            output_shape,
            output_bounds,
            time_interval,
        )
        .await;
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

        let output_shape: GridShape2D = [256, 256].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());
        let time_interval = TimeInterval::new_unchecked(1_388_534_400_000, 1_393_632_000_000); // 2014-01-01 - 2014-03-01

        let c = query_gdal_source(
            &mut exe_ctx,
            &query_ctx,
            id,
            output_shape,
            output_bounds,
            time_interval,
        )
        .await;
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

        let output_shape: GridShape2D = [256, 256].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());
        let time_interval = TimeInterval::new_unchecked(1_380_585_600_000, 1_380_585_600_000); // 2013-10-01 - 2013-10-01

        let c = query_gdal_source(
            &mut exe_ctx,
            &query_ctx,
            id,
            output_shape,
            output_bounds,
            time_interval,
        )
        .await;
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

        let output_shape: GridShape2D = [256, 256].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());
        let time_interval = TimeInterval::new_unchecked(1_420_074_000_000, 1_420_074_000_000); // 2015-01-01 - 2015-01-01

        let c = query_gdal_source(
            &mut exe_ctx,
            &query_ctx,
            id,
            output_shape,
            output_bounds,
            time_interval,
        )
        .await;
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

        let output_shape: GridShape2D = [256, 256].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());
        let time_interval = TimeInterval::new_unchecked(1_385_856_000_000, 1_388_534_400_000); // 2013-12-01 - 2014-01-01

        let c = query_gdal_source(
            &mut exe_ctx,
            &query_ctx,
            id,
            output_shape,
            output_bounds,
            time_interval,
        )
        .await;
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

        let tile = GdalRasterLoader::load_tile_async::<f64>(params, tile_info, time_interval).await;

        assert!(tile.is_ok());

        let expected = RasterTile2D::<f64>::new_with_tile_info(
            time_interval,
            tile_info,
            EmptyGrid2D::new(output_shape).into(),
        );

        assert_eq!(tile.unwrap(), expected);
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
    fn gdal_geotransform_to_bounds_neg_y_0() {
        let gt = GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(0., 0.),
            x_pixel_size: 1.,
            y_pixel_size: -1.,
        };

        let sb = gt.spatial_partition(10, 10);

        let exp = SpatialPartition2D::new(Coordinate2D::new(0., 0.), Coordinate2D::new(10., -10.))
            .unwrap();

        assert_eq!(sb, exp);
    }

    #[test]
    fn gdal_geotransform_to_bounds_neg_y_5() {
        let gt = GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(5., 5.),
            x_pixel_size: 0.5,
            y_pixel_size: -0.5,
        };

        let sb = gt.spatial_partition(10, 10);

        let exp =
            SpatialPartition2D::new(Coordinate2D::new(5., 5.), Coordinate2D::new(10., 0.)).unwrap();

        assert_eq!(sb, exp);
    }

    #[test]
    fn gdal_geotransform_to_bounds_pos_y_0() {
        let gt = GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(0., 0.),
            x_pixel_size: 1.,
            y_pixel_size: 1.,
        };

        let sb = gt.spatial_partition(10, 10);

        let exp = SpatialPartition2D::new(Coordinate2D::new(0., 10.), Coordinate2D::new(10., 0.))
            .unwrap();

        assert_eq!(sb, exp);
    }

    #[test]
    fn gdal_geotransform_to_bounds_pos_y_5() {
        let gt = GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(5., -5.),
            x_pixel_size: 0.5,
            y_pixel_size: 0.5,
        };

        let sb = gt.spatial_partition(10, 10);

        let exp = SpatialPartition2D::new(Coordinate2D::new(5., 0.), Coordinate2D::new(10., -5.))
            .unwrap();

        assert_eq!(sb, exp);
    }

    #[test]
    fn gdal_read_window_data_origin_upper_left() {
        let gt = GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(5., -5.),
            x_pixel_size: 0.5,
            y_pixel_size: -0.5,
        };

        let sb = SpatialPartition2D::new(Coordinate2D::new(8., -7.), Coordinate2D::new(10., -10.))
            .unwrap();

        let rw = gt.spatial_partition_to_read_window(&sb);

        let exp = GdalReadWindow {
            read_size_x: 4,
            read_size_y: 6,
            read_start_x: 6,
            read_start_y: 4,
        };

        assert_eq!(rw, exp);
    }

    #[test]
    fn gdal_read_window_data_origin_lower_left() {
        let gt = GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(0., 0.),
            x_pixel_size: 1.,
            y_pixel_size: 1.,
        };

        let sb = SpatialPartition2D::new(Coordinate2D::new(0., 10.), Coordinate2D::new(10., 0.))
            .unwrap();

        let rw = gt.spatial_partition_to_read_window(&sb);

        let exp = GdalReadWindow {
            read_size_x: 10,
            read_size_y: 10,
            read_start_x: 0,
            read_start_y: 0,
        };

        assert_eq!(rw, exp);
    }

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
        };

        let tile_information =
            TileInformation::with_partition_and_shape(output_bounds, output_shape);

        let RasterTile2D {
            global_geo_transform: _,
            grid_array: grid,
            tile_position: _,
            time: _,
            properties: _,
        } = GdalRasterLoader::load_tile_data::<u8>(
            &up_side_down_params,
            tile_information,
            TimeInterval::default(),
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
    }
}
