use crate::adapters::{FillerTileCacheExpirationStrategy, SparseTilesFillAdapter};
use crate::engine::{
    CanonicOperatorName, MetaData, OperatorData, OperatorName, QueryProcessor, WorkflowOperatorPath,
};
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
use geoengine_datatypes::primitives::CacheHint;
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, Coordinate2D, DateTimeParseFormat, RasterQueryRectangle,
    RasterSpatialQueryRectangle, SpatialPartition2D, SpatialPartitioned,
};
use geoengine_datatypes::raster::{
    EmptyGrid, GeoTransform, GridBoundingBoxExt, GridBounds, GridIdx2D, GridOrEmpty, GridOrEmpty2D,
    GridShape2D, GridShapeAccess, MapElements, MaskedGrid, NoDataValueGrid, Pixel, RasterDataType,
    RasterProperties, RasterPropertiesEntry, RasterPropertiesEntryType, RasterPropertiesKey,
    RasterTile2D, TilingStrategy, ChangeGridBounds,
};
use geoengine_datatypes::raster::{GridIntersection, TileInformation};
use geoengine_datatypes::util::test::TestDefault;
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
use std::ffi::CString;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::time::Instant;

mod error;
mod loading_info;

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
pub struct GdalSourceParameters {
    pub data: NamedData,
}

impl OperatorData for GdalSourceParameters {
    fn data_names_collect(&self, data_names: &mut Vec<NamedData>) {
        data_names.push(self.data.clone());
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
    pub retry: Option<GdalRetryOptions>,
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub struct GdalRetryOptions {
    pub max_retries: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct GdalReadWindow {
    read_start_x: isize, // pixelspace origin
    read_start_y: isize,
    read_size_x: usize, // pixelspace size
    read_size_y: usize,
}

impl GdalReadWindow {

    pub fn new(
        start: GridIdx2D,
        size: GridShape2D,
    ) -> Self {
        Self {
            read_start_x: start.x(),
            read_start_y: start.y(),
            read_size_x: size.axis_size_x(),
            read_size_y: size.axis_size_y(),
        }
    }

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
            read_size_x,
            read_size_y,
        }
    }

    fn grid_bounds_resolution_to_read_window_and_target_grid(&self, dataset_raster_size: GridShape2D, tile_info: &TileInformation) -> Option<(GdalReadWindow, GridBoundingBox2D)> {
        let gdal_dataset_geotransform = self;
        let gdal_dataset_pixels_x = dataset_raster_size.axis_size_x();
        let gdal_dataset_pixels_y = dataset_raster_size.axis_size_y();
    
    
        // figure out if the y axis is flipped
        let is_y_axis_flipped = tile_info
            .global_geo_transform
            .y_pixel_size()
            .is_sign_negative()
            != gdal_dataset_geotransform.y_pixel_size.is_sign_negative();
    
        if is_y_axis_flipped {
            debug!("The GDAL data has a flipped y-axis. Need to unflip it!");
        }
    
        // this are the bounds of the dataset in pixel space relative to the origin of the dataset
        let data_grid_bounds_in_data_geotransform = GridBoundingBox2D::new_unchecked(
            [0, 0],
            [
                (gdal_dataset_pixels_y -1) as isize, // we need to subtract one because the pixel index is zero based and grid bounds are inclusive
                (gdal_dataset_pixels_x -1) as isize,
            ],
        );
    
        // build a GeoTransform with the origin of the data but the pixel size of the tile we want to fill.
        // Fixme: this needs to change when the resolution is specific to the dataset
        let data_geo_transform_with_query_res = GeoTransform::new(
            gdal_dataset_geotransform.origin_coordinate,
            tile_info.global_geo_transform.x_pixel_size(),
            tile_info.global_geo_transform.y_pixel_size(),
        );
    
        // check that the tile we are trying to fill is anchored at the tiling origin
        // TODO: we should allow that the anchor point of the tile is not zero. However this will not change anything here except the method name.
        //debug_assert_eq!(
        //    tile_info.global_geo_transform.nearest_pixel_to_zero(),
        //    GridIdx([0, 0]),
        //    "tile is not anchored at the tiling origin of the dataset"
        //);
    
        // calculate the pixel offset between the tile and the data based on the anchor "pixel" which is relative to the origin of the dataset and the data resolution.
        // data_origin -> tile_origin aka. positive offset if the tile is to the right and below the data origin
        let pixel_offset_between_tile_and_data_in_query_res = data_geo_transform_with_query_res.nearest_pixel_to_zero() - tile_info.global_geo_transform.nearest_pixel_to_zero();
    
        // get the bounds of the tile in the pixels relative to the tiling origin
        let tile_pixel_bounds_in_query_res = tile_info.global_pixel_bounds();
    
        // now we move the tile grid we need to fill into the dataset grid space which lets us calculate the intersection between the tile and the dataset in dataset grid space.
        let shifted_tile_grid_bounds_in_query_res = tile_pixel_bounds_in_query_res.shift_by_offset(pixel_offset_between_tile_and_data_in_query_res);
    
        // ----- From here on we work with pixel coordinates relative to the data origin aka the ul of the raster is [0,0] -----
    
        // Now we need to calculate the intersection in the pixel size of the dataset to figure out what we need to read from the dataset.
        // Here it gets a bit tricky because we need to take into account that the tile and the dataset can have different resolutions.
        // If the resolution we request is a multiple of the dataset resolution we can just divide the intersection area by the resolution.
    
        let x_factor =
            gdal_dataset_geotransform.x_pixel_size / tile_info.global_geo_transform.x_pixel_size();
        let y_factor =
            gdal_dataset_geotransform.y_pixel_size / tile_info.global_geo_transform.y_pixel_size() ; // TODO: care for non negative y axis
    
        if !approx_eq!(f64, x_factor.fract(), 0.0) {
            log::debug!(
                "The x resolution of the tile is not a multiple of the dataset resolution: {} / {} = {}",
                gdal_dataset_geotransform.x_pixel_size,
                tile_info.global_geo_transform.x_pixel_size(),            
                x_factor
            );
        }
    
        if !approx_eq!(f64, y_factor.fract(), 0.0) {
            log::debug!(
                "The y resolution of the tile is not a multiple of the dataset resolution: {} / {} = {}",
                gdal_dataset_geotransform.y_pixel_size,
                tile_info.global_geo_transform.y_pixel_size(),            
                y_factor
            );
        }
    
        // create a pixel bounding box of the dataset with the pixel size of the tile
        let dataset_grid_bounds_in_query_res = GridBoundingBox2D::new_unchecked(
            [0, 0],
            [
                (gdal_dataset_pixels_y as f64 * y_factor).floor() as isize -1, // we do -1 because the pixel index is zero based and grid bounds are inclusive
                (gdal_dataset_pixels_x as f64 * x_factor).floor() as isize -1, // TODO: figure out if we need to ceil here
            ],
        );
    
        // calculate the intersection between the tile and the dataset in dataset grid space.
        // This implies that the [0,0] pixel is the origin of the dataset.
        let intersection_area_in_query_res = dataset_grid_bounds_in_query_res.intersection(&shifted_tile_grid_bounds_in_query_res);
    
        // if there is no intersection we can return an empty grid. This can happen if the tile is outside of the dataset.
        let intersection_area_in_query_res = if let Some(intersection_area_in_query_res) = intersection_area_in_query_res {
            intersection_area_in_query_res
        } else {
            debug!("Tile {:?} does not intersect dataset.", &tile_info);
            return None;
        };
    
        // calculate the location of the intersection in the pixel space of the dataset
        let ul_x_in_data_res = (intersection_area_in_query_res.min_index().x() ) as f64 / x_factor ; 
        let ul_y_in_data_res = (intersection_area_in_query_res.min_index().y() ) as f64 / y_factor ;
        let lr_x_in_data_res = (intersection_area_in_query_res.max_index().x() + 1) as f64 / x_factor - 1.; // The +1 -1 is caused by our inclusiveness of pixels in grid bounds
        let lr_y_in_data_res = (intersection_area_in_query_res.max_index().y() + 1) as f64 / y_factor - 1.;
    
        // we can't read data outside of the dataset so we need to clamp the intersection to the dataset bounds and we also need to correct the intersection area
        // since the pixels we want to fill might be smaller than the pixels of the dataset we need to calculate the offset of the tile pixels relative to the dataset pixels
        fn correct_ul(ul: f64) -> (f64, f64) {
            if ul < 0.0 {
                (0., ul) // negative ul means we are outside of the dataset and have to adapt the area we are reading from the dataset
            } else {
                (ul.floor(), ul.fract()) // positive ul means we are inside the dataset and the pixels we are going to fill are a fraction of the dataset pixels. Therefore we need to pad the pixels we read from the dataset to fill the pixel area
            }
        }
    
        let (ul_y_in_data_res, ul_y_correction) = correct_ul(ul_y_in_data_res); // don't need to pass min index because we already know it's zero
        let (ul_x_in_data_res, ul_x_correction) = correct_ul(ul_x_in_data_res);
    
        fn correct_lr(lr: f64, max: f64) -> (f64, f64) {
            if lr > max {
                (max, lr - max) // an lr value larger then the dataset size means we are outside of the dataset and have to adapt the area we are reading from the dataset
            } else if approx_eq!(f64, lr.fract(), 0.) {
                (lr.floor(), 0.0) // an lr value equal to the dataset size means we are exactly at the dataset border and don't need to correct the area we are reading from the dataset
            } else {
                (lr.floor(), lr.fract() - 1.0) // a lr value smaller then the dataset size means we are inside the dataset and might have a fraction of a dataset pixel to correct by padding
            }
        }
    
        let (lr_y_in_data_res, lr_y_correction) = correct_lr(lr_y_in_data_res, data_grid_bounds_in_data_geotransform.max_index().y() as f64);
        let (lr_x_in_data_res, lr_x_correction) = correct_lr(lr_x_in_data_res, data_grid_bounds_in_data_geotransform.max_index().x() as f64);
    
        // this are the bounds of the intersection in pixel space of the dataset clipped to the dataset bounds aka the area we need to read from the dataset
        let dataset_intersection_area = GridBoundingBox2D::new_unchecked(
            [ul_y_in_data_res as isize, ul_x_in_data_res as isize ],
            [lr_y_in_data_res as isize, lr_x_in_data_res as isize],
        );
    
        // now we need to adapt our read window in target pixel space to the clipped dataset intersection area
        // first we use the correction values to find out if we need to add padding by a fraction of a dataset pixel
        let fraction_tile_pixel_offset_ul_x = ul_x_correction * x_factor; // TODO: round?
        let fraction_tile_pixel_offset_ul_y = ul_y_correction * y_factor;   
        // then we need to add the offset of the tile pixels relative to the dataset pixels
        // this is the offset of the tile pixels relative to the dataset pixels upper left corner
        let tile_pixel_offset_ul: GridIdx2D = GridIdx([fraction_tile_pixel_offset_ul_y.round() as isize, fraction_tile_pixel_offset_ul_x.round() as isize]) + pixel_offset_between_tile_and_data_in_query_res;
    
        debug!(
            "tile_pixel_offset_ul: {:?}, fraction_tile_pixel_offset_ul_y: {}, fraction_tile_pixel_offset_ul_x: {}",
            tile_pixel_offset_ul, fraction_tile_pixel_offset_ul_y, fraction_tile_pixel_offset_ul_x, 
        );
    
        // we also need to adapt the target pixel space read window to the clipped dataset intersection area
        // first we use the correction values to find out if we need to add padding by a fraction of a dataset pixel
        let tile_pixel_offset_lr_x = lr_x_correction * x_factor;
        let tile_pixel_offset_lr_y = lr_y_correction * y_factor;
        
        // this is the offset of the tile pixels relative to the dataset pixels lower right corner
        let tile_pixel_offset_lr: GridIdx2D = GridIdx([tile_pixel_offset_lr_y.round() as isize, tile_pixel_offset_lr_x.round() as isize]) + pixel_offset_between_tile_and_data_in_query_res;
    
        debug!(
            "tile_pixel_offset_lr: {:?}, tile_pixel_offset_lr_y: {}, tile_pixel_offset_lr_x: {}",
            tile_pixel_offset_lr, tile_pixel_offset_lr_y, tile_pixel_offset_lr_x
        );
    
        // now this is the grid we need to fill with the read window
        // TODO: we might also use "+" if we invert the output of the correction functions
        let corrected_intersection_area_in_query_res = GridBoundingBox2D::new_unchecked(
            intersection_area_in_query_res.min_index() - tile_pixel_offset_ul,
            intersection_area_in_query_res.max_index() - tile_pixel_offset_lr,
        );
    
        let is_ez_case = corrected_intersection_area_in_query_res == intersection_area_in_query_res && corrected_intersection_area_in_query_res.grid_shape() == tile_info.tile_size_in_pixels;
    
        let gdal_read_window = GdalReadWindow::new(
            dataset_intersection_area.min_index(),
            dataset_intersection_area.grid_shape(),
        );

        Some((gdal_read_window, corrected_intersection_area_in_query_res))   
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
        cache_hint: CacheHint,
    ) -> Result<RasterTile2D<T>> {
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
                        Self::load_tile_data(&ds, tile_information, tile_time, cache_hint)
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

    async fn load_tile_async<T: Pixel + GdalType + FromPrimitive>(
        dataset_params: Option<GdalDatasetParameters>,
        tile_information: TileInformation,
        tile_time: TimeInterval,
        cache_hint: CacheHint,
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
                Self::load_tile_data_async(ds, tile_information, tile_time, cache_hint).await
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
        tile_information: TileInformation,
        tile_time: TimeInterval,
        cache_hint: CacheHint,
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

        if let Err(error) = &dataset_result {
            let is_file_not_found = error_is_gdal_file_not_found(error);

            let err_result = match dataset_params.file_not_found_handling {
                FileNotFoundHandling::NoData if is_file_not_found => {
                    Ok(create_no_data_tile(tile_information, tile_time, cache_hint))
                }
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

        let result_tile = read_raster_tile_with_properties(
            &dataset,
            dataset_params,
            tile_information,
            tile_time,
            cache_hint,
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
        stream::iter(
            tiling_strategy
                .tile_information_iterator_from_grid_bounds(query.spatial_query.grid_bounds)
                .map(move |tile| {
                    GdalRasterLoader::load_tile_async(
                        info.params.clone(),
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

    async fn _query<'a>(
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

        let query_geo_transform = query.spatial_query().geo_transform;

        debug_assert_eq!(
            query_geo_transform.origin_coordinate, self.tiling_specification.origin_coordinate,
            "query origin does not match tiling origin",
        );

        if query_geo_transform.origin_coordinate != self.tiling_specification.origin_coordinate {
            debug!(
                "Query origin {:?} does not match tiling origin {:?}.",
                query_geo_transform.origin_coordinate, self.tiling_specification.origin_coordinate
            );
        }

        let result_descriptor = self.meta_data.result_descriptor().await?;

        // TODO: we now longer map all origins to the query origin. However, this requires a bit more logic
        let data_geo_transform = result_descriptor
            .geo_transform();

        let pixel_nearest_to_zero = data_geo_transform.nearest_pixel_to_zero(); // TODO: could also be nearest to anchor coordinate?
        let coordinate_nearest_to_zero =
            data_geo_transform.grid_idx_to_pixel_upper_left_coordinate_2d(pixel_nearest_to_zero);

        let pixel_distance_origin_from_nearest_zero = pixel_nearest_to_zero * -1;

        debug!(
            "pixel_distance_origin_from_nearest_zero: {:?}",
            pixel_distance_origin_from_nearest_zero
        );

        let tiling_spec = TilingSpecification {
            tile_size_in_pixels: self.tiling_specification.tile_size_in_pixels,
            origin_coordinate: coordinate_nearest_to_zero,
        };

        debug!("tiling_spec: {:?}", tiling_spec);

        // A `GeoTransform` maps pixel space to world space.
        // Usually a SRS has axis directions pointing "up" (y-axis) and "up" (y-axis).
        // We are not aware of spatial reference systems where the x-axis points to the right.
        // However, there are spatial reference systems where the y-axis points downwards.
        // The standard "pixel-space" starts at the top-left corner of a `GeoTransform` and points down-right.
        // Therefore, the pixel size on the x-axis is always increasing
        let pixel_size_x = query_geo_transform.x_pixel_size();
        debug_assert!(pixel_size_x.is_sign_positive());
        // and the y-axis should only be positive if the y-axis of the spatial reference system also "points down".
        // NOTE: at the moment we do not allow "down pointing" y-axis.
        let pixel_size_y = query_geo_transform.y_pixel_size();
        debug_assert!(pixel_size_y.is_sign_negative());

        let tiling_strategy =
            TilingStrategy::new_with_tiling_spec(tiling_spec, pixel_size_x, pixel_size_y);

        let mut empty = false;
        debug!("result descr shape: {:?}, result_desc geo_transform: {:?}", result_descriptor.pixel_bounds, result_descriptor.geo_transform);
        debug!("query bbox: {:?}", query.spatial_query);

        
        if !result_descriptor.spatial_bounds().intersects(&query.spatial_query().spatial_partition()) {
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
            tiling_strategy.tile_grid_box(query.spatial_query().spatial_partition()),
            tiling_strategy.geo_transform,
            tiling_strategy.tile_size_in_pixels,
            FillerTileCacheExpirationStrategy::DerivedFromSurroundingTiles,
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
        path: WorkflowOperatorPath,
        context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let data_id = context.resolve_named_data(&self.params.data).await?;
        let meta_data: GdalMetaData = context.meta_data(&data_id).await?;

        debug!("Initializing GdalSource for {:?}.", &self.params.data);
        debug!("GdalSource path: {:?}", path);

        let op = InitializedGdalSourceOperator {
            name: CanonicOperatorName::from(&self),
            result_descriptor: meta_data.result_descriptor().await?,
            meta_data,
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

    // check that the dataset pixel size is the same as the one we get from GDAL
    debug_assert_eq!(gdal_dataset_pixels_x, dataset_params.width);
    debug_assert_eq!(gdal_dataset_pixels_y, dataset_params.height);

    let raster_shape = GridShape2D::new([gdal_dataset_pixels_y, gdal_dataset_pixels_x]);

    // figure out if the y axis is flipped
    let is_y_axis_flipped = tile_info
        .global_geo_transform
        .y_pixel_size()
        .is_sign_negative()
        != gdal_dataset_geotransform.y_pixel_size.is_sign_negative();

    if is_y_axis_flipped {
        debug!("The GDAL data has a flipped y-axis. Need to unflip it!");
    }

    let Some((gdal_read_window, grid_bounds)) = gdal_dataset_geotransform.grid_bounds_resolution_to_read_window_and_target_grid(raster_shape, &tile_info) else {
        return Ok(GridOrEmpty::from(EmptyGrid::new(tile_info.tile_size_in_pixels)));
    };

    let is_ez_case = false;

    let result_grid = if is_ez_case {
        read_grid_from_raster(
            rasterband,
            &gdal_read_window,
            tile_info.tile_size_in_pixels,
            dataset_params,
            is_y_axis_flipped,
        )?
    } else {
        let r: GridOrEmpty<GridBoundingBox2D, T> = read_grid_from_raster(rasterband, &gdal_read_window, grid_bounds, dataset_params, is_y_axis_flipped)?;
        let mut tile_raster = GridOrEmpty::from(EmptyGrid::new(tile_info.global_pixel_bounds()));
        tile_raster.grid_blit_from(&r);
        tile_raster.unbounded()        
    };

    Ok(result_grid)
}

/// This method reads the data for a single tile with a specified size from the GDAL dataset and adds the requested metadata as properties to the tile.
fn read_raster_tile_with_properties<T: Pixel + gdal::raster::GdalType + FromPrimitive>(
    dataset: &GdalDataset,
    dataset_params: &GdalDatasetParameters,
    tile_info: TileInformation,
    tile_time: TimeInterval,
    cache_hint: CacheHint,
) -> Result<RasterTile2D<T>> {
    let rasterband = dataset.rasterband(dataset_params.rasterband_channel as isize)?;

    let result_grid = read_grid_and_handle_edges(tile_info, dataset, &rasterband, dataset_params)?;

    let mut properties = RasterProperties::default();

    // always read the scale and offset values from the rasterband
    properties_from_band(&mut properties, &rasterband);

    // read the properties from the dataset and rasterband metadata
    if let Some(properties_mapping) = dataset_params.properties_mapping.as_ref() {
        properties_from_gdal_metadata(&mut properties, dataset, properties_mapping);
        properties_from_gdal_metadata(&mut properties, &rasterband, properties_mapping);
    }

    // TODO: add cache_hint
    Ok(RasterTile2D::new_with_tile_info_and_properties(
        tile_time,
        tile_info,
        result_grid,
        properties,
        cache_hint,
    ))
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
        EmptyGrid::new(tile_info.tile_size_in_pixels).into(),
        RasterProperties::default(),
        cache_hint,
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
    use geoengine_datatypes::primitives::{AxisAlignedRectangle, SpatialPartition2D, TimeInstance};
    use geoengine_datatypes::raster::{
        EmptyGrid2D, GridBounds, GridIdx2D, TilesEqualIgnoringCacheHint,
    };
    use geoengine_datatypes::raster::{TileInformation, TilingStrategy};
    use geoengine_datatypes::util::gdal::hide_gdal_errors;
    use geoengine_datatypes::{primitives::SpatialResolution, raster::GridShape2D};
    use httptest::matchers::request;
    use httptest::{responders, Expectation, Server};

    async fn query_gdal_source(
        exe_ctx: &MockExecutionContext,
        query_ctx: &MockQueryContext,
        name: NamedData,
        output_shape: GridShape2D,
        output_bounds: SpatialPartition2D,
        time_interval: TimeInterval,
    ) -> Vec<Result<RasterTile2D<u8>>> {
        let op = GdalSource {
            params: GdalSourceParameters { data: name.clone() },
        }
        .boxed();

        let x_query_resolution = output_bounds.size_x() / output_shape.axis_size_x() as f64;
        let y_query_resolution = output_bounds.size_y() / output_shape.axis_size_y() as f64;
        let spatial_resolution =
            SpatialResolution::new_unchecked(x_query_resolution, y_query_resolution);

        let o = op
            .initialize(WorkflowOperatorPath::initialize_root(), exe_ctx)
            .await
            .unwrap();

        o.query_processor()
            .unwrap()
            .get_u8()
            .unwrap()
            .raster_query(
                RasterQueryRectangle::with_partition_and_resolution_and_origin(
                    output_bounds,
                    spatial_resolution,
                    exe_ctx.tiling_specification.origin_coordinate,
                    time_interval,
                ),
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
                retry: None,
            },
            TileInformation::with_partition_and_shape(output_bounds, output_shape),
            TimeInterval::default(),
            CacheHint::default(),
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
            cache_hint: _,
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
            cache_hint: _,
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

        let output_shape: GridShape2D = [256, 256].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());
        let time_interval = TimeInterval::new_unchecked(1_388_534_400_000, 1_388_534_400_001); // 2014-01-01

        let c = query_gdal_source(
            &exe_ctx,
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
            &exe_ctx,
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
            &exe_ctx,
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
            &exe_ctx,
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
            &exe_ctx,
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

        let tile = GdalRasterLoader::load_tile_async::<f64>(
            params,
            tile_info,
            time_interval,
            CacheHint::default(),
        )
        .await;

        assert!(tile.is_ok());

        let expected = RasterTile2D::<f64>::new_with_tile_info(
            time_interval,
            tile_info,
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
            retry: None,
        };

        let tile_information =
            TileInformation::with_partition_and_shape(output_bounds, output_shape);

        let RasterTile2D {
            global_geo_transform: _,
            grid_array: grid,
            tile_position: _,
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

    #[test]
    fn read_raster_and_offset_scale() {
        let output_shape: GridShape2D = [8, 8].into();
        let output_bounds =
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());

        let up_side_down_params = GdalDatasetParameters {
            file_path: test_data!(
                "raster/modis_ndvi/with_offset_scale/MOD13A2_M_NDVI_2014-01-01.TIFF"
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
            properties_mapping: None,
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

        assert_eq!(properties.offset_option(), Some(37.));
        assert_eq!(properties.scale_option(), Some(3.7));

        assert!(approx_eq!(f64, properties.offset(), 37.));
        assert!(approx_eq!(f64, properties.scale(), 3.7));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
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

        let tile_info = TileInformation {
            tile_size_in_pixels: [100, 100].into(),
            global_tile_position: [0, 0].into(),
            global_geo_transform: TestDefault::test_default(),
        };

        let tile_time = TimeInterval::default();

        // file doesn't exist => no data
        let result =
            GdalRasterLoader::load_tile_data::<u8>(&ds, tile_info, tile_time, CacheHint::default())
                .unwrap();
        assert!(matches!(result.grid_array, GridOrEmpty::Empty(_)));

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
            GdalRasterLoader::load_tile_data::<u8>(&ds, tile_info, tile_time, CacheHint::default());
        assert!(result.is_err());

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
        let result =
            GdalRasterLoader::load_tile_data::<u8>(&ds, tile_info, tile_time, CacheHint::default())
                .unwrap();
        assert!(matches!(result.grid_array, GridOrEmpty::Empty(_)));

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
        let result =
            GdalRasterLoader::load_tile_data::<u8>(&ds, tile_info, tile_time, CacheHint::default());
        assert!(result.is_err());
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
            tile_info,
            time_interval,
            CacheHint::seconds(1234),
        )
        .await;

        assert!(tile.is_ok());

        let expected = RasterTile2D::<f64>::new_with_tile_info(
            time_interval,
            tile_info,
            EmptyGrid2D::new(output_shape).into(),
            CacheHint::seconds(1234),
        );

        assert!(tile.unwrap().tiles_equal_ignoring_cache_hint(&expected));
    }

    #[test]
    fn gdal_geotransform_to_read_bounds() {
        let gdal_geo_transform: GdalDatasetGeoTransform = GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(0., 0.),
            x_pixel_size: 1.,
            y_pixel_size: -1.,
        };

        let gdal_data_size = GridShape2D::new([1024, 1024]);

        let ti: TileInformation = TileInformation::new(GridIdx([1,1]), GridShape2D::new([512,512]), GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.));

        let (read_window, target_bounds) = gdal_geo_transform.grid_bounds_resolution_to_read_window_and_target_grid(
            gdal_data_size,
            &ti,
        ).unwrap();

        assert_eq!(read_window, GdalReadWindow {
            read_size_x: 512,
            read_size_y: 512,
            read_start_x: 512,
            read_start_y: 512,
        });

        assert_eq!(target_bounds, GridBoundingBox2D::new(GridIdx([512,512]), GridIdx([1023,1023])).unwrap());
    }

    #[test]
    fn gdal_geotransform_to_read_bounds_half_res() {
        let gdal_geo_transform: GdalDatasetGeoTransform = GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(0., 0.),
            x_pixel_size: 1.,
            y_pixel_size: -1.,
        };

        let gdal_data_size = GridShape2D::new([1024, 1024]);

        let ti: TileInformation = TileInformation::new(GridIdx([0,0]), GridShape2D::new([512,512]), GeoTransform::new(Coordinate2D::new(0., 0.), 2., -2.));

        let (read_window, target_bounds) = gdal_geo_transform.grid_bounds_resolution_to_read_window_and_target_grid(
            gdal_data_size,
            &ti,
        ).unwrap();

        assert_eq!(read_window, GdalReadWindow {
            read_size_x: 1024,
            read_size_y: 1024,
            read_start_x: 0,
            read_start_y: 0,
        });

        assert_eq!(target_bounds, GridBoundingBox2D::new(GridIdx([0,0]), GridIdx([511,511])).unwrap());
    }

    #[test]
    fn gdal_geotransform_to_read_bounds_2x_res() {
        let gdal_geo_transform: GdalDatasetGeoTransform = GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(0., 0.),
            x_pixel_size: 1.,
            y_pixel_size: -1.,
        };

        let gdal_data_size = GridShape2D::new([1024, 1024]);

        let ti: TileInformation = TileInformation::new(GridIdx([0,0]), GridShape2D::new([512,512]), GeoTransform::new(Coordinate2D::new(0., 0.), 0.5, -0.5));

        let (read_window, target_bounds) = gdal_geo_transform.grid_bounds_resolution_to_read_window_and_target_grid(
            gdal_data_size,
            &ti,
        ).unwrap();

        assert_eq!(read_window, GdalReadWindow {
            read_size_x: 256,
            read_size_y: 256,
            read_start_x: 0,
            read_start_y: 0,
        });

        assert_eq!(target_bounds, GridBoundingBox2D::new(GridIdx([0,0]), GridIdx([511,511])).unwrap());
    }

    #[test]
    fn gdal_geotransform_to_read_bounds_ul_out() {
        let gdal_geo_transform: GdalDatasetGeoTransform = GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(-3., 3.),
            x_pixel_size: 1.,
            y_pixel_size: -1.,
        };

        let gdal_data_size = GridShape2D::new([1024, 1024]);
        let tile_grid_shape = GridShape2D::new([512,512]);
        let tiling_global_geo_transfom = GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.);

        let ti: TileInformation = TileInformation::new(GridIdx([0,0]), tile_grid_shape, tiling_global_geo_transfom);

        let (read_window, target_bounds) = gdal_geo_transform.grid_bounds_resolution_to_read_window_and_target_grid(
            gdal_data_size,
            &ti,
        ).unwrap();

        // since the origin of the tile is at -3,3 and the "coordinate nearest to zero" is 0,0 the tile at tile position 0,0 maps to the read window starting at 3,3 with 512x512 pixels
        assert_eq!(read_window, GdalReadWindow {
            read_size_x: 512,
            read_size_y: 512,
            read_start_x: 3,
            read_start_y: 3,
        });

        // the data maps to the complete tile
        assert_eq!(target_bounds, GridBoundingBox2D::new(GridIdx([0,0]), GridIdx([511,511])).unwrap());

        let ti: TileInformation = TileInformation::new(GridIdx([1,1]), tile_grid_shape, tiling_global_geo_transfom);

        let (read_window, target_bounds) = gdal_geo_transform.grid_bounds_resolution_to_read_window_and_target_grid(
            gdal_data_size,
            &ti,
        ).unwrap();

        // since the origin of the tile is at -3,3 and the "coordinate nearest to zero" is 0,0 the tile at tile position 1,1 maps to the read window starting at 515,515 (512+3, 512+3) with 512x512 pixels
        assert_eq!(read_window, GdalReadWindow {
            read_size_x: 509,
            read_size_y: 509,
            read_start_x: 515,
            read_start_y: 515,
        });

        // the data maps only to a part of the tile since the data is only 1024x1024 pixels in size. So the tile at tile position 1,1 maps to the data starting at 515,515 (512+3, 512+3) with 509x509 pixels left.
        assert_eq!(target_bounds, GridBoundingBox2D::new(GridIdx([512,512]), GridIdx([1020,1020])).unwrap());


    }

    #[test]
    fn gdal_geotransform_to_read_bounds_ul_in() {
        let gdal_geo_transform: GdalDatasetGeoTransform = GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(3., -3.),
            x_pixel_size: 1.,
            y_pixel_size: -1.,
        };

        let gdal_data_size = GridShape2D::new([1024, 1024]);
        let tile_grid_shape = GridShape2D::new([512,512]);
        let tiling_global_geo_transfom = GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.);

        let ti: TileInformation = TileInformation::new(GridIdx([0,0]), tile_grid_shape, tiling_global_geo_transfom);

        let (read_window, target_bounds) = gdal_geo_transform.grid_bounds_resolution_to_read_window_and_target_grid(
            gdal_data_size,
            &ti,
        ).unwrap();

        // in this case the data origin is at 3,-3 which is inside the tile at tile position 0,0. Since the tile starts at the "coordinate nearest to zero, which is 0.0,0.0" we need to read the data starting at data 0,0 with 509x509 pixels (512-3, 512-3).
        assert_eq!(read_window, GdalReadWindow {
            read_size_x: 509,
            read_size_y: 509,
            read_start_x: 0,
            read_start_y: 0,
        });

        // in this case, the data only maps to the last 509x509 pixels of the tile. So the data we read does not fill a whole tile.
        assert_eq!(target_bounds, GridBoundingBox2D::new(GridIdx([3,3]), GridIdx([511,511])).unwrap());

        let ti: TileInformation = TileInformation::new(GridIdx([1,1]), tile_grid_shape, tiling_global_geo_transfom);

        let (read_window, target_bounds) = gdal_geo_transform.grid_bounds_resolution_to_read_window_and_target_grid(
            gdal_data_size,
            &ti,
        ).unwrap();

        assert_eq!(read_window, GdalReadWindow {
            read_size_x: 512,
            read_size_y: 512,
            read_start_x: 509,
            read_start_y: 509,
        });

        assert_eq!(target_bounds, GridBoundingBox2D::new(GridIdx([512,512]), GridIdx([1023,1023])).unwrap());

        let ti: TileInformation = TileInformation::new(GridIdx([2,2]), tile_grid_shape, tiling_global_geo_transfom);

        let (read_window, target_bounds) = gdal_geo_transform.grid_bounds_resolution_to_read_window_and_target_grid(
            gdal_data_size,
            &ti,
        ).unwrap();

        assert_eq!(read_window, GdalReadWindow {
            read_size_x: 3,
            read_size_y: 3,
            read_start_x: 1021,
            read_start_y: 1021,
        });

        assert_eq!(target_bounds, GridBoundingBox2D::new(GridIdx([1024,1024]), GridIdx([1026,1026])).unwrap());
    }

    #[test]
    fn gdal_geotransform_to_read_bounds_ul_out_frac_res() {
        let gdal_geo_transform: GdalDatasetGeoTransform = GdalDatasetGeoTransform { 
            origin_coordinate: Coordinate2D::new(-9., 9.),
            x_pixel_size: 9.,
            y_pixel_size: -9.,
        };
        let gdal_data_size = GridShape2D::new([1024, 1024]);
        let tile_grid_shape = GridShape2D::new([512,512]);
        let tiling_global_geo_transfom = GeoTransform::new(Coordinate2D::new(-0., 0.), 3., -3.);

        let ti: TileInformation = TileInformation::new(GridIdx([0,0]), tile_grid_shape, tiling_global_geo_transfom);

        let (read_window, target_bounds) = gdal_geo_transform.grid_bounds_resolution_to_read_window_and_target_grid(
            gdal_data_size,
            &ti,
        ).unwrap();

        assert_eq!(read_window, GdalReadWindow {
            read_size_x: 170, // 
            read_size_y: 170,
            read_start_x: 1,
            read_start_y: 1,
        });

        assert_eq!(target_bounds, GridBoundingBox2D::new(GridIdx([0,0]), GridIdx([512,512])).unwrap()); // we need to read 683 pixels but we only want 682.6666666666666 pixels.

        let ti: TileInformation = TileInformation::new(GridIdx([1,1]), tile_grid_shape, tiling_global_geo_transfom);

        let (read_window, target_bounds) = gdal_geo_transform.grid_bounds_resolution_to_read_window_and_target_grid(
            gdal_data_size,
            &ti,
        ).unwrap();

        assert_eq!(read_window, GdalReadWindow {
            read_size_x: 171,
            read_size_y: 171,
            read_start_x: 171,
            read_start_y: 171,
        });

        assert_eq!(target_bounds, GridBoundingBox2D::new(GridIdx([510,510]), GridIdx([1025,1025])).unwrap());

        let ti: TileInformation = TileInformation::new(GridIdx([2,2]), tile_grid_shape, tiling_global_geo_transfom);

        let (read_window, target_bounds) = gdal_geo_transform.grid_bounds_resolution_to_read_window_and_target_grid(
            gdal_data_size,
            &ti,
        ).unwrap();

        assert_eq!(read_window, GdalReadWindow {
            read_size_x: 171,
            read_size_y: 171,
            read_start_x: 342,
            read_start_y: 342,
        });

        assert_eq!(target_bounds, GridBoundingBox2D::new(GridIdx([1023,1023]), GridIdx([1535,1535])).unwrap());


    }

}
