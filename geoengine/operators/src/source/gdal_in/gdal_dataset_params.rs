use crate::error::Error; // TODO: figure out more scoped errors!
use crate::util::Result; // TODO: use Result with more scoped errors.
use crate::util::input::float_option_with_nan;
use float_cmp::ApproxEq;
use geoengine_datatypes::{
    primitives::{Coordinate2D, DateTimeParseFormat, TimeInterval},
    raster::{
        GeoTransform, GridBoundingBox2D, GridShapeAccess, RasterPropertiesEntryType,
        RasterPropertiesKey, SpatialGridDefinition,
    },
    util::test::TestDefault,
};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    path::PathBuf,
};

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

    pub fn gdal_config_options_vsi_curl(&self) -> &'static [(&'static str, &'static str)] {
        &[
            ("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR"),
            ("CPL_VSIL_CURL_ALLOWED_EXTENSIONS", ".tif,.tiff,.jp2,.ovr"),
            ("CPL_VSIL_CURL_CHUNK_SIZE", "1048576"), // 1mb TODO: need to tune this!
            ("VSI_CACHE", "TRUE"),
            ("VSI_CACHE_SIZE", "67108864 "), // 64mb per worker! TODO: need to tune this!
        ]
    }

    pub fn is_vis_curl(&self) -> bool {
        self.file_path.starts_with("/vsicurl/") || self.file_path.starts_with("/vsis3/")
    }

    pub fn gdal_config_options_with_defaults(&self) -> Option<Vec<(String, String)>> {
        if !self.is_vis_curl() {
            return self.gdal_config_options.clone();
        }

        let mut opts = self.gdal_config_options.clone().unwrap_or(Vec::new());
        let opt_keys: Vec<&str> = opts.iter().map(|(k, _)| k.as_ref()).collect();

        let mut use_defaults: Vec<(String, String)> = self
            .gdal_config_options_vsi_curl()
            .iter()
            .filter(|(k, _)| !opt_keys.contains(k))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        opts.append(&mut use_defaults);

        if opts.is_empty() { None } else { Some(opts) }
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub enum TimeReference {
    Start,
    End,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub struct GdalSourceTimePlaceholder {
    pub format: DateTimeParseFormat,
    pub reference: TimeReference,
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
