use super::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalMetadataMapping,
    GdalRetryOptions, GdalSourceTimePlaceholder,
};
use crate::{engine::RasterResultDescriptor, error::Error};
use geoengine_datatypes::{
    delegate_from_to_sql,
    primitives::{CacheTtlSeconds, TimeInstance, TimeInterval, TimeStep},
    util::StringPair,
};
use postgres_types::{FromSql, ToSql};

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "GdalDatasetParameters")]
pub struct GdalDatasetParametersDbType {
    pub file_path: String,
    pub rasterband_channel: i64,
    pub geo_transform: GdalDatasetGeoTransform,
    pub width: i64,
    pub height: i64,
    pub file_not_found_handling: FileNotFoundHandling,
    pub no_data_value: Option<f64>,
    pub properties_mapping: Option<Vec<GdalMetadataMapping>>,
    pub gdal_open_options: Option<Vec<String>>,
    pub gdal_config_options: Option<Vec<StringPair>>,
    pub allow_alphaband_as_mask: bool,
    pub retry: Option<GdalRetryOptions>,
}

impl From<&GdalDatasetParameters> for GdalDatasetParametersDbType {
    fn from(other: &GdalDatasetParameters) -> Self {
        Self {
            file_path: other.file_path.to_string_lossy().to_string(),
            rasterband_channel: other.rasterband_channel as i64,
            geo_transform: other.geo_transform,
            width: other.width as i64,
            height: other.height as i64,
            file_not_found_handling: other.file_not_found_handling,
            no_data_value: other.no_data_value,
            properties_mapping: other.properties_mapping.clone(),
            gdal_open_options: other.gdal_open_options.clone(),
            gdal_config_options: other
                .gdal_config_options
                .clone()
                .map(|v| v.into_iter().map(Into::into).collect()),
            allow_alphaband_as_mask: other.allow_alphaband_as_mask,
            retry: other.retry,
        }
    }
}

impl TryFrom<GdalDatasetParametersDbType> for GdalDatasetParameters {
    type Error = Error;

    fn try_from(other: GdalDatasetParametersDbType) -> Result<Self, Self::Error> {
        Ok(Self {
            file_path: other.file_path.into(),
            rasterband_channel: other.rasterband_channel as usize,
            geo_transform: other.geo_transform,
            width: other.width as usize,
            height: other.height as usize,
            file_not_found_handling: other.file_not_found_handling,
            no_data_value: other.no_data_value,
            properties_mapping: other.properties_mapping,
            gdal_open_options: other.gdal_open_options,
            gdal_config_options: other
                .gdal_config_options
                .map(|v| v.into_iter().map(Into::into).collect()),
            allow_alphaband_as_mask: other.allow_alphaband_as_mask,
            retry: other.retry,
        })
    }
}

#[derive(Debug, FromSql, ToSql)]
#[postgres(name = "GdalRetryOptions")]
pub struct GdalRetryOptionsDbType {
    pub max_retries: i64,
}

impl From<&GdalRetryOptions> for GdalRetryOptionsDbType {
    fn from(other: &GdalRetryOptions) -> Self {
        Self {
            max_retries: other.max_retries as i64,
        }
    }
}

impl TryFrom<GdalRetryOptionsDbType> for GdalRetryOptions {
    type Error = Error;

    fn try_from(other: GdalRetryOptionsDbType) -> Result<Self, Self::Error> {
        Ok(Self {
            max_retries: other.max_retries as usize,
        })
    }
}

#[derive(Debug, ToSql, FromSql, PartialEq)]
pub struct TextGdalSourceTimePlaceholderKeyValue {
    pub key: String,
    pub value: GdalSourceTimePlaceholder,
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "GdalMetaDataRegular")]
pub struct GdalMetaDataRegularDbType {
    pub result_descriptor: RasterResultDescriptor,
    pub params: GdalDatasetParameters,
    pub time_placeholders: Vec<TextGdalSourceTimePlaceholderKeyValue>,
    pub data_time: TimeInterval,
    pub step: TimeStep,
    pub cache_ttl: CacheTtlSeconds,
}

delegate_from_to_sql!(GdalDatasetParameters, GdalDatasetParametersDbType);
delegate_from_to_sql!(GdalRetryOptions, GdalRetryOptionsDbType);
