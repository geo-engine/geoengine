use crate::{
    engine::RasterResultDescriptor,
    error::Error,
    source::{
        gdal_worker_process::{GdalDatasetParameters, GdalSourceTimePlaceholder},
        gdal_source::loading_info::{GdalMetaDataRegular, GdalMetadataNetCdfCf},
    },
};
use geoengine_datatypes::{
    delegate_from_to_sql,
    primitives::{CacheTtlSeconds, TimeInstance, TimeInterval, TimeStep},
};
use postgres_types::{FromSql, ToSql};

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

impl From<&GdalMetaDataRegular> for GdalMetaDataRegularDbType {
    fn from(other: &GdalMetaDataRegular) -> Self {
        Self {
            result_descriptor: other.result_descriptor.clone(),
            params: other.params.clone(),
            time_placeholders: other
                .time_placeholders
                .iter()
                .map(|(key, value)| TextGdalSourceTimePlaceholderKeyValue {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
            data_time: other.data_time,
            step: other.step,
            cache_ttl: other.cache_ttl,
        }
    }
}

impl TryFrom<GdalMetaDataRegularDbType> for GdalMetaDataRegular {
    type Error = Error;

    fn try_from(other: GdalMetaDataRegularDbType) -> Result<Self, Self::Error> {
        Ok(Self {
            result_descriptor: other.result_descriptor,
            params: other.params,
            time_placeholders: other
                .time_placeholders
                .iter()
                .map(|item| (item.key.clone(), item.value.clone()))
                .collect(),
            data_time: other.data_time,
            step: other.step,
            cache_ttl: other.cache_ttl,
        })
    }
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "GdalMetadataNetCdfCf")]
pub struct GdalMetadataNetCdfCfDbType {
    pub result_descriptor: RasterResultDescriptor,
    pub params: GdalDatasetParameters,
    pub start: TimeInstance,
    pub end: TimeInstance,
    pub step: TimeStep,
    pub band_offset: i64,
    pub cache_ttl: CacheTtlSeconds,
}

impl From<&GdalMetadataNetCdfCf> for GdalMetadataNetCdfCfDbType {
    fn from(other: &GdalMetadataNetCdfCf) -> Self {
        Self {
            result_descriptor: other.result_descriptor.clone(),
            params: other.params.clone(),
            start: other.start,
            end: other.end,
            step: other.step,
            band_offset: other.band_offset as i64,
            cache_ttl: other.cache_ttl,
        }
    }
}

impl TryFrom<GdalMetadataNetCdfCfDbType> for GdalMetadataNetCdfCf {
    type Error = Error;

    fn try_from(other: GdalMetadataNetCdfCfDbType) -> Result<Self, Self::Error> {
        Ok(Self {
            result_descriptor: other.result_descriptor,
            params: other.params,
            start: other.start,
            end: other.end,
            step: other.step,
            band_offset: other.band_offset as usize,
            cache_ttl: other.cache_ttl,
        })
    }
}

delegate_from_to_sql!(GdalMetadataNetCdfCf, GdalMetadataNetCdfCfDbType);
delegate_from_to_sql!(GdalMetaDataRegular, GdalMetaDataRegularDbType);
