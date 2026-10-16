use super::loading_info::{GdalMdMetaData, MdVariable};
use crate::engine::RasterResultDescriptor;
use geoengine_datatypes::{delegate_from_to_sql, primitives::CacheTtlSeconds};
use postgres_types::{FromSql, Json, ToSql};

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "GdalMdMetaData")]
pub struct GdalMdMetaDataDbType {
    pub result_descriptor: RasterResultDescriptor,
    pub variables: Json<Vec<MdVariable>>,
    pub wrap: bool,
    pub cache_ttl: Option<CacheTtlSeconds>,
}

impl From<&GdalMdMetaData> for GdalMdMetaDataDbType {
    fn from(other: &GdalMdMetaData) -> Self {
        Self {
            result_descriptor: other.result_descriptor.clone(),
            variables: Json(other.variables.clone()),
            wrap: other.wrap,
            cache_ttl: other.cache_ttl,
        }
    }
}

impl From<GdalMdMetaDataDbType> for GdalMdMetaData {
    fn from(other: GdalMdMetaDataDbType) -> Self {
        Self {
            result_descriptor: other.result_descriptor,
            variables: other.variables.0,
            wrap: other.wrap,
            cache_ttl: other.cache_ttl,
        }
    }
}

delegate_from_to_sql!(GdalMdMetaData, GdalMdMetaDataDbType);
