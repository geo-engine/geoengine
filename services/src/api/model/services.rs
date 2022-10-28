use crate::api::model::operators::{
    GdalMetaDataList, GdalMetaDataRegular, GdalMetaDataStatic, GdalMetadataNetCdfCf, MockMetaData,
    OgrMetaData,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, ToSchema)]
#[serde(tag = "type")]
pub enum MetaDataDefinition {
    MockMetaData(MockMetaData),
    OgrMetaData(OgrMetaData),
    GdalMetaDataRegular(GdalMetaDataRegular),
    GdalStatic(GdalMetaDataStatic),
    GdalMetadataNetCdfCf(GdalMetadataNetCdfCf),
    GdalMetaDataList(GdalMetaDataList),
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetaDataSuggestion {
    pub main_file: String,
    pub meta_data: MetaDataDefinition,
}
