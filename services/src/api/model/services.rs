use crate::api::model::datatypes::DatasetId;
use crate::api::model::operators::{
    GdalMetaDataList, GdalMetaDataRegular, GdalMetaDataStatic, GdalMetadataNetCdfCf, MockMetaData,
    OgrMetaData,
};
use crate::datasets::listing::Provenance;
use crate::datasets::upload::{UploadId, VolumeName};
use crate::error::Result;
use crate::projects::Symbology;
use crate::util::user_input::UserInput;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(tag = "type")]
pub enum MetaDataDefinition {
    MockMetaData(MockMetaData),
    OgrMetaData(OgrMetaData),
    GdalMetaDataRegular(GdalMetaDataRegular),
    GdalStatic(GdalMetaDataStatic),
    GdalMetadataNetCdfCf(GdalMetadataNetCdfCf),
    GdalMetaDataList(GdalMetaDataList),
}

impl From<crate::datasets::storage::MetaDataDefinition> for MetaDataDefinition {
    fn from(value: crate::datasets::storage::MetaDataDefinition) -> Self {
        match value {
            crate::datasets::storage::MetaDataDefinition::MockMetaData(x) => {
                Self::MockMetaData(x.into())
            }
            crate::datasets::storage::MetaDataDefinition::OgrMetaData(x) => {
                Self::OgrMetaData(x.into())
            }
            crate::datasets::storage::MetaDataDefinition::GdalMetaDataRegular(x) => {
                Self::GdalMetaDataRegular(x.into())
            }
            crate::datasets::storage::MetaDataDefinition::GdalStatic(x) => {
                Self::GdalStatic(x.into())
            }
            crate::datasets::storage::MetaDataDefinition::GdalMetadataNetCdfCf(x) => {
                Self::GdalMetadataNetCdfCf(x.into())
            }
            crate::datasets::storage::MetaDataDefinition::GdalMetaDataList(x) => {
                Self::GdalMetaDataList(x.into())
            }
        }
    }
}

impl From<MetaDataDefinition> for crate::datasets::storage::MetaDataDefinition {
    fn from(value: MetaDataDefinition) -> Self {
        match value {
            MetaDataDefinition::MockMetaData(x) => Self::MockMetaData(x.into()),
            MetaDataDefinition::OgrMetaData(x) => Self::OgrMetaData(x.into()),
            MetaDataDefinition::GdalMetaDataRegular(x) => Self::GdalMetaDataRegular(x.into()),
            MetaDataDefinition::GdalStatic(x) => Self::GdalStatic(x.into()),
            MetaDataDefinition::GdalMetadataNetCdfCf(x) => Self::GdalMetadataNetCdfCf(x.into()),
            MetaDataDefinition::GdalMetaDataList(x) => Self::GdalMetaDataList(x.into()),
        }
    }
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetaDataSuggestion {
    pub main_file: String,
    pub meta_data: MetaDataDefinition,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AddDataset {
    pub id: Option<DatasetId>,
    pub name: String,
    pub description: String,
    pub source_operator: String,
    pub symbology: Option<Symbology>,
    pub provenance: Option<Vec<Provenance>>,
}

impl UserInput for AddDataset {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatasetDefinition {
    pub properties: AddDataset,
    pub meta_data: MetaDataDefinition,
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateDataset {
    pub data_path: DataPath,
    pub definition: DatasetDefinition,
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum DataPath {
    Volume(VolumeName),
    Upload(UploadId),
}
