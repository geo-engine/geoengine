use std::path::PathBuf;

use crate::api::model::operators::{
    GdalMetaDataList, GdalMetaDataRegular, GdalMetaDataStatic, GdalMetadataNetCdfCf, MockMetaData,
    OgrMetaData,
};
use crate::datasets::DatasetName;
use crate::datasets::storage::validate_tags;
use crate::datasets::upload::{UploadId, VolumeName};
use crate::projects::Symbology;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use validator::{Validate, ValidationErrors};

use super::datatypes::DataId;

#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug, Clone, ToSchema, PartialEq)]
#[serde(untagged)]
#[schema(discriminator = "type")]
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
                Self::MockMetaData(MockMetaData {
                    r#type: Default::default(),
                    loading_info: x.loading_info.into(),
                    result_descriptor: x.result_descriptor.into(),
                })
            }
            crate::datasets::storage::MetaDataDefinition::OgrMetaData(x) => {
                Self::OgrMetaData(OgrMetaData {
                    r#type: Default::default(),
                    loading_info: x.loading_info.into(),
                    result_descriptor: x.result_descriptor.into(),
                })
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
    pub layer_name: String,
    pub meta_data: MetaDataDefinition,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
// TODO: validate user input
pub struct AddDataset {
    pub name: Option<DatasetName>,
    pub display_name: String,
    pub description: String,
    pub source_operator: String,
    pub symbology: Option<Symbology>,
    pub provenance: Option<Vec<Provenance>>,
    pub tags: Option<Vec<String>>,
}

impl From<AddDataset> for crate::datasets::storage::AddDataset {
    fn from(value: AddDataset) -> Self {
        Self {
            name: value.name,
            display_name: value.display_name,
            description: value.description,
            source_operator: value.source_operator,
            symbology: value.symbology,
            provenance: value
                .provenance
                .map(|v| v.into_iter().map(Into::into).collect()),
            tags: value.tags,
        }
    }
}

impl From<crate::datasets::storage::AddDataset> for AddDataset {
    fn from(value: crate::datasets::storage::AddDataset) -> Self {
        Self {
            name: value.name,
            display_name: value.display_name,
            description: value.description,
            source_operator: value.source_operator,
            symbology: value.symbology,
            provenance: value
                .provenance
                .map(|v| v.into_iter().map(Into::into).collect()),
            tags: value.tags,
        }
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

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema, Validate)]
pub struct UpdateDataset {
    pub name: DatasetName,
    #[validate(length(min = 1))]
    pub display_name: String,
    pub description: String,
    #[validate(custom(function = "validate_tags"))]
    pub tags: Vec<String>,
}

#[derive(Deserialize, Serialize, PartialEq, Eq, Debug, Clone, ToSchema, Validate)]
pub struct Provenance {
    #[validate(length(min = 1))]
    pub citation: String,
    #[validate(length(min = 1))]
    pub license: String,
    #[validate(length(min = 1))]
    pub uri: String,
}

impl From<Provenance> for crate::datasets::listing::Provenance {
    fn from(value: Provenance) -> Self {
        Self {
            citation: value.citation,
            license: value.license,
            uri: value.uri,
        }
    }
}

impl From<crate::datasets::listing::Provenance> for Provenance {
    fn from(value: crate::datasets::listing::Provenance) -> Self {
        Self {
            citation: value.citation,
            license: value.license,
            uri: value.uri,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct Provenances {
    pub provenances: Vec<Provenance>,
}

impl Validate for Provenances {
    fn validate(&self) -> Result<(), ValidationErrors> {
        for provenance in &self.provenances {
            provenance.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ProvenanceOutput {
    pub data: DataId,
    pub provenance: Option<Vec<Provenance>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct Volume {
    pub name: String,
    pub path: Option<String>,
}

impl From<&Volume> for crate::datasets::upload::Volume {
    fn from(value: &Volume) -> Self {
        Self {
            name: VolumeName(value.name.clone()),
            path: value.path.as_ref().map_or_else(PathBuf::new, PathBuf::from),
        }
    }
}
