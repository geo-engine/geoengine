use super::DatasetName;
use crate::datasets::storage::{validate_tags, Dataset};
use crate::error::Result;
use crate::projects::Symbology;
use crate::util::config::{get_config_element, DatasetService};
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataId, DatasetId};
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_operators::engine::{
    MetaDataProvider, RasterResultDescriptor, TypedResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use utoipa::{IntoParams, ToSchema};
use validator::{Validate, ValidationError};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatasetListing {
    pub id: DatasetId,
    pub name: DatasetName,
    pub display_name: String,
    pub description: String,
    pub tags: Vec<String>,
    pub source_operator: String,
    pub result_descriptor: TypedResultDescriptor,
    pub symbology: Option<Symbology>,
    // TODO: meta data like bounds, resolution
}

#[derive(Debug, Deserialize, Serialize, Clone, IntoParams, Validate)]
#[into_params(parameter_in = Query)]
pub struct DatasetListOptions {
    #[param(example = "Germany")]
    #[validate(length(min = 3, max = 256))]
    pub filter: Option<String>,
    #[param(example = "NameAsc")]
    pub order: OrderBy,
    #[param(example = 0)]
    pub offset: u32,
    #[param(example = 2)]
    #[validate(custom = "validate_list_limit")]
    pub limit: u32,
    #[param(example = "['tag1', 'tag2']")]
    #[validate(custom = "validate_tags")]
    pub tags: Option<Vec<String>>,
}

fn validate_list_limit(value: u32) -> Result<(), ValidationError> {
    let limit = get_config_element::<DatasetService>()
        .expect("should exist because it is defined in the default config")
        .list_limit;
    if value <= limit {
        return Ok(());
    }

    let mut err = ValidationError::new("limit (too large)");
    err.add_param::<u32>(Cow::Borrowed("max limit"), &limit);
    Err(err)
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSchema)]
pub enum OrderBy {
    NameAsc,
    NameDesc,
}

/// Listing of stored datasets
#[async_trait]
pub trait DatasetProvider: Send
    + Sync
    + MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    + MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
{
    async fn list_datasets(&self, options: DatasetListOptions) -> Result<Vec<DatasetListing>>;

    async fn load_dataset(&self, dataset: &DatasetId) -> Result<Dataset>;

    async fn load_provenance(&self, dataset: &DatasetId) -> Result<ProvenanceOutput>;

    async fn resolve_dataset_name_to_id(&self, name: &DatasetName) -> Result<Option<DatasetId>>;

    async fn dataset_autocomplete_search(
        &self,
        tags: Option<Vec<String>>,
        search_string: String,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<String>>;
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, ToSchema)]
pub struct ProvenanceOutput {
    pub data: DataId,
    pub provenance: Option<Vec<Provenance>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, ToSchema, ToSql, FromSql)]
pub struct Provenance {
    pub citation: String,
    pub license: String,
    pub uri: String,
}
