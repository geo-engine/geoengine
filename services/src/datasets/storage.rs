use crate::contexts::Session;
use crate::datasets::listing::{DatasetListing, DatasetProvider, ExternalDatasetProvider};
use crate::datasets::upload::UploadDb;
use crate::datasets::upload::UploadId;
use crate::error;
use crate::error::Result;
use crate::projects::Symbology;
use crate::storage::{ListOption, Listable, Storable};
use crate::util::user_input::{UserInput, Validated};
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, InternalDatasetId};
use geoengine_datatypes::primitives::VectorQueryRectangle;
use geoengine_operators::engine::{MetaData, MetaDataLookupResult};
use geoengine_operators::source::{GdalMetaDataList, GdalMetadataNetCdfCf};
use geoengine_operators::{engine::StaticMetaData, source::OgrSourceDataset};
use geoengine_operators::{
    engine::TypedResultDescriptor, mock::MockDatasetDataSourceLoadingInfo,
    source::GdalMetaDataStatic,
};
use geoengine_operators::{engine::VectorResultDescriptor, source::GdalMetaDataRegular};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use std::fmt::Debug;

use super::listing::{DatasetListOptions, Provenance};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    pub name: String,
    pub description: String,
    pub result_descriptor: TypedResultDescriptor, // TODO: store separately like `MetaDataDefinition`
    pub source_operator: String,
    pub symbology: Option<Symbology>,
    pub provenance: Option<Provenance>,
}

impl Storable for Dataset {
    type Id = InternalDatasetId; // TODO: internal dataset id?
    type Item = Dataset;
    type ItemListing = DatasetListing;
    type ListOptions = DatasetListOptions;
}

impl Listable<Dataset> for Dataset {
    fn list(&self, id: &InternalDatasetId) -> DatasetListing {
        DatasetListing {
            id: (*id).into(),
            name: self.name.clone(),
            description: self.description.clone(),
            tags: vec![], // TODO
            source_operator: self.source_operator.clone(),
            result_descriptor: self.result_descriptor.clone(),
            symbology: self.symbology.clone(),
        }
    }
}

impl ListOption for DatasetListOptions {
    type Item = Dataset;

    fn offset(&self) -> u32 {
        self.offset
    }

    fn limit(&self) -> u32 {
        self.limit
    }

    fn compare_items(&self, a: &Dataset, b: &Dataset) -> std::cmp::Ordering {
        match self.order {
            super::listing::OrderBy::NameAsc => a.name.cmp(&b.name),
            super::listing::OrderBy::NameDesc => b.name.cmp(&a.name),
        }
    }

    fn retain(&self, item: &Self::Item) -> bool {
        // TODO
        true
    }
}

impl UserInput for Dataset {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AddDataset {
    pub id: Option<DatasetId>,
    pub name: String,
    pub description: String,
    pub source_operator: String,
    pub symbology: Option<Symbology>,
    pub provenance: Option<Provenance>,
}

impl UserInput for AddDataset {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DatasetProviderListing {
    pub id: DatasetProviderId,
    pub type_name: String,
    pub name: String,
    // more meta data (number of datasets, ...)
}

impl
    From<(
        DatasetProviderId,
        Box<dyn ExternalDatasetProviderDefinition>,
    )> for DatasetProviderListing
{
    fn from(
        v: (
            DatasetProviderId,
            Box<dyn ExternalDatasetProviderDefinition>,
        ),
    ) -> Self {
        Self {
            id: v.0,
            type_name: v.1.type_name(),
            name: v.1.name(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum AddDatasetProvider {
    AddMockDatasetProvider(AddMockDatasetProvider),
    // TODO: geo catalog, wcs, ...
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AddMockDatasetProvider {
    pub datasets: Vec<Dataset>,
}

impl UserInput for AddDatasetProvider {
    fn validate(&self) -> Result<()> {
        todo!()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatasetProviderListOptions {
    // TODO: filter
    pub offset: u32,
    pub limit: u32,
}

impl ListOption for DatasetProviderListOptions {
    type Item = Box<dyn ExternalDatasetProviderDefinition>;

    fn offset(&self) -> u32 {
        self.offset
    }

    fn limit(&self) -> u32 {
        self.limit
    }

    fn compare_items(&self, a: &Self::Item, b: &Self::Item) -> std::cmp::Ordering {
        // TODO
        std::cmp::Ordering::Equal
    }

    fn retain(&self, item: &Self::Item) -> bool {
        // TODO
        true
    }
}

impl UserInput for DatasetProviderListOptions {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DatasetDefinition {
    pub properties: Dataset,
    pub meta_data: MetaDataDefinition,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AddDatasetDefinition {
    pub properties: AddDataset,
    pub meta_data: MetaDataDefinition, // TODO: refactor into result_descriptor as separate field because every dataset needs it independently of the meta data
}

impl AddDatasetDefinition {
    // TODO: make sync again when result_descriptor is available as a field
    pub async fn dataset(&self) -> Result<Dataset> {
        Ok(Dataset {
            name: self.properties.name.clone(),
            description: self.properties.description.clone(),
            result_descriptor: self.meta_data.result_descriptor().await?,
            source_operator: self.properties.source_operator.clone(),
            symbology: self.properties.symbology.clone(),
            provenance: self.properties.provenance.clone(),
        })
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateDataset {
    pub upload: UploadId,
    pub definition: AddDatasetDefinition,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AutoCreateDataset {
    pub upload: UploadId,
    pub dataset_name: String,
    pub dataset_description: String,
    pub main_file: String,
}

impl UserInput for AutoCreateDataset {
    fn validate(&self) -> Result<()> {
        // TODO: more sophisticated input validation
        ensure!(!self.dataset_name.is_empty(), error::InvalidDatasetName);
        ensure!(
            !self.main_file.is_empty()
                && !self.main_file.contains('/')
                && !self.main_file.contains(".."),
            error::InvalidUploadFileName
        );

        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SuggestMetaData {
    pub upload: UploadId,
    pub main_file: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MetaDataSuggestion {
    pub main_file: String,
    pub meta_data: MetaDataDefinition,
}

#[allow(clippy::large_enum_variant)]
#[derive(PartialEq, Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum MetaDataDefinition {
    MockMetaData(
        StaticMetaData<
            MockDatasetDataSourceLoadingInfo,
            VectorResultDescriptor,
            VectorQueryRectangle,
        >,
    ),
    OgrMetaData(StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>),
    GdalMetaDataRegular(GdalMetaDataRegular),
    GdalStatic(GdalMetaDataStatic),
    GdalMetadataNetCdfCf(GdalMetadataNetCdfCf),
    GdalMetaDataList(GdalMetaDataList),
}

impl MetaDataDefinition {
    pub fn source_operator_type(&self) -> &str {
        match self {
            MetaDataDefinition::MockMetaData(_) => "MockDatasetDataSource",
            MetaDataDefinition::OgrMetaData(_) => "OgrSource",
            MetaDataDefinition::GdalMetaDataRegular(_)
            | MetaDataDefinition::GdalStatic(_)
            | MetaDataDefinition::GdalMetadataNetCdfCf(_)
            | MetaDataDefinition::GdalMetaDataList(_) => "GdalSource",
        }
    }

    pub async fn result_descriptor(&self) -> Result<TypedResultDescriptor> {
        match self {
            MetaDataDefinition::MockMetaData(m) => m
                .result_descriptor()
                .await
                .map(Into::into)
                .context(error::Operator),
            MetaDataDefinition::OgrMetaData(m) => m
                .result_descriptor()
                .await
                .map(Into::into)
                .context(error::Operator),
            MetaDataDefinition::GdalMetaDataRegular(m) => m
                .result_descriptor()
                .await
                .map(Into::into)
                .context(error::Operator),
            MetaDataDefinition::GdalStatic(m) => m
                .result_descriptor()
                .await
                .map(Into::into)
                .context(error::Operator),
            MetaDataDefinition::GdalMetadataNetCdfCf(m) => m
                .result_descriptor()
                .await
                .map(Into::into)
                .context(error::Operator),
            MetaDataDefinition::GdalMetaDataList(m) => m
                .result_descriptor()
                .await
                .map(Into::into)
                .context(error::Operator),
        }
    }
}

impl From<MetaDataDefinition> for MetaDataLookupResult {
    fn from(definition: MetaDataDefinition) -> Self {
        match definition {
            MetaDataDefinition::MockMetaData(m) => Self::Mock(Box::new(m)),
            MetaDataDefinition::OgrMetaData(m) => Self::Ogr(Box::new(m)),
            MetaDataDefinition::GdalMetaDataRegular(m) => Self::Gdal(Box::new(m)),
            MetaDataDefinition::GdalStatic(m) => Self::Gdal(Box::new(m)),
            MetaDataDefinition::GdalMetadataNetCdfCf(m) => Self::Gdal(Box::new(m)),
            MetaDataDefinition::GdalMetaDataList(m) => Self::Gdal(Box::new(m)),
        }
    }
}

impl UserInput for MetaDataDefinition {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

impl Storable for MetaDataDefinition {
    type Id = InternalDatasetId;
    type Item = MetaDataDefinition;
    type ItemListing = MetaDataDefinitionListing; // TODO
    type ListOptions = MetaDataDefinitionListOptions;
}

#[derive(Clone)]
pub struct MetaDataDefinitionListOptions {
    pub offset: u32,
    pub limit: u32,
}

impl ListOption for MetaDataDefinitionListOptions {
    type Item = MetaDataDefinition;

    fn offset(&self) -> u32 {
        self.offset
    }

    fn limit(&self) -> u32 {
        self.limit
    }

    fn compare_items(&self, a: &Self::Item, b: &Self::Item) -> std::cmp::Ordering {
        // TODO
        std::cmp::Ordering::Equal
    }

    fn retain(&self, item: &Self::Item) -> bool {
        // TODO
        true
    }
}

impl UserInput for MetaDataDefinitionListOptions {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

pub struct MetaDataDefinitionListing {}

impl Listable<MetaDataDefinition> for MetaDataDefinition {
    fn list(&self, id: &InternalDatasetId) -> MetaDataDefinitionListing {
        MetaDataDefinitionListing {}
    }
}

/// Handling of datasets provided by geo engine internally, staged and by external providers
#[async_trait]
pub trait DatasetDb<S: Session>:
    DatasetStore<S> + DatasetProvider<S> + DatasetProviderDb<S> + UploadDb<S> + Send + Sync
{
}

/// Storage and access of external dataset providers
#[async_trait]
pub trait DatasetProviderDb<S: Session> {
    /// Add an external dataset `provider` by `user`
    // TODO: require special privilege to be able to add external dataset provider and to access external data in general
    async fn add_dataset_provider(
        &mut self,
        session: &S,
        provider: Box<dyn ExternalDatasetProviderDefinition>,
    ) -> Result<DatasetProviderId>;

    /// List available providers for `user` filtered by `options`
    async fn list_dataset_providers(
        &self,
        session: &S,
        options: Validated<DatasetProviderListOptions>,
    ) -> Result<Vec<DatasetProviderListing>>;

    /// Get dataset `provider` for `user`
    async fn dataset_provider(
        &self,
        session: &S,
        provider: DatasetProviderId,
    ) -> Result<Box<dyn ExternalDatasetProvider>>;
}

/// Defines the type of meta data a `DatasetDB` is able to store
pub trait DatasetStorer: Send + Sync {
    type StorageType: Send + Sync;
}

/// Allow storage of meta data of a particular storage type, e.g. `HashMapStorable` meta data for
/// `HashMapDatasetDB`
#[async_trait]
pub trait DatasetStore<S: Session>: DatasetStorer {
    async fn add_dataset(
        &mut self,
        session: &S,
        dataset: Validated<Dataset>,
        meta_data: Self::StorageType,
    ) -> Result<DatasetId>;

    /// turn given `meta` data definition into the corresponding `StorageType` for the `DatasetStore`
    /// for use in the `add_dataset` method
    fn wrap_meta_data(&self, meta: MetaDataDefinition) -> Self::StorageType;
}

#[typetag::serde(tag = "type")]
#[async_trait]
pub trait ExternalDatasetProviderDefinition:
    CloneableDatasetProviderDefinition + Send + Sync + std::fmt::Debug
{
    /// create the actual provider for data listing and access
    async fn initialize(self: Box<Self>) -> Result<Box<dyn ExternalDatasetProvider>>;

    /// the type of the provider
    fn type_name(&self) -> String;

    /// name of the external data source
    fn name(&self) -> String;

    /// id of the provider
    fn id(&self) -> DatasetProviderId;
}

pub trait CloneableDatasetProviderDefinition {
    fn clone_boxed_provider(&self) -> Box<dyn ExternalDatasetProviderDefinition>;
}

impl<T> CloneableDatasetProviderDefinition for T
where
    T: 'static + ExternalDatasetProviderDefinition + Clone,
{
    fn clone_boxed_provider(&self) -> Box<dyn ExternalDatasetProviderDefinition> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn ExternalDatasetProviderDefinition> {
    fn clone(&self) -> Box<dyn ExternalDatasetProviderDefinition> {
        self.clone_boxed_provider()
    }
}

impl UserInput for Box<dyn ExternalDatasetProviderDefinition> {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

impl<T> UserInput for Box<T>
where
    T: ExternalDatasetProviderDefinition + Clone,
{
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

impl Storable for Box<dyn ExternalDatasetProviderDefinition> {
    type Id = DatasetProviderId;
    type Item = Box<dyn ExternalDatasetProviderDefinition>;
    type ItemListing = DatasetProviderListing;
    type ListOptions = DatasetProviderListOptions;
}

impl Listable<Box<dyn ExternalDatasetProviderDefinition>>
    for Box<dyn ExternalDatasetProviderDefinition>
{
    fn list(&self, id: &DatasetProviderId) -> DatasetProviderListing {
        DatasetProviderListing {
            id: *id,
            name: self.name(),
            type_name: self.type_name(),
        }
    }
}
