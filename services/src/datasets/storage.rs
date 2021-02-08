use crate::datasets::listing::{DataSetListing, DataSetProvider};
use crate::error::Result;
use crate::projects::project::LayerInfo;
use crate::users::user::UserId;
use crate::util::user_input::{UserInput, Validated};
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataSetId, DataSetProviderId, InternalDataSetId};
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use geoengine_datatypes::util::Identifier;
use geoengine_operators::mock::MockDataSetDataSourceLoadingInfo;
use geoengine_operators::source::OgrSourceDataset;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataSet {
    pub id: DataSetId,
    pub name: String,
    pub description: String,
    pub data_type: LayerInfo, // TODO: custom type instead of reusing existing one?
    pub source_operator: String,
    pub spatial_reference: SpatialReferenceOption,
}

impl DataSet {
    pub fn listing(&self) -> DataSetListing {
        DataSetListing {
            id: self.id.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            tags: vec![], // TODO
            source_operator: self.source_operator.clone(),
            spatial_reference: self.spatial_reference,
        }
    }
}

impl From<AddDataSet> for DataSet {
    fn from(value: AddDataSet) -> Self {
        Self {
            id: DataSetId::Internal(InternalDataSetId::new()),
            name: value.name,
            description: value.description,
            data_type: value.data_type,
            source_operator: value.source_operator,
            spatial_reference: value.spatial_reference,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AddDataSet {
    pub name: String,
    pub description: String,
    pub data_type: LayerInfo, // TODO: custom type?
    pub source_operator: String,
    pub spatial_reference: SpatialReferenceOption,
}

impl UserInput for AddDataSet {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ImportDataSet {
    pub name: String,
    pub description: String,
    pub data_type: LayerInfo,
    pub source_operator: String,
    pub spatial_reference: SpatialReferenceOption,
}

impl From<ImportDataSet> for DataSet {
    fn from(value: ImportDataSet) -> Self {
        DataSet {
            id: DataSetId::Internal(InternalDataSetId::new()),
            name: value.name,
            description: value.description,
            data_type: value.data_type,
            source_operator: value.source_operator,
            spatial_reference: value.spatial_reference,
        }
    }
}

impl UserInput for ImportDataSet {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataSetProviderListing {
    pub id: DataSetProviderId,
    pub name: String,
    pub description: String,
    // more meta data (number of data sets, ...)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum AddDataSetProvider {
    AddMockDataSetProvider(AddMockDataSetProvider),
    // TODO: geo catalog, wcs, ...
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AddMockDataSetProvider {
    pub data_sets: Vec<DataSet>,
}

impl UserInput for AddDataSetProvider {
    fn validate(&self) -> Result<()> {
        todo!()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataSetProviderListOptions {
    // TODO: filter
    pub offset: u32,
    pub limit: u32,
}

impl UserInput for DataSetProviderListOptions {
    fn validate(&self) -> Result<()> {
        todo!()
    }
}

#[allow(clippy::large_enum_variant)] // TODO: box?
pub enum DataSetLoadingInfo {
    Raster(RasterLoadingInfo),
    Vector(VectorLoadingInfo),
}

pub struct GdalLoadingInfo {
    pub file: PathBuf,
}

pub type RasterLoadingInfo = GdalLoadingInfo;

#[allow(clippy::large_enum_variant)] // TODO: box?
pub enum VectorLoadingInfo {
    Mock(MockDataSetDataSourceLoadingInfo),
    Ogr(OgrSourceDataset),
}

/// Handling of data sets provided by geo engine internally, staged and by external providers
#[async_trait]
pub trait DataSetDB: DataSetProvider + DataSetProviderDB + Send + Sync {}

/// Storage and access of external data set providers
#[async_trait]
pub trait DataSetProviderDB {
    /// Add an external data set `provider` by `user`
    // TODO: require special privilege to be able to add external data set provider and to access external data in general
    async fn add_data_set_provider(
        &mut self,
        user: UserId,
        provider: Validated<AddDataSetProvider>,
    ) -> Result<DataSetProviderId>;

    /// List available providers for `user` filtered by `options`
    async fn list_data_set_providers(
        &self,
        user: UserId,
        options: Validated<DataSetProviderListOptions>,
    ) -> Result<Vec<DataSetProviderListing>>;

    /// Get data set `provider` for `user`
    async fn data_set_provider(
        &self,
        user: UserId,
        provider: DataSetProviderId,
    ) -> Result<&dyn DataSetProvider>;
}

/// Defines the type of meta data a `DataSetDB` is able to store
pub trait DataSetStorer: Send + Sync {
    type StorageType: Send + Sync;
}

/// Allow storage of meta data of a particular storage type, e.g. `HashMapStorable` meta data for
/// `HashMapDataSetDB`
#[async_trait]
pub trait DataSetStore: DataSetStorer {
    async fn add_data_set(
        &mut self,
        user: UserId,
        data_set: Validated<AddDataSet>,
        meta_data: Self::StorageType,
    ) -> Result<DataSetId>;
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub enum DataSetPermission {
    Read,
    Write,
    Owner,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct UserDataSetPermission {
    pub user: UserId,
    pub data_set: InternalDataSetId,
    pub permission: DataSetPermission,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub enum DataSetProviderPermission {
    Read,
    Write,
    Owner,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct UserDataSetProviderPermission {
    pub user: UserId,
    pub external_provider: DataSetProviderId,
    pub permission: DataSetProviderPermission,
}
