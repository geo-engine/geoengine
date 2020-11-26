use crate::datasets::listing::DataSetProvider;
use crate::error::Result;
use crate::projects::project::LayerInfo;
use crate::users::user::UserId;
use crate::util::user_input::{UserInput, Validated};
use async_trait::async_trait;
use futures::stream::BoxStream;
use geoengine_datatypes::collections::FeatureCollection;
use geoengine_datatypes::dataset::{DataSetId, DataSetProviderId, InternalDataSetId};
use geoengine_datatypes::identifiers::Identifier;
use geoengine_datatypes::primitives::Geometry;
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataSet {
    pub id: DataSetId,
    pub name: String,
    pub data_type: LayerInfo,    // TODO: custom type?
    pub source_operator: String, // TODO: enum?
}

impl From<AddDataSet> for DataSet {
    fn from(value: AddDataSet) -> Self {
        Self {
            id: DataSetId::Internal(InternalDataSetId::new()),
            name: value.name,
            data_type: value.data_type,
            source_operator: value.source_operator,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AddDataSet {
    pub name: String,
    pub data_type: LayerInfo,    // TODO: custom type?
    pub source_operator: String, // TODO: enum?
}

impl UserInput for AddDataSet {
    fn validate(&self) -> Result<()> {
        todo!()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ImportDataSet {
    pub name: String,
}

impl UserInput for ImportDataSet {
    fn validate(&self) -> Result<()> {
        todo!()
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

#[async_trait]
pub trait DataSetDB: DataSetProvider + Send + Sync {
    /// Add data set (meta data) pointing to existing data in database or on file system
    async fn add(&mut self, user: UserId, data_set: Validated<AddDataSet>) -> Result<DataSetId>;

    /// import data from a stream of tiles (e.g. output of some operator)
    async fn import_raster_data<T: Pixel>(
        &mut self,
        user: UserId,
        data_set: Validated<ImportDataSet>,
        stream: BoxStream<'_, Result<RasterTile2D<T>>>,
    ) -> Result<DataSetId>;

    /// import data from a stream of tiles (e.g. output of some operator)
    async fn import_vector_data<G: Geometry>(
        &mut self,
        user: UserId,
        data_set: Validated<ImportDataSet>,
        stream: BoxStream<'_, Result<FeatureCollection<G>>>,
    ) -> Result<DataSetId>;

    // TODO: delete?
    async fn add_data_set_permission(
        &mut self,
        data_set: DataSetId,
        user: UserId,
        permission: DataSetPermission,
    ) -> Result<()>;

    // TODO: update permissions

    // TODO: update data set

    // TODO: require special privilege to be able to add external data set provider and to access external data in general
    async fn add_data_set_provider(
        &mut self,
        user: UserId,
        provider: Validated<AddDataSetProvider>,
    ) -> Result<DataSetProviderId>;

    // TODO: share data set provider/manage permissions

    async fn list_data_set_providers(
        &self,
        user: UserId,
        options: Validated<DataSetProviderListOptions>,
    ) -> Result<Vec<DataSetProviderListing>>;

    async fn data_set_provider(
        &self,
        user: UserId,
        provider: DataSetProviderId,
    ) -> Result<&dyn DataSetProvider>;
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSql, FromSql)]
pub enum DataSetPermission {
    Read,
    Write,
    Owner,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct UserDataSetPermission {
    pub user: UserId,
    pub data_set: DataSetId,
    pub permission: DataSetPermission,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash, ToSql, FromSql)]
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
