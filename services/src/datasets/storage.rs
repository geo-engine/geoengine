use crate::datasets::listing::DataSetProvider;
use crate::error::Result;
use crate::projects::project::LayerInfo;
use crate::users::user::UserId;
use crate::util::user_input::{UserInput, Validated};
use async_trait::async_trait;
use futures::stream::BoxStream;
use geoengine_datatypes::collections::{FeatureCollection, TypedFeatureCollection};
use geoengine_datatypes::dataset::{
    DataSetId, DataSetProviderId, InternalDataSetId, StagingDataSetId,
};
use geoengine_datatypes::identifiers::Identifier;
use geoengine_datatypes::primitives::Geometry;
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use geoengine_operators::engine::{RasterResultDescriptor, VectorResultDescriptor};
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
    pub source_operator: String, // TODO: enum?
}

impl From<AddDataSet> for DataSet {
    fn from(value: AddDataSet) -> Self {
        Self {
            id: DataSetId::Internal(InternalDataSetId::new()),
            name: value.name,
            description: value.description,
            data_type: value.data_type,
            source_operator: value.source_operator,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AddDataSet {
    pub name: String,
    pub description: String,
    pub data_type: LayerInfo,    // TODO: custom type?
    pub source_operator: String, // TODO: enum?
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
}

impl From<ImportDataSet> for DataSet {
    fn from(value: ImportDataSet) -> Self {
        DataSet {
            id: DataSetId::Internal(InternalDataSetId::new()),
            name: value.name,
            description: value.description,
            data_type: value.data_type,
            source_operator: value.source_operator,
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
pub trait DataSetDB: DataSetProvider + Send + Sync {
    /// Add raster data for `user` with `data_set_info` and `loading_info` without import
    async fn add_raster_data(
        &mut self,
        user: UserId,
        data_set_info: Validated<AddDataSet>,
        loading_info: RasterLoadingInfo,
        result: RasterResultDescriptor,
    ) -> Result<InternalDataSetId>;

    /// Add vector data for `user` with `data_set_info` and `loading_info` without import
    async fn add_vector_data(
        &mut self,
        user: UserId,
        data_set_info: Validated<AddDataSet>,
        loading_info: VectorLoadingInfo,
        result: VectorResultDescriptor,
    ) -> Result<InternalDataSetId>;

    /// Stage raster data for import for `user` and `loading_info`. Returns a `StagingDataSetId`
    async fn stage_raster_data(
        &mut self,
        user: UserId,
        loading_info: RasterLoadingInfo,
        result: RasterResultDescriptor,
    ) -> Result<StagingDataSetId>;

    /// Stage vector data for import for `user` and `loading_info`. Returns a `StagingDataSetId`
    async fn stage_vector_data(
        &mut self,
        user: UserId,
        loading_info: VectorLoadingInfo,
        result: VectorResultDescriptor,
    ) -> Result<StagingDataSetId>;

    // TODO: list staged data sets?

    /// Remove staged data for `user` and `data_set`
    // TODO: remove data or only loading info?
    async fn unstage_data(&mut self, user: UserId, data_set: StagingDataSetId) -> Result<()>;

    /// Import `data _set` from a `stream` of tiles (e.g. output of some operator) for `user`
    async fn import_raster_data<T: Pixel>(
        &mut self,
        user: UserId,
        data_set: Validated<ImportDataSet>,
        stream: BoxStream<'_, geoengine_operators::util::Result<RasterTile2D<T>>>,
        meta: RasterResultDescriptor,
    ) -> Result<InternalDataSetId>;

    /// Import `data _set` from a `stream` of tiles (e.g. output of some operator) for `user`
    async fn import_vector_data<G: Geometry>(
        &mut self,
        user: UserId,
        data_set: Validated<ImportDataSet>,
        stream: BoxStream<'_, geoengine_operators::util::Result<FeatureCollection<G>>>,
        meta: VectorResultDescriptor,
    ) -> Result<InternalDataSetId>
    where
        FeatureCollection<G>: Into<TypedFeatureCollection>; // TODO remove bound

    /// Add `permission` by `user` for `data_set`
    async fn add_data_set_permission(
        &mut self,
        data_set: InternalDataSetId,
        user: UserId,
        permission: DataSetPermission,
    ) -> Result<()>;

    /// Remove `permission` by `user` for `data_set`
    async fn remove_data_set_permission(
        &mut self,
        data_set: InternalDataSetId,
        user: UserId,
        permission: DataSetPermission,
    ) -> Result<()>;

    // TODO: update data set

    /// Add an external data set `provider` by `user`
    // TODO: require special privilege to be able to add external data set provider and to access external data in general
    async fn add_data_set_provider(
        &mut self,
        user: UserId,
        provider: Validated<AddDataSetProvider>,
    ) -> Result<DataSetProviderId>;

    // TODO: share data set provider/manage permissions

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
