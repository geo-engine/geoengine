use super::storage::DataSetDB;
use crate::datasets::listing::{
    DataSetListOptions, DataSetListing, DataSetProvider, TypedDataSetProvider,
};
use crate::datasets::storage::{
    AddDataSet, AddDataSetProvider, DataSet, DataSetProviderListOptions, DataSetProviderListing,
    ImportDataSet, UserDataSetProviderPermission,
};
use crate::datasets::storage::{DataSetPermission, UserDataSetPermission};
use crate::error;
use crate::error::Error::UnknownDataSetProviderId;
use crate::error::Result;
use crate::users::user::UserId;
use crate::util::user_input::Validated;
use async_trait::async_trait;
use futures::stream::BoxStream;
use geoengine_datatypes::collections::FeatureCollection;
use geoengine_datatypes::dataset::{DataSetId, DataSetProviderId};
use geoengine_datatypes::identifiers::Identifier;
use geoengine_datatypes::primitives::Geometry;
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use geoengine_operators::engine::LoadingInfo;
use snafu::ensure;
use std::collections::HashMap;

// enum InMemoryData {
//     Raster(InMemoryRaster),
//     Points, // ...
// }
//
// struct InMemoryRaster {}

struct InMemoryDataSet {
    pub data_set: DataSet,
    // TODO: pub data: InMemoryData,
}

#[derive(Default)]
pub struct HashmapDataSetDB {
    data_sets: HashMap<DataSetId, InMemoryDataSet>,
    data_set_permissions: Vec<UserDataSetPermission>,
    external_providers: HashMap<DataSetProviderId, Box<dyn DataSetProvider>>,
    external_provider_permissions: Vec<UserDataSetProviderPermission>,
}

#[async_trait]
impl DataSetDB for HashmapDataSetDB {
    async fn add(&mut self, user: UserId, data_set: Validated<AddDataSet>) -> Result<DataSetId> {
        let data_set: DataSet = data_set.user_input.into();
        let id = data_set.id.clone();

        self.data_set_permissions.push(UserDataSetPermission {
            user,
            data_set: id.clone(),
            permission: DataSetPermission::Owner,
        });

        self.data_sets
            .insert(id.clone(), InMemoryDataSet { data_set });
        Ok(id)
    }

    async fn import_raster_data<T: Pixel>(
        &mut self,
        _user: UserId,
        _data_set: Validated<ImportDataSet>,
        _stream: BoxStream<'_, Result<RasterTile2D<T>>>,
    ) -> Result<DataSetId> {
        todo!()
    }

    async fn import_vector_data<G: Geometry>(
        &mut self,
        _user: UserId,
        _data_set: Validated<ImportDataSet>,
        _stream: BoxStream<'_, Result<FeatureCollection<G>>>,
    ) -> Result<DataSetId> {
        todo!()
    }

    async fn add_data_set_permission(
        &mut self,
        _data_set: DataSetId,
        _user: UserId,
        _permission: DataSetPermission,
    ) -> Result<()> {
        todo!()
    }

    async fn add_data_set_provider(
        &mut self,
        _user: UserId,
        provider: Validated<AddDataSetProvider>,
    ) -> Result<DataSetProviderId> {
        // TODO: check permission
        let id = DataSetProviderId::new();
        self.external_providers.insert(
            id,
            TypedDataSetProvider::new(provider.user_input).into_box(),
        );
        Ok(id)
    }

    async fn list_data_set_providers(
        &self,
        user: UserId,
        _options: Validated<DataSetProviderListOptions>,
    ) -> Result<Vec<DataSetProviderListing>> {
        // TODO: options
        Ok(self
            .external_provider_permissions
            .iter()
            .filter_map(|p| {
                if p.user == user {
                    // let provider = self.external_providers.get(&p.external_provider).unwrap();
                    Some(DataSetProviderListing {
                        // TODO
                        id: p.external_provider,
                        name: "".to_string(),
                        description: "".to_string(),
                    })
                } else {
                    None
                }
            })
            .collect())
    }

    async fn data_set_provider(
        &self,
        user: UserId,
        provider: DataSetProviderId,
    ) -> Result<&dyn DataSetProvider> {
        ensure!(
            self.external_provider_permissions
                .iter()
                .any(|p| p.user == user && p.external_provider == provider),
            error::DataSetListingProviderUnauthorized
        );

        self.external_providers
            .get(&provider)
            .map(AsRef::as_ref)
            .ok_or(UnknownDataSetProviderId)
    }
}

#[async_trait]
impl DataSetProvider for HashmapDataSetDB {
    async fn list(
        &self,
        user: UserId,
        _options: Validated<DataSetListOptions>,
    ) -> Result<Vec<DataSetListing>> {
        // TODO: use options
        Ok(self
            .data_set_permissions
            .iter()
            .filter_map(|p| {
                if p.user == user {
                    let d = self.data_sets.get(&p.data_set).unwrap();
                    Some(DataSetListing {
                        id: d.data_set.id.clone(),
                        name: d.data_set.name.clone(),
                        description: "".to_string(), // TODO
                        tags: vec![],                // TODO
                    })
                } else {
                    None
                }
            })
            .collect())
    }

    async fn loading_info(&self, user: UserId, data_set: DataSetId) -> Result<LoadingInfo> {
        ensure!(
            self.data_set_permissions
                .iter()
                .any(|p| p.user == user && p.data_set == data_set),
            error::DataSetPermissionDenied
        );

        let _data_set = self
            .data_sets
            .get(&data_set)
            .ok_or(error::Error::DataSetPermissionDenied)?; // TODO: custom error?

        // TODO: depending on layer type (raster/vector) return instructions to load from
        //       engines corresponding internal data store which does not yet exist
        todo!()
    }
}
