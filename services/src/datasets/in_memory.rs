use super::storage::DataSetDB;
use crate::datasets::listing::{DataSetListing, DataSetProvider};
use crate::datasets::storage::{DataSet, DataSetProviderListing, UserDataSetProviderPermission};
use crate::datasets::storage::{DataSetPermission, UserDataSetPermission};
use crate::error;
use crate::error::Error::UnknownDataSetProviderId;
use crate::error::Result;
use crate::users::user::UserId;
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataSetId, DataSetProviderId};
use geoengine_operators::engine::LoadingInfo;
use snafu::ensure;
use std::collections::HashMap;

#[derive(Default)]
pub struct HashmapDataSetDB {
    data_sets: HashMap<DataSetId, DataSet>,
    data_set_permissions: Vec<UserDataSetPermission>,
    external_providers: HashMap<DataSetProviderId, Box<dyn DataSetProvider>>,
    external_provider_permissions: Vec<UserDataSetProviderPermission>,
}

#[async_trait]
impl DataSetDB for HashmapDataSetDB {
    async fn add(&mut self, user: UserId, data_set: DataSet) -> Result<()> {
        self.data_set_permissions.push(UserDataSetPermission {
            user,
            data_set: data_set.id.clone(),
            permission: DataSetPermission::Owner,
        });
        self.data_sets.insert(data_set.id.clone(), data_set);
        Ok(())
    }

    async fn add_data_set_permission(
        &mut self,
        _data_set: DataSetId,
        _user: UserId,
        _permission: DataSetPermission,
    ) -> Result<()> {
        todo!()
    }

    async fn list_data_set_providers(&self, user: UserId) -> Result<Vec<DataSetProviderListing>> {
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
    async fn list(&self, user: UserId) -> Result<Vec<DataSetListing>> {
        Ok(self
            .data_set_permissions
            .iter()
            .filter_map(|p| {
                if p.user == user {
                    let d = self.data_sets.get(&p.data_set).unwrap();
                    Some(DataSetListing {
                        id: d.id.clone(),
                        name: d.name.clone(),
                        description: "".to_string(), // TODO
                        tags: vec![],                // TODO
                    })
                } else {
                    None
                }
            })
            .collect())
    }

    async fn loading_info(&self, _user: UserId, _data_set: DataSetId) -> Result<LoadingInfo> {
        todo!()
    }
}
