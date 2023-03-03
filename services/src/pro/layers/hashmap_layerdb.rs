use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, StreamExt};
use geoengine_datatypes::primitives::{DateTime, Duration};
use openidconnect::SubjectIdentifier;
use pwhash::bcrypt;
use snafu::ensure;

use crate::contexts::{Db, SessionId};
use crate::error::{self, Error, Result};
use crate::layers::add_from_directory::UNSORTED_COLLECTION_ID;
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{
    AddLayer, AddLayerCollection, CollectionItem, Layer, LayerCollection,
    LayerCollectionListOptions,
};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use crate::layers::storage::{
    HashMapLayerDbBackend, LayerDb, LayerProviderDb, LayerProviderListing,
    LayerProviderListingOptions, INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
};
use crate::pro::contexts::ProInMemoryDb;
use crate::pro::permissions::{Permission, PermissionDb, Role, RoleId};
use crate::pro::users::{
    User, UserCredentials, UserDb, UserId, UserInfo, UserRegistration, UserSession,
};
use crate::projects::{ProjectId, STRectangle};
use crate::util::user_input::Validated;
use geoengine_datatypes::util::Identifier;

use std::cmp::Ordering;

use crate::api::model::datatypes::{DataProviderId, LayerId};
use crate::util::user_input::UserInput;
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, RwLockWriteGuard};
use uuid::Uuid;

// pub struct ProHashMapLayerDbBackend {
//     layer_db: HashMapLayerDb,
//     layer_permissions: Vec<LayerPermission>,
//     // collection_permissions: Vec<LayerCollectionPermission>,
// }

// impl Default for ProHashMapLayerDbBackend {
//     fn default() -> Self {
//         Self {
//             layer_db: HashMapLayerDb::default(),
//             layer_permissions: vec![],
//             // collection_permissions: vec![
//             //     LayerCollectionPermission {
//             //         collection: LayerCollectionId(INTERNAL_LAYER_DB_ROOT_COLLECTION_ID.to_string()),
//             //         role: Role::system_role_id(),
//             //         permission: Permission::Owner,
//             //     },
//             //     LayerCollectionPermission {
//             //         collection: LayerCollectionId(UNSORTED_COLLECTION_ID.to_string()),
//             //         role: Role::system_role_id(),
//             //         permission: Permission::Owner,
//             //     },
//             // ],
//         }
//     }
// }

// // TODO: generify over id?
// #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
// struct LayerPermission {
//     layer: LayerId,
//     role: RoleId,
//     permission: Permission,
// }

// #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
// struct LayerCollectionPermission {
//     collection: LayerCollectionId,
//     role: RoleId,
//     permission: Permission,
// }

// pub struct ProHashMapLayerDb {
//     backend: Db<ProHashMapLayerDbBackend>,
//     session: UserSession,
// }

// impl ProHashMapLayerDb {
//     pub fn new(backend: Db<ProHashMapLayerDbBackend>, session: UserSession) -> Self {
//         Self { backend, session }
//     }

//     pub fn system(backend: Db<ProHashMapLayerDbBackend>) -> Self {
//         Self {
//             backend,
//             session: UserSession::system_session(),
//         }
//     }
// }

#[async_trait]
impl LayerDb for ProInMemoryDb {
    async fn add_layer(
        &self,
        layer: Validated<AddLayer>,
        collection: &LayerCollectionId,
    ) -> Result<LayerId> {
        ensure!(
            self.has_permission(collection.clone(), Permission::Owner)
                .await?,
            error::PermissionDenied
        );

        let backend = &self.backend.layer_db;

        let layer_id = backend.add_layer(layer, collection).await?;

        self.add_permission(
            self.session.user.id.into(),
            layer_id.clone(),
            Permission::Owner,
        )
        .await?;

        Ok(layer_id)
    }

    async fn add_layer_with_id(
        &self,
        id: &LayerId,
        layer: Validated<AddLayer>,
        collection: &LayerCollectionId,
    ) -> Result<()> {
        ensure!(
            self.has_permission(collection.clone(), Permission::Owner)
                .await?,
            error::PermissionDenied
        );

        let mut backend = &self.backend.layer_db;

        backend.add_layer_with_id(id, layer, collection).await?;

        self.add_permission(self.session.user.id.into(), id.clone(), Permission::Owner)
            .await?;

        Ok(())
    }

    async fn add_layer_to_collection(
        &self,
        layer: &LayerId,
        collection: &LayerCollectionId,
    ) -> Result<()> {
        ensure!(
            self.has_permission(collection.clone(), Permission::Owner)
                .await?,
            error::PermissionDenied
        );

        let backend = &self.backend.layer_db;

        backend.add_layer_to_collection(layer, collection).await;

        self.add_permission(
            self.session.user.id.into(),
            layer.clone(),
            Permission::Owner,
        )
        .await?;

        Ok(())
    }

    async fn add_layer_collection(
        &self,
        collection: Validated<AddLayerCollection>,
        parent: &LayerCollectionId,
    ) -> Result<LayerCollectionId> {
        ensure!(
            self.has_permission(parent.clone(), Permission::Owner)
                .await?,
            error::PermissionDenied
        );

        let backend = &self.backend.layer_db;

        let collection_id = backend
            .add_layer_collection(collection.clone(), parent)
            .await?;

        self.add_permission(
            self.session.user.id.into(),
            collection_id.clone(),
            Permission::Owner,
        )
        .await?;

        Ok(collection_id)
    }

    async fn add_collection_with_id(
        &self,
        id: &LayerCollectionId,
        collection: Validated<AddLayerCollection>,
        parent: &LayerCollectionId,
    ) -> Result<()> {
        ensure!(
            self.has_permission(parent.clone(), Permission::Owner)
                .await?,
            error::PermissionDenied
        );

        let mut backend = &self.backend.layer_db;

        backend
            .add_collection_with_id(id, collection, parent)
            .await?;

        self.add_permission(self.session.user.id.into(), id.clone(), Permission::Owner)
            .await?;

        Ok(())
    }

    async fn add_collection_to_parent(
        &self,
        collection: &LayerCollectionId,
        parent: &LayerCollectionId,
    ) -> Result<()> {
        ensure!(
            self.has_permission(parent.clone(), Permission::Owner)
                .await?,
            error::PermissionDenied
        );

        let mut backend = &self.backend.layer_db;

        backend.add_collection_to_parent(collection, parent).await;

        Ok(())
    }

    async fn remove_layer_collection(&self, collection: &LayerCollectionId) -> Result<()> {
        ensure!(
            self.has_permission(collection.clone(), Permission::Owner)
                .await?,
            error::PermissionDenied
        );

        let mut backend = &self.backend.layer_db;

        backend.remove_layer_collection(collection).await?;

        // TODO: clean up permissions, also of children (orphaned, without matching collection)

        Ok(())
    }

    async fn remove_layer_collection_from_parent(
        &self,
        collection: &LayerCollectionId,
        parent: &LayerCollectionId,
    ) -> Result<()> {
        ensure!(
            self.has_permission(parent.clone(), Permission::Owner)
                .await?,
            error::PermissionDenied
        );

        ensure!(
            self.has_permission(collection.clone(), Permission::Owner)
                .await?,
            error::PermissionDenied
        );

        let mut backend = &self.backend.layer_db;

        backend
            .remove_layer_collection_from_parent(collection, parent)
            .await?;

        // TODO: clean up permissions, also of children (orphaned, without matching collection)

        Ok(())
    }

    async fn remove_layer_from_collection(
        &self,
        layer_id: &LayerId,
        collection_id: &LayerCollectionId,
    ) -> Result<()> {
        ensure!(
            self.has_permission(layer_id.clone(), Permission::Owner)
                .await?,
            error::PermissionDenied
        );

        ensure!(
            self.has_permission(collection_id.clone(), Permission::Owner)
                .await?,
            error::PermissionDenied
        );

        let mut backend = &self.backend.layer_db;

        backend
            .remove_layer_from_collection(layer_id, collection_id)
            .await?;

        // TODO: clean up permissions, also of children (orphaned, without matching collection)

        Ok(())
    }
}

#[async_trait]
impl LayerCollectionProvider for ProInMemoryDb {
    async fn load_layer_collection(
        &self,
        collection_id: &LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<LayerCollection> {
        // TODO: permissions

        let backend = &self.backend.layer_db;

        let options = options.user_input;

        // filter collection items by read/owner permissions
        let mut collection = backend
            .load_layer_collection(
                collection_id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: u32::MAX,
                }
                .validated()
                .expect("should be valid because of the selection of offset and limit"),
            )
            .await?;

        let items = stream::iter(collection.items)
            .filter_map(|i| async move {
                let keep = match &i {
                    CollectionItem::Layer(l) => self
                        .has_permission(l.id.layer_id.clone(), Permission::Read)
                        .await
                        .unwrap_or(false),
                    CollectionItem::Collection(c) => self
                        .has_permission(c.id.collection_id.clone(), Permission::Read)
                        .await
                        .unwrap_or(false),
                };

                if keep {
                    Some(i)
                } else {
                    None
                }
            })
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .collect()
            .await;

        collection.items = items;

        Ok(collection)
    }

    async fn get_root_layer_collection_id(&self) -> Result<LayerCollectionId> {
        self.backend.layer_db.get_root_layer_collection_id().await
    }

    async fn load_layer(&self, id: &LayerId) -> Result<Layer> {
        ensure!(
            self.has_permission(id.clone(), Permission::Read).await?,
            error::PermissionDenied
        );

        self.backend.layer_db.load_layer(id).await
    }
}

#[async_trait]
impl LayerProviderDb for ProInMemoryDb {
    async fn add_layer_provider(
        &self,
        provider: Box<dyn DataProviderDefinition>,
    ) -> Result<DataProviderId> {
        let id = provider.id();

        self.backend
            .layer_provider_db
            .write()
            .await
            .external_providers
            .insert(id, provider);

        Ok(id)
    }

    async fn list_layer_providers(
        &self,
        options: Validated<LayerProviderListingOptions>,
    ) -> Result<Vec<LayerProviderListing>> {
        let options = options.user_input;

        let mut listing = self
            .backend
            .layer_provider_db
            .read()
            .await
            .external_providers
            .iter()
            .map(|(id, provider)| LayerProviderListing {
                id: *id,
                name: provider.name(),
                description: provider.type_name().to_string(),
            })
            .collect::<Vec<_>>();

        // TODO: sort option
        listing.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(listing
            .into_iter()
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .collect())
    }

    async fn load_layer_provider(&self, id: DataProviderId) -> Result<Box<dyn DataProvider>> {
        self.backend
            .layer_provider_db
            .read()
            .await
            .external_providers
            .get(&id)
            .cloned()
            .ok_or(Error::UnknownProviderId)?
            .initialize()
            .await
    }
}

// #[async_trait]
// impl PermissionDb<LayerId> for ProHashMapLayerDb {
//     async fn add_permission(
//         &self,
//         role: &RoleId,
//         resource: &LayerId,
//         permission: Permission,
//     ) -> Result<()> {
//         let mut backend = self.backend.write().await;

//         // check owner permission on layer
//         ensure!(
//             backend
//                 .layer_permissions
//                 .iter()
//                 .any(|p| p.layer == *resource
//                     && self.session.roles.iter().any(|r| r == &p.role)
//                     && p.permission == Permission::Owner),
//             error::PermissionDenied
//         );

//         // TODO: check if permission already exists

//         backend.layer_permissions.push(LayerPermission {
//             layer: resource.clone(),
//             role: *role,
//             permission,
//         });

//         Ok(())
//     }

//     async fn remove_permission(
//         &self,
//         role: &RoleId,
//         resource: &LayerId,
//         permission: Permission,
//     ) -> Result<()> {
//         let mut backend = self.backend.write().await;

//         // check owner permission on layer
//         ensure!(
//             backend
//                 .layer_permissions
//                 .iter()
//                 .any(|p| p.layer == *resource
//                     && self.session.roles.iter().any(|r| r == &p.role)
//                     && p.permission == Permission::Owner),
//             error::PermissionDenied
//         );

//         // TODO: check if permission already exists

//         let layer_permission = LayerPermission {
//             layer: resource.clone(),
//             role: *role,
//             permission,
//         };

//         backend.layer_permissions.retain(|p| p != &layer_permission);

//         Ok(())
//     }
// }

// #[async_trait]
// impl PermissionDb<LayerCollectionId> for ProHashMapLayerDb {
//     async fn add_permission(
//         &self,
//         role: &RoleId,
//         resource: &LayerCollectionId,
//         permission: Permission,
//     ) -> Result<()> {
//         let mut backend = self.backend.write().await;

//         // check owner permission on collection
//         ensure!(
//             backend
//                 .collection_permissions
//                 .iter()
//                 .any(|p| p.collection == *resource
//                     && self.session.roles.iter().any(|r| r == &p.role)
//                     && p.permission == Permission::Owner),
//             error::PermissionDenied
//         );

//         // TODO: check if permission exists

//         backend
//             .collection_permissions
//             .push(LayerCollectionPermission {
//                 collection: resource.clone(),
//                 role: *role,
//                 permission,
//             });

//         Ok(())
//     }

//     async fn remove_permission(
//         &self,
//         role: &RoleId,
//         resource: &LayerCollectionId,
//         permission: Permission,
//     ) -> Result<()> {
//         let mut backend = self.backend.write().await;

//         // check owner permission on collection
//         ensure!(
//             backend
//                 .collection_permissions
//                 .iter()
//                 .any(|p| p.collection == *resource
//                     && self.session.roles.iter().any(|r| r == &p.role)
//                     && p.permission == Permission::Owner),
//             error::PermissionDenied
//         );

//         // TODO: check if permission exists

//         let collection_permission = LayerCollectionPermission {
//             collection: resource.clone(),
//             role: *role,
//             permission,
//         };

//         backend
//             .collection_permissions
//             .retain(|p| p != &collection_permission);

//         Ok(())
//     }
// }
