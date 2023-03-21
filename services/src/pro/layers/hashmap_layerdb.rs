use async_trait::async_trait;
use futures::{stream, StreamExt};

use snafu::ensure;

use crate::error::{self, Error, Result};

use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{
    AddLayer, AddLayerCollection, CollectionItem, Layer, LayerCollection,
    LayerCollectionListOptions,
};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use crate::layers::storage::{
    LayerDb, LayerProviderDb, LayerProviderListing, LayerProviderListingOptions,
};
use crate::pro::contexts::ProInMemoryDb;
use crate::pro::permissions::{Permission, PermissionDb};

use crate::util::user_input::Validated;

use crate::api::model::datatypes::{DataProviderId, LayerId};
use crate::util::user_input::UserInput;

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

        self.create_resource(layer_id.clone()).await?;

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

        let backend = &self.backend.layer_db;

        backend.add_layer_with_id(id, layer, collection).await?;

        self.create_resource(id.clone()).await?;

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

        backend.add_layer_to_collection(layer, collection).await?;

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

        self.create_resource(collection_id.clone()).await?;

        Ok(collection_id)
    }

    async fn add_layer_collection_with_id(
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

        let backend = &self.backend.layer_db;

        backend
            .add_layer_collection_with_id(id, collection, parent)
            .await?;

        self.create_resource(id.clone()).await?;

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

        let backend = &self.backend.layer_db;

        backend.add_collection_to_parent(collection, parent).await?;

        Ok(())
    }

    async fn remove_layer_collection(&self, collection: &LayerCollectionId) -> Result<()> {
        ensure!(
            self.has_permission(collection.clone(), Permission::Owner)
                .await?,
            error::PermissionDenied
        );

        let backend = &self.backend.layer_db;

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
            self.has_permission(collection.clone(), Permission::Owner)
                .await?,
            error::PermissionDenied
        );

        let backend = &self.backend.layer_db;

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

        let backend = &self.backend.layer_db;

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
        ensure!(
            self.has_permission(collection_id.clone(), Permission::Read)
                .await?,
            error::PermissionDenied
        );

        let backend = &self.backend.layer_db;

        let options = options.user_input;

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
        ensure!(self.session.is_admin(), error::PermissionDenied);

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

#[cfg(test)]
mod tests {
    use geoengine_datatypes::util::test::TestDefault;

    use crate::{
        contexts::{ApplicationContext, Context, MockableSession},
        pro::{contexts::ProInMemoryContext, users::UserSession, util::tests::admin_login},
    };

    use super::*;

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn it_restricts_permissions() {
        let app_ctx = ProInMemoryContext::test_default();

        let admin_session = admin_login(&app_ctx).await;
        let session1 = UserSession::mock();

        let admin_db = app_ctx.session_context(admin_session.clone()).db();
        let db1 = app_ctx.session_context(session1.clone()).db();

        let root = admin_db.get_root_layer_collection_id().await.unwrap();

        // add new collection as admin
        let new_collection_id = admin_db
            .add_layer_collection(
                AddLayerCollection {
                    name: "admin collection".to_string(),
                    description: String::new(),
                }
                .validated()
                .unwrap(),
                &root,
            )
            .await
            .unwrap();

        // load as regular user, not visible
        let collection = db1
            .load_layer_collection(
                &root,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 10,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(!collection.items.iter().any(|c| match c {
            CollectionItem::Collection(c) => c.id.collection_id == new_collection_id,
            CollectionItem::Layer(_) => false,
        }));

        // give user read permission
        admin_db
            .add_permission(
                session1.user.id.into(),
                new_collection_id.clone(),
                Permission::Read,
            )
            .await
            .unwrap();

        // now visible
        let collection = db1
            .load_layer_collection(
                &root,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 10,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        assert!(collection.items.iter().any(|c| match c {
            CollectionItem::Collection(c) => c.id.collection_id == new_collection_id,
            CollectionItem::Layer(_) => false,
        }));

        // add new layer in the collection as user, fails because only read permission
        let result = db1
            .add_layer_collection(
                AddLayerCollection {
                    name: "user layer".to_string(),
                    description: String::new(),
                }
                .validated()
                .unwrap(),
                &new_collection_id,
            )
            .await;

        assert!(result.is_err());

        // give user owner permission
        admin_db
            .add_permission(
                session1.user.id.into(),
                new_collection_id.clone(),
                Permission::Owner,
            )
            .await
            .unwrap();

        // add now works
        db1.add_layer_collection(
            AddLayerCollection {
                name: "user layer".to_string(),
                description: String::new(),
            }
            .validated()
            .unwrap(),
            &new_collection_id,
        )
        .await
        .unwrap();

        // remove permissions again
        admin_db
            .remove_permission(
                session1.user.id.into(),
                new_collection_id.clone(),
                Permission::Read,
            )
            .await
            .unwrap();
        admin_db
            .remove_permission(
                session1.user.id.into(),
                new_collection_id.clone(),
                Permission::Owner,
            )
            .await
            .unwrap();

        // access is gone now
        let result = db1
            .add_layer_collection(
                AddLayerCollection {
                    name: "user layer".to_string(),
                    description: String::new(),
                }
                .validated()
                .unwrap(),
                &root,
            )
            .await;

        assert!(result.is_err());

        let collection = db1
            .load_layer_collection(
                &root,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 10,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        assert!(!collection.items.iter().any(|c| match c {
            CollectionItem::Collection(c) => c.id.collection_id == new_collection_id,
            CollectionItem::Layer(_) => false,
        }));
    }
}
