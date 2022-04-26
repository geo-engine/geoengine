use std::{collections::HashMap, sync::Arc};

use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, InternalDatasetId};
use tokio::sync::RwLock;

use crate::{
    datasets::{
        listing::{DatasetListOptions, DatasetListing, ExternalDatasetProvider},
        storage::{
            Dataset, DatasetProviderListOptions, DatasetProviderListing,
            ExternalDatasetProviderDefinition, MetaDataDefinition, MetaDataDefinitionListOptions,
            MetaDataDefinitionListing,
        },
        upload::{Upload, UploadId, UploadListOptions, UploadListing},
    },
    storage::ListOption,
    workflows::workflow::{Workflow, WorkflowId, WorkflowListOptions, WorkflowListing},
};

use super::{GeoEngineStore, StoredDataset};

#[derive(Debug, Default)]
pub struct InMemoryStoreBackend {
    pub(crate) workflows: HashMap<WorkflowId, Workflow>,
    pub(crate) datasets: HashMap<InternalDatasetId, Dataset>,
    pub(crate) metadata: HashMap<InternalDatasetId, MetaDataDefinition>,
    pub(crate) uploads: HashMap<UploadId, Upload>,
    pub(crate) providers: HashMap<DatasetProviderId, Box<dyn ExternalDatasetProviderDefinition>>,
}

pub struct InMemoryStore {
    pub(crate) backend: Arc<RwLock<InMemoryStoreBackend>>,
}

impl InMemoryStore {
    pub fn new(backend: Arc<RwLock<InMemoryStoreBackend>>) -> Self {
        Self { backend }
    }
}

impl GeoEngineStore for InMemoryStore {}

#[macro_export]
macro_rules! impl_in_memory_store {
    // TODO: get types from `Storable` trait
    ($map_name:ident, $item:ty, $item_id:ty, $item_listing:ty, $item_options:ty) => {
        #[async_trait::async_trait]
        impl $crate::storage::Store<$item> for InMemoryStore {
            async fn create(
                &mut self,
                item: $crate::util::user_input::Validated<$item>,
            ) -> $crate::error::Result<$item_id> {
                let item = item.user_input;
                let id = <$item_id as geoengine_datatypes::util::Identifier>::new(); // TODO: derive possibly, e.g. for Workflows by hashing
                self.backend.write().await.$map_name.insert(id, item);
                Ok(id)
            }

            async fn create_with_id(
                &mut self,
                id: &$item_id,
                item: $crate::util::user_input::Validated<$item>,
            ) -> $crate::error::Result<$item_id> {
                let workflow = item.user_input;
                self.backend.write().await.$map_name.insert(*id, workflow);
                Ok(*id)
            }

            async fn read(&self, id: &$item_id) -> $crate::error::Result<$item> {
                self.backend
                    .read()
                    .await
                    .$map_name
                    .get(id)
                    .cloned()
                    .ok_or($crate::error::Error::NoWorkflowForGivenId) // TODO: use correct error
            }

            async fn update(
                &mut self,
                id: &$item_id,
                item: $crate::util::user_input::Validated<$item>,
            ) -> $crate::error::Result<()> {
                let workflow = item.user_input;
                self.backend.write().await.$map_name.insert(*id, workflow);
                Ok(())
            }

            async fn delete(&mut self, id: &$item_id) -> $crate::error::Result<()> {
                self.backend.write().await.$map_name.remove(id);
                Ok(())
            }

            async fn list(
                &self,
                options: $crate::util::user_input::Validated<$item_options>,
            ) -> $crate::error::Result<Vec<$item_listing>> {
                let options = options.user_input;

                let item_map = &self.backend.read().await.$map_name;

                let mut items: Vec<_> = item_map
                    .iter()
                    .filter(|(_, item)| options.retain(item))
                    .collect();

                items.sort_by(|a, b| options.compare_items(a.1, b.1));

                let items = items
                    .into_iter()
                    .skip(options.offset() as usize)
                    .take(options.limit() as usize)
                    .map(|(id, v)| {
                        let listing = <$item as $crate::storage::Listable<$item>>::list(v, id);
                        listing
                    })
                    .collect();

                Ok(items)
            }
        }
    };
}

impl_in_memory_store!(
    workflows,
    Workflow,
    WorkflowId,
    WorkflowListing,
    WorkflowListOptions
);
impl_in_memory_store!(
    datasets,
    Dataset,
    InternalDatasetId,
    DatasetListing,
    DatasetListOptions
);
impl_in_memory_store!(
    metadata,
    MetaDataDefinition,
    InternalDatasetId,
    MetaDataDefinitionListing,
    MetaDataDefinitionListOptions
);
impl_in_memory_store!(uploads, Upload, UploadId, UploadListing, UploadListOptions);
impl_in_memory_store!(
    providers,
    Box<dyn ExternalDatasetProviderDefinition>,
    DatasetProviderId,
    DatasetProviderListing,
    DatasetProviderListOptions
);
