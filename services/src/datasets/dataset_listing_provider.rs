use std::{collections::HashMap, str::FromStr};

use async_trait::async_trait;
use geoengine_datatypes::dataset::LayerId;
use serde::{Deserialize, Serialize};

use crate::{
    datasets::listing::DatasetProvider,
    error::Result,
    layers::{
        layer::{
            CollectionItem, Layer, LayerCollection, LayerCollectionListing, LayerListing,
            ProviderLayerCollectionId, ProviderLayerId,
        },
        listing::{
            LayerCollectionId, LayerCollectionProvider, ProviderCapabilities, SearchCapabilities,
        },
    },
    util::operators::source_operator_from_dataset,
    workflows::workflow::Workflow,
};

use geoengine_datatypes::dataset::{DataProviderId, DatasetId};

use super::listing::DatasetListOptions;

/// Singleton Provider with id `5ad54b5e_e536_47e9_b9df_e729bfc7aaeb`
pub const DATASET_LISTING_PROVIDER_ID: DataProviderId =
    DataProviderId::from_u128(0x5ad54b5e_e536_47e9_b9df_e729bfc7aaeb);

const TAG_PREFIX: &str = "tags:";
const TAG_WILDCARD: &str = "*";
const ROOT_COLLECTION_ID: &str = "root";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DatasetLayerListingDefinition {
    pub name: String,
    pub description: String,
    pub collection: Vec<DatasetLayerListingCollection>,
}

#[derive(Debug)]
pub struct DatasetLayerListingProvider<D> {
    pub dataset_provider: D,
    pub name: String,
    pub description: String,
    pub collections: Vec<DatasetLayerListingCollection>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DatasetLayerListingCollection {
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
}

impl<D> DatasetLayerListingProvider<D>
where
    D: DatasetProvider,
{
    /// Creates a new `DatasetLayerListingProvider` with the following collections:
    pub fn with_all_datasets(dataset_provider: D) -> Self {
        let collections = vec![
            DatasetLayerListingCollection {
                name: "User Uploads".to_string(),
                description: "Datasets uploaded by the user.".to_string(),
                tags: vec!["upload".to_string()],
            },
            DatasetLayerListingCollection {
                name: "Workflows".to_string(),
                description: "Datasets created from workflows.".to_string(),
                tags: vec!["workflow".to_string()],
            },
            DatasetLayerListingCollection {
                name: "All Datasets".to_string(),
                description: "All datasets".to_string(),
                tags: vec![TAG_WILDCARD.to_string()],
            },
        ];

        let definition = DatasetLayerListingDefinition {
            name: "User Data Listing".to_string(),
            description: "User specific datasets grouped by tags".to_string(),
            collection: collections,
        };

        Self::new(dataset_provider, definition)
    }

    /// Creates a new `DatasetLayerListingProvider` with provided tag collections
    pub fn new(dataset_provider: D, definition: DatasetLayerListingDefinition) -> Self {
        Self {
            dataset_provider,
            name: definition.name,
            description: definition.description,
            collections: definition.collection,
        }
    }

    /// Generates a provider listing collection item for this provider
    pub fn generate_provider_listing_collection_item(&self) -> CollectionItem {
        CollectionItem::Collection(LayerCollectionListing {
            id: ProviderLayerCollectionId {
                provider_id: DATASET_LISTING_PROVIDER_ID,
                collection_id: Self::root_collection_id(),
            },
            name: self.name.clone(),
            description: self.description.clone(),
            properties: Default::default(),
        })
    }

    /// Generates the root collection for this provider
    pub fn generate_root_collection(&self, offset: u32, limit: u32) -> LayerCollection {
        let collection_items = self
            .collections
            .iter()
            .skip(offset as usize)
            .take(limit as usize)
            .map(|c| {
                CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: DATASET_LISTING_PROVIDER_ID,
                        collection_id: LayerCollectionId(
                            TAG_PREFIX.to_owned() + &c.tags.join(",").clone(),
                        ),
                    },
                    name: c.name.clone(),
                    description: c.description.clone(),
                    properties: vec![],
                })
            })
            .collect();

        LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: DATASET_LISTING_PROVIDER_ID,
                collection_id: Self::root_collection_id(),
            },
            name: self.name.clone(),
            description: self.description.clone(),
            items: collection_items,
            entry_label: None,
            properties: vec![],
        }
    }

    /// Generates a collection for the given tags
    pub async fn generate_tags_collection(
        &self,
        tags: Vec<String>,
        offset: u32,
        limit: u32,
    ) -> Result<LayerCollection> {
        let query_tags = if tags.is_empty() {
            None
        } else {
            Some(tags.clone())
        };

        let query: DatasetListOptions = DatasetListOptions {
            filter: None,
            order: crate::datasets::listing::OrderBy::NameAsc,
            offset,
            limit,
            tags: query_tags,
        };

        let datasets = self.dataset_provider.list_datasets(query).await.unwrap();

        let collection_items = datasets
            .iter()
            .map(|d| {
                CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: DATASET_LISTING_PROVIDER_ID,
                        layer_id: LayerId(d.id.to_string()),
                    },
                    name: d.display_name.clone(),
                    description: d.description.clone(),
                    properties: vec![],
                })
            })
            .collect();

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: DATASET_LISTING_PROVIDER_ID,
                collection_id: LayerCollectionId(tags.join(",").to_string()),
            },
            name: tags.join(","),
            description: String::new(),
            items: collection_items,
            entry_label: None,
            properties: vec![],
        })
    }

    pub fn root_collection_id() -> LayerCollectionId {
        LayerCollectionId(ROOT_COLLECTION_ID.to_owned())
    }
}

#[async_trait]
impl<D> LayerCollectionProvider for DatasetLayerListingProvider<D>
where
    D: DatasetProvider,
{
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            listing: true,
            search: SearchCapabilities::none(),
        }
    }

    async fn load_layer_collection(
        &self,
        collection: &LayerCollectionId,
        options: crate::layers::layer::LayerCollectionListOptions,
    ) -> Result<LayerCollection, crate::error::Error> {
        tracing::debug!("Loading dataset layer collection: {:?}", collection);

        if collection == &Self::root_collection_id()
            || collection.0.is_empty()
            || collection.0 == TAG_PREFIX
        {
            return Ok(self.generate_root_collection(options.offset, options.limit));
        }

        if !collection.0.starts_with(TAG_PREFIX) {
            return Err(crate::error::Error::InvalidLayerCollectionId);
        }

        let tags = if collection.0[TAG_PREFIX.len()..] == *TAG_WILDCARD {
            vec![]
        } else {
            collection.0[TAG_PREFIX.len()..]
                .split(',')
                .map(std::string::ToString::to_string)
                .collect()
        };

        return self
            .generate_tags_collection(tags, options.offset, options.limit)
            .await;
    }

    async fn get_root_layer_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(Self::root_collection_id())
    }

    async fn load_layer(&self, id: &LayerId) -> Result<crate::layers::layer::Layer> {
        let dataset_id = DatasetId::from_str(&id.0)?;

        let dataset = self.dataset_provider.load_dataset(&dataset_id).await?;

        let operator =
            source_operator_from_dataset(&dataset.source_operator, &dataset.name.into())?;

        Ok(Layer {
            id: ProviderLayerId {
                provider_id: DATASET_LISTING_PROVIDER_ID,
                layer_id: id.clone(),
            },
            name: dataset.display_name,
            description: dataset.description,
            workflow: Workflow { operator },
            symbology: dataset.symbology,
            properties: vec![],
            metadata: HashMap::new(),
        })
    }
}
