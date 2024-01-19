use std::{collections::HashMap, str::FromStr};

use async_trait::async_trait;
use geoengine_datatypes::{
    dataset::{DataId, LayerId},
    primitives::{RasterQueryRectangle, VectorQueryRectangle},
};
use geoengine_operators::{
    engine::{MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};

use crate::{
    contexts::GeoEngineDb,
    datasets::listing::DatasetProvider,
    error::Result,
    layers::{
        external::{DataProvider, DataProviderDefinition},
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

use super::listing::{DatasetListOptions, ProvenanceOutput};

const TAG_PREFIX: &str = "tags:";
const TAG_WILDCARD: &str = "*";
const ROOT_COLLECTION_ID: &str = "root";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub struct DatasetLayerListingProviderDefinition {
    pub id: DataProviderId,
    pub name: String,
    pub description: String,
    pub priority: Option<i16>,
    pub collections: Vec<DatasetLayerListingCollection>,
}

pub struct DatasetLayerListingProvider<D> {
    pub dataset_provider: D,
    pub id: DataProviderId,
    pub name: String,
    pub description: String,
    pub collections: Vec<DatasetLayerListingCollection>,
}

// manual Debug implementations because `DataProvider` requires it but `DatasetProvider` doesn't provide it
#[allow(clippy::missing_fields_in_debug)]
impl<D> std::fmt::Debug for DatasetLayerListingProvider<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasetLayerListingProvider")
            .field("name", &self.name)
            .field("description", &self.description)
            .field("collections", &self.collections)
            .finish()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, FromSql, ToSql)]
pub struct DatasetLayerListingCollection {
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
}

#[async_trait]
impl<D: GeoEngineDb> DataProviderDefinition<D> for DatasetLayerListingProviderDefinition {
    async fn initialize(self: Box<Self>, db: D) -> Result<Box<dyn DataProvider>> {
        Ok(Box::new(DatasetLayerListingProvider {
            dataset_provider: db,
            id: self.id,
            name: self.name,
            description: self.description,
            collections: self.collections,
        }))
    }

    fn type_name(&self) -> &'static str {
        "DatasetLayerListingProviderDefinition"
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DataProviderId {
        self.id
    }

    fn priority(&self) -> i16 {
        self.priority.unwrap_or(0)
    }
}

impl<D> DatasetLayerListingProvider<D>
where
    D: DatasetProvider + Send + Sync + 'static,
{
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
                        provider_id: self.id,
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
                provider_id: self.id,
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
                        provider_id: self.id,
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
                provider_id: self.id,
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
    D: DatasetProvider + Send + Sync + 'static,
{
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            listing: true,
            search: SearchCapabilities::none(),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
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
                provider_id: self.id,
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

#[async_trait]
impl<D> DataProvider for DatasetLayerListingProvider<D>
where
    D: DatasetProvider + Send + Sync + 'static,
{
    async fn provenance(&self, _id: &DataId) -> Result<ProvenanceOutput> {
        // never called but handled by the dataset provider
        Err(crate::error::Error::NotImplemented {
            message: "provenance output is available via the Dataset DB".to_string(),
        })
    }
}

#[async_trait]
impl<D>
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for DatasetLayerListingProvider<D>
where
    D: DatasetProvider + Send + Sync + 'static,
{
    async fn meta_data(
        &self,
        _id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<
            dyn MetaData<
                MockDatasetDataSourceLoadingInfo,
                VectorResultDescriptor,
                VectorQueryRectangle,
            >,
        >,
        geoengine_operators::error::Error,
    > {
        // never called but handled by the dataset provider
        Err(geoengine_operators::error::Error::NotImplemented)
    }
}

#[async_trait]
impl<D> MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for DatasetLayerListingProvider<D>
where
    D: DatasetProvider + Send + Sync + 'static,
{
    async fn meta_data(
        &self,
        _id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        // never called but handled by the dataset provider
        Err(geoengine_operators::error::Error::NotImplemented)
    }
}

#[async_trait]
impl<D> MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for DatasetLayerListingProvider<D>
where
    D: DatasetProvider + Send + Sync + 'static,
{
    async fn meta_data(
        &self,
        _id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        // never called but handled by the dataset provider
        Err(geoengine_operators::error::Error::NotImplemented)
    }
}
