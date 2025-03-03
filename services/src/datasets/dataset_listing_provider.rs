use std::{borrow::Cow, collections::HashMap, str::FromStr};

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
            SearchParameters, SearchType, SearchTypes,
        },
    },
    util::operators::source_operator_from_dataset,
    workflows::workflow::Workflow,
};

use geoengine_datatypes::dataset::{DataProviderId, DatasetId};

use super::listing::{DatasetListOptions, DatasetListing, OrderBy, ProvenanceOutput};

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
        tags: Option<Vec<String>>,
        offset: u32,
        limit: u32,
    ) -> Result<LayerCollection> {
        let tags_str = tags
            .as_ref()
            .map_or_else(String::new, |tags| tags.join(","));

        let query: DatasetListOptions = DatasetListOptions {
            filter: None,
            order: crate::datasets::listing::OrderBy::NameAsc,
            offset,
            limit,
            tags,
        };

        let datasets = self.dataset_provider.list_datasets(query).await?;

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: self.id,
                collection_id: LayerCollectionId(tags_str.clone()),
            },
            name: tags_str,
            description: String::new(),
            items: self.datasets_to_collection_items(&datasets),
            entry_label: None,
            properties: vec![],
        })
    }

    pub fn root_collection_id() -> LayerCollectionId {
        LayerCollectionId(ROOT_COLLECTION_ID.to_owned())
    }

    fn is_root_collection(collection_id: &LayerCollectionId) -> bool {
        let collection_str = &collection_id.0;
        collection_str == ROOT_COLLECTION_ID
            || collection_str.is_empty()
            || collection_str == TAG_PREFIX
    }

    fn datasets_to_collection_items(&self, datasets: &[DatasetListing]) -> Vec<CollectionItem> {
        datasets
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
            .collect()
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
            search: SearchCapabilities {
                search_types: SearchTypes {
                    fulltext: true,
                    prefix: false,
                },
                autocomplete: true,
                filters: None,
            },
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

        if Self::is_root_collection(collection) {
            return Ok(self.generate_root_collection(options.offset, options.limit));
        }

        let tags = tags_from_collection_id(collection)?;

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

    async fn search(
        &self,
        collection_id: &LayerCollectionId,
        search: SearchParameters,
    ) -> Result<LayerCollection> {
        capabilities_check(&search)?;

        let tags = if Self::is_root_collection(collection_id) {
            None
        } else {
            tags_from_collection_id(collection_id)?
        };

        let layer_collection_name = format!("Search results for '{}'", search.search_string);
        let layer_collection_description = format!(
            "Searched in {}",
            if let Some(tags) = &tags {
                Cow::from(tags.join(","))
            } else {
                Cow::from("root")
            }
        );

        let datasets = self
            .dataset_provider
            .list_datasets(DatasetListOptions {
                filter: Some(search.search_string),
                order: OrderBy::NameAsc,
                offset: search.offset,
                limit: search.limit,
                tags,
            })
            .await?;

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: self.id,
                collection_id: LayerCollectionId("search".to_string()),
            },
            name: layer_collection_name,
            description: layer_collection_description,
            items: self.datasets_to_collection_items(&datasets),
            entry_label: None,
            properties: vec![],
        })
    }

    async fn autocomplete_search(
        &self,
        collection_id: &LayerCollectionId,
        search: SearchParameters,
    ) -> Result<Vec<String>> {
        capabilities_check(&search)?;

        let tags = if Self::is_root_collection(collection_id) {
            None
        } else {
            tags_from_collection_id(collection_id)?
        };

        self.dataset_provider
            .dataset_autocomplete_search(tags, search.search_string, search.limit, search.offset)
            .await
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

fn tags_from_collection_id(collection_id: &LayerCollectionId) -> Result<Option<Vec<String>>> {
    let collection_str = &collection_id.0;

    if !collection_str.starts_with(TAG_PREFIX) {
        return Err(crate::error::Error::InvalidLayerCollectionId);
    }

    if collection_str[TAG_PREFIX.len()..] == *TAG_WILDCARD {
        return Ok(None);
    }

    Ok(Some(
        collection_str[TAG_PREFIX.len()..]
            .split(',')
            .map(std::string::ToString::to_string)
            .collect(),
    ))
}

fn capabilities_check(search_params: &SearchParameters) -> Result<()> {
    if search_params.search_type != SearchType::Fulltext {
        return Err(crate::error::Error::NotImplemented {
            message: "Only fulltext search is supported".to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        contexts::SessionContext,
        contexts::{PostgresContext, PostgresDb, PostgresSessionContext},
        datasets::{AddDataset, storage::DatasetStore},
        ge_context,
        layers::storage::LayerProviderDb,
    };
    use geoengine_datatypes::{
        collections::VectorDataType,
        primitives::{CacheTtlSeconds, TimeGranularity, TimeStep},
        raster::RasterDataType,
        spatial_reference::SpatialReferenceOption,
    };
    use geoengine_operators::{
        engine::{RasterBandDescriptors, StaticMetaData},
        source::{
            FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters,
            GdalMetaDataRegular, OgrSourceErrorSpec,
        },
    };
    use tokio_postgres::NoTls;

    #[ge_context::test(user = "admin")]
    #[allow(clippy::too_many_lines)]
    async fn it_searches(_app_ctx: PostgresContext<NoTls>, ctx: PostgresSessionContext<NoTls>) {
        let db = ctx.db();

        let provider = DatasetLayerListingProviderDefinition {
            id: DataProviderId::from_u128(0xcbb2_1ee3_d15d_45c5_a175_6696_4adf_4e85),
            name: "User Data Listing".to_string(),
            description: "User specific datasets grouped by tags.".to_string(),
            priority: None,
            collections: vec![
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
                    tags: vec!["*".to_string()],
                },
            ],
        };

        let provider_id = db.add_layer_provider(provider.into()).await.unwrap();

        add_two_datasets(&db).await;

        let provider = db.load_layer_provider(provider_id).await.unwrap();

        let layer_collection_id_root = provider.get_root_layer_collection_id().await.unwrap();
        let layer_collection_id_star = LayerCollectionId("tags:*".to_string());
        let layer_collection_id_tag = LayerCollectionId("tags:upload".to_string());

        let result = provider
            .search(
                &layer_collection_id_star,
                SearchParameters {
                    search_string: "dataset".to_string(),
                    search_type: SearchType::Fulltext,
                    offset: 0,
                    limit: 10,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            result
                .items
                .iter()
                .map(CollectionItem::name)
                .collect::<Vec<_>>(),
            vec!["GdalDataset", "OgrDataset"]
        );

        let result = provider
            .search(
                &layer_collection_id_tag,
                SearchParameters {
                    search_string: "dataset".to_string(),
                    search_type: SearchType::Fulltext,
                    offset: 0,
                    limit: 10,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            result
                .items
                .iter()
                .map(CollectionItem::name)
                .collect::<Vec<_>>(),
            vec!["OgrDataset"]
        );

        let result = provider
            .search(
                &layer_collection_id_root,
                SearchParameters {
                    search_string: "Gdal".to_string(),
                    search_type: SearchType::Fulltext,
                    offset: 0,
                    limit: 10,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            result
                .items
                .iter()
                .map(CollectionItem::name)
                .collect::<Vec<_>>(),
            vec!["GdalDataset"]
        );

        assert!(
            provider
                .search(
                    &layer_collection_id_root,
                    SearchParameters {
                        search_string: "Gdal".to_string(),
                        search_type: SearchType::Prefix,
                        offset: 0,
                        limit: 10,
                    },
                )
                .await
                .is_err()
        );
    }

    #[ge_context::test(user = "admin")]
    async fn it_autocompletes(ctx: PostgresSessionContext<NoTls>) {
        let db = ctx.db();

        let provider = DatasetLayerListingProviderDefinition {
            id: DataProviderId::from_u128(0xcbb2_1ee3_d15d_45c5_a175_6696_4adf_4e85),
            name: "User Data Listing".to_string(),
            description: "User specific datasets grouped by tags.".to_string(),
            priority: None,
            collections: vec![
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
                    tags: vec!["*".to_string()],
                },
            ],
        };

        let provider_id = db.add_layer_provider(provider.into()).await.unwrap();

        add_two_datasets(&db).await;

        let provider = db.load_layer_provider(provider_id).await.unwrap();

        let layer_collection_id_root = provider.get_root_layer_collection_id().await.unwrap();
        let layer_collection_id_star = LayerCollectionId("tags:*".to_string());
        let layer_collection_id_tag = LayerCollectionId("tags:upload".to_string());

        assert_eq!(
            provider
                .autocomplete_search(
                    &layer_collection_id_star,
                    SearchParameters {
                        search_string: "dataset".to_string(),
                        search_type: SearchType::Fulltext,
                        offset: 0,
                        limit: 10,
                    },
                )
                .await
                .unwrap(),
            vec!["GdalDataset", "OgrDataset"]
        );

        assert_eq!(
            provider
                .autocomplete_search(
                    &layer_collection_id_tag,
                    SearchParameters {
                        search_string: "dataset".to_string(),
                        search_type: SearchType::Fulltext,
                        offset: 0,
                        limit: 10,
                    },
                )
                .await
                .unwrap(),
            vec!["OgrDataset"]
        );

        assert_eq!(
            provider
                .autocomplete_search(
                    &layer_collection_id_root,
                    SearchParameters {
                        search_string: "Gdal".to_string(),
                        search_type: SearchType::Fulltext,
                        offset: 0,
                        limit: 10,
                    },
                )
                .await
                .unwrap(),
            vec!["GdalDataset"]
        );

        assert!(
            provider
                .autocomplete_search(
                    &layer_collection_id_root,
                    SearchParameters {
                        search_string: "Gdal".to_string(),
                        search_type: SearchType::Prefix,
                        offset: 0,
                        limit: 10,
                    },
                )
                .await
                .is_err()
        );
    }

    async fn add_two_datasets(db: &PostgresDb<NoTls>) {
        let vector_descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let raster_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            time: None,
            bbox: None,
            resolution: None,
            bands: RasterBandDescriptors::new_single_band(),
        };

        let vector_ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
            symbology: None,
            provenance: None,
            tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
        };

        let raster_ds = AddDataset {
            name: None,
            display_name: "GdalDataset".to_string(),
            description: "My Gdal dataset".to_string(),
            source_operator: "GdalSource".to_string(),
            symbology: None,
            provenance: None,
            tags: Some(vec!["test".to_owned()]),
        };

        let gdal_params = GdalDatasetParameters {
            file_path: Default::default(),
            rasterband_channel: 0,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: Default::default(),
                x_pixel_size: 0.0,
                y_pixel_size: 0.0,
            },
            width: 0,
            height: 0,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: None,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: false,
            retry: None,
        };

        let vector_meta = StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: String::new(),
                data_type: None,
                time: Default::default(),
                default_geometry: None,
                columns: None,
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            },
            result_descriptor: vector_descriptor.clone(),
            phantom: Default::default(),
        };

        let raster_meta = GdalMetaDataRegular {
            result_descriptor: raster_descriptor.clone(),
            params: gdal_params.clone(),
            time_placeholders: Default::default(),
            data_time: Default::default(),
            step: TimeStep {
                granularity: TimeGranularity::Millis,
                step: 0,
            },
            cache_ttl: CacheTtlSeconds::default(),
        };

        let _ = db.add_dataset(vector_ds, vector_meta.into()).await.unwrap();

        let _ = db
            .add_dataset(raster_ds.clone(), raster_meta.into())
            .await
            .unwrap();
    }
}
