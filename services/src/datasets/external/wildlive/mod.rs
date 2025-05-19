use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

use crate::{
    contexts::GeoEngineDb,
    datasets::listing::{Provenance, ProvenanceOutput},
    layers::{
        external::{DataProvider, DataProviderDefinition},
        layer::{
            CollectionItem, Layer, LayerCollection, LayerCollectionListOptions,
            LayerCollectionListing, LayerListing, ProviderLayerCollectionId, ProviderLayerId,
        },
        listing::{
            LayerCollectionId, LayerCollectionProvider, ProviderCapabilities, SearchCapabilities,
        },
    },
    users::UserId,
    workflows::workflow::Workflow,
};
use async_trait::async_trait;
use cache::{DatasetFilename, WildliveCache};
use datasets::{project_stations_dataset, projects_dataset};
use geoengine_datatypes::{
    collections::VectorDataType,
    dataset::{DataId, DataProviderId, LayerId, NamedData},
    error::BoxedResultExt,
    primitives::{
        CacheTtlSeconds, FeatureDataType, Measurement, RasterQueryRectangle, VectorQueryRectangle,
    },
    spatial_reference::SpatialReference,
};
use geoengine_operators::{
    engine::{
        MetaData, MetaDataProvider, RasterResultDescriptor, StaticMetaData, VectorColumnInfo,
        VectorOperator, VectorResultDescriptor,
    },
    mock::MockDatasetDataSourceLoadingInfo,
    source::{
        GdalLoadingInfo, OgrSource, OgrSourceColumnSpec, OgrSourceDataset,
        OgrSourceDatasetTimeType, OgrSourceErrorSpec, OgrSourceParameters,
    },
};
use geojson::GeoJson;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use url::Url;

pub use error::WildliveError;

mod cache;
mod datasets;
mod error;

type Result<T, E = WildliveError> = std::result::Result<T, E>;

// TODO: remove on removal of data connector
static CACHES: LazyLock<Arc<RwLock<HashMap<DataProviderId, WildliveCache>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, FromSql, ToSql)]
pub struct WildliveDataConnectorDefinition {
    pub id: DataProviderId,
    pub name: String,
    pub description: String,
    pub api_key: Option<String>,
    pub priority: Option<i16>,
}

#[derive(Debug)]
pub struct WildliveDataConnector {
    id: DataProviderId,
    api_endpoint: Url,
    user: Option<UserId>,
    api_key: Option<String>,
    name: String,
    description: String,
    cache: WildliveCache,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", tag = "type")]
enum WildliveCollectionId {
    Projects,
    #[serde(rename_all = "camelCase")]
    Project {
        project_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
enum WildliveLayerId {
    Projects,
    #[serde(rename_all = "camelCase")]
    Stations {
        project_id: String,
    },
    #[serde(rename_all = "camelCase")]
    Captures {
        project_id: String,
    },
}

#[async_trait]
impl<D: GeoEngineDb> DataProviderDefinition<D> for WildliveDataConnectorDefinition {
    async fn initialize(self: Box<Self>, _db: D) -> crate::error::Result<Box<dyn DataProvider>> {
        let caches = (*CACHES).clone();
        let existing_cache = caches.read().await.get(&self.id).cloned();

        let cache = match existing_cache {
            Some(cache) => cache,
            None => {
                let mut cache_map = caches.write().await;
                let cache = WildliveCache::new()?;

                cache_map.entry(self.id.clone()).or_insert(cache).clone()
            }
        };

        Ok(Box::new(WildliveDataConnector {
            id: self.id,
            api_endpoint: Url::parse("https://wildlive.senckenberg.de/api/")
                .map_err(|source| WildliveError::InvalidUrl { source })?,
            user: None, // TODO: get user from db
            api_key: self.api_key,
            name: self.name,
            description: self.description,
            cache,
        }))
    }

    fn type_name(&self) -> &'static str {
        "WildLIVE! Portal Connector"
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

#[async_trait]
impl DataProvider for WildliveDataConnector {
    async fn provenance(&self, id: &DataId) -> crate::error::Result<ProvenanceOutput> {
        Ok(ProvenanceOutput {
            data: id.clone(),
            provenance: Some(vec![
                // TODO: check if this is correct
                Provenance {
                    citation: "WildLIVE! Portal".to_string(),
                    license: "CC-BY-4.0 (https://spdx.org/licenses/CC-BY-4.0)".to_string(),
                    uri: "https://wildlive.senckenberg.de".to_string(),
                },
            ]),
        })
    }
}

#[async_trait]
impl LayerCollectionProvider for WildliveDataConnector {
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
        collection_id: &LayerCollectionId,
        options: LayerCollectionListOptions,
    ) -> crate::error::Result<LayerCollection> {
        match WildliveCollectionId::try_from(collection_id.clone())? {
            WildliveCollectionId::Projects => {
                let mut items: Vec<_> = vec![CollectionItem::Layer(LayerListing {
                    r#type: Default::default(),
                    id: self.layer_id(WildliveLayerId::Projects)?,
                    name: "Projects".to_string(),
                    description: "Overview of all projects".to_string(),
                    properties: Vec::new(),
                })];

                let projects = datasets::projects(
                    &self.api_endpoint,
                    self.api_key.as_ref().map(String::as_str),
                )
                .await?;

                for project in projects {
                    items.push(CollectionItem::Collection(LayerCollectionListing {
                        r#type: Default::default(),
                        id: self.collection_id(WildliveCollectionId::Project {
                            project_id: project.id,
                        })?,
                        name: project.name,
                        description: project.description,
                        properties: Vec::new(),
                    }));
                }

                Ok(LayerCollection {
                    id: self.collection_id(WildliveCollectionId::Projects)?,
                    name: "WildLIVE! Projects".to_string(),
                    description: "List of all projects".to_string(),
                    // TODO: paginate request instead of loading all projects
                    items: items
                        .drain(subset_range(options.offset, options.limit, items.len()))
                        .collect(),
                    entry_label: None,
                    properties: Vec::new(),
                })
            }
            WildliveCollectionId::Project { project_id } => {
                let mut items: Vec<_> = vec![CollectionItem::Layer(LayerListing {
                    r#type: Default::default(),
                    id: self.layer_id(WildliveLayerId::Stations {
                        project_id: project_id.clone(),
                    })?,
                    name: "Stations".to_string(),
                    description: "Overview of all project stations".to_string(),
                    properties: Vec::new(),
                })];

                Ok(LayerCollection {
                    id: self.collection_id(WildliveCollectionId::Project { project_id })?,
                    name: "WildLIVE! Projects".to_string(),
                    description: "List of all projects".to_string(),
                    items: items
                        .drain(subset_range(options.offset, options.limit, items.len()))
                        .collect(),
                    entry_label: None,
                    properties: Vec::new(),
                })
            }
        }
    }

    async fn get_root_layer_collection_id(&self) -> crate::error::Result<LayerCollectionId> {
        Ok(WildliveCollectionId::Projects.try_into()?)
    }

    async fn load_layer(&self, id: &LayerId) -> crate::error::Result<Layer> {
        match WildliveLayerId::try_from(id.clone())? {
            WildliveLayerId::Projects => Ok(Layer {
                id: self.layer_id(WildliveLayerId::Projects)?,
                name: "Projects".to_string(),
                description: "Overview of all projects".to_string(),
                workflow: Workflow {
                    operator: OgrSource {
                        params: OgrSourceParameters {
                            data: self.named_data(WildliveLayerId::Projects)?,
                            attribute_projection: None,
                            attribute_filters: None,
                        },
                    }
                    .boxed()
                    .into(),
                },
                symbology: None,
                properties: Vec::new(),
                metadata: Default::default(),
            }),
            WildliveLayerId::Stations { project_id } => Ok(Layer {
                id: self.layer_id(WildliveLayerId::Stations {
                    project_id: project_id.clone(),
                })?,
                name: format!("Stations for project {project_id}"),
                description: format!("Overview of all stations within project {project_id}"),
                workflow: Workflow {
                    operator: OgrSource {
                        params: OgrSourceParameters {
                            data: self.named_data(WildliveLayerId::Stations { project_id })?,
                            attribute_projection: None,
                            attribute_filters: None,
                        },
                    }
                    .boxed()
                    .into(),
                },
                symbology: None,
                properties: Vec::new(),
                metadata: Default::default(),
            }),
            WildliveLayerId::Captures { project_id: _ } => {
                todo!("implement WildliveDataProvider::load_layer for captures");
            }
        }
    }
}

fn subset_range(offset: u32, limit: u32, length: usize) -> std::ops::Range<usize> {
    let offset = offset as usize;
    let limit = limit as usize;

    if offset >= length {
        return 0..0;
    }

    offset..(offset + limit).min(length)
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for WildliveDataConnector
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let layer_id = if let DataId::External(data_id) = id {
            data_id.layer_id.clone()
        } else {
            return Err(geoengine_operators::error::Error::InvalidDataId);
        };

        let layer_id = WildliveLayerId::try_from(layer_id).map_err(|_| {
            // TODO: be more verbose
            geoengine_operators::error::Error::InvalidDataId
        })?;

        self.meta_data(&layer_id).await.map_err(|error| {
            geoengine_operators::error::Error::MetaData {
                source: Box::new(error),
            }
        })
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for WildliveDataConnector
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
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for WildliveDataConnector
{
    async fn meta_data(
        &self,
        _id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

impl WildliveDataConnector {
    fn layer_id(&self, id: WildliveLayerId) -> Result<ProviderLayerId> {
        Ok(ProviderLayerId {
            provider_id: self.id.clone(),
            layer_id: id.try_into()?,
        })
    }

    fn collection_id(&self, id: WildliveCollectionId) -> Result<ProviderLayerCollectionId> {
        Ok(ProviderLayerCollectionId {
            provider_id: self.id.clone(),
            collection_id: id.try_into()?,
        })
    }

    fn named_data(&self, id: WildliveLayerId) -> Result<NamedData> {
        Ok(NamedData {
            namespace: self.user.map(|u| u.to_string()),
            provider: Some(self.id.to_string()),
            name: LayerId::try_from(id)?.0,
        })
    }

    async fn meta_data(
        &self,
        layer_id: &WildliveLayerId,
    ) -> Result<Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>>
    {
        let dataset_name = layer_id.filename()?;

        let file_name = match self.cache.get(&dataset_name).await? {
            Some(file_name) => file_name,
            None => {
                let geojson: geojson::FeatureCollection = match layer_id {
                    WildliveLayerId::Projects => {
                        projects_dataset(
                            &self.api_endpoint,
                            self.api_key.as_ref().map(String::as_str),
                        )
                        .await?
                    }
                    WildliveLayerId::Stations { project_id } => {
                        project_stations_dataset(
                            &self.api_endpoint,
                            self.api_key.as_ref().map(String::as_str),
                            &project_id,
                        )
                        .await?
                    }
                    WildliveLayerId::Captures { project_id: _ } => {
                        todo!()
                    }
                };

                self.cache
                    .add(dataset_name.clone(), GeoJson::from(geojson).to_string())
                    .await?
            }
        };

        // TODO: remove outdated datasets after TTL

        match layer_id {
            WildliveLayerId::Projects => {
                let columns: HashMap<String, VectorColumnInfo> = [
                    (
                        "id".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "name".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "description".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                ]
                .iter()
                .cloned()
                .collect();

                Ok(Box::new(StaticMetaData {
                    loading_info: OgrSourceDataset {
                        layer_name: layer_id.layer_name()?,
                        file_name,
                        data_type: Some(VectorDataType::MultiPolygon),
                        time: OgrSourceDatasetTimeType::None,
                        default_geometry: None,
                        columns: Some(OgrSourceColumnSpec {
                            format_specifics: None,
                            x: String::new(),
                            y: None,
                            int: vec![],
                            float: vec![],
                            text: columns
                                .iter()
                                .filter_map(|(name, info)| {
                                    if info.data_type == FeatureDataType::Text {
                                        Some(name.clone())
                                    } else {
                                        None
                                    }
                                })
                                .collect(),
                            bool: vec![],
                            datetime: vec![],
                            rename: None,
                        }),
                        force_ogr_time_filter: false,
                        force_ogr_spatial_filter: false,
                        on_error: OgrSourceErrorSpec::Abort,
                        sql_query: None,
                        attribute_query: None,
                        cache_ttl: CacheTtlSeconds::default(),
                    },
                    result_descriptor: VectorResultDescriptor {
                        data_type: VectorDataType::MultiPolygon,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        columns,
                        time: None, // TODO
                        bbox: None, // TODO
                    },
                    phantom: std::marker::PhantomData,
                }))
            }
            WildliveLayerId::Stations { project_id: _ } => {
                let columns: HashMap<String, VectorColumnInfo> = [
                    (
                        "id".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "name".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "description".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "location".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                ]
                .iter()
                .cloned()
                .collect();

                Ok(Box::new(StaticMetaData {
                    loading_info: OgrSourceDataset {
                        layer_name: layer_id.layer_name()?,
                        file_name,
                        data_type: Some(VectorDataType::MultiPoint),
                        time: OgrSourceDatasetTimeType::None,
                        default_geometry: None,
                        columns: Some(OgrSourceColumnSpec {
                            format_specifics: None,
                            x: String::new(),
                            y: None,
                            int: vec![],
                            float: vec![],
                            text: columns
                                .iter()
                                .filter_map(|(name, info)| {
                                    if info.data_type == FeatureDataType::Text {
                                        Some(name.clone())
                                    } else {
                                        None
                                    }
                                })
                                .collect(),
                            bool: vec![],
                            datetime: vec![],
                            rename: None,
                        }),
                        force_ogr_time_filter: false,
                        force_ogr_spatial_filter: false,
                        on_error: OgrSourceErrorSpec::Abort,
                        sql_query: None,
                        attribute_query: None,
                        cache_ttl: CacheTtlSeconds::default(),
                    },
                    result_descriptor: VectorResultDescriptor {
                        data_type: VectorDataType::MultiPoint,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        columns,
                        time: None, // TODO
                        bbox: None, // TODO
                    },
                    phantom: std::marker::PhantomData,
                }))
            }
            WildliveLayerId::Captures { project_id: _ } => {
                todo!("implement WildliveDataProvider::meta_data for captures");
            }
        }
    }
}

impl TryFrom<WildliveCollectionId> for LayerCollectionId {
    type Error = WildliveError;

    fn try_from(value: WildliveCollectionId) -> std::result::Result<Self, Self::Error> {
        let string = serde_urlencoded::to_string(value)
            .boxed_context(error::UnableToSerializeCollectionId)?;
        Ok(LayerCollectionId(string))
    }
}

impl TryFrom<LayerCollectionId> for WildliveCollectionId {
    type Error = WildliveError;

    fn try_from(value: LayerCollectionId) -> std::result::Result<Self, Self::Error> {
        serde_urlencoded::from_str(&value.0).boxed_context(error::UnableToSerializeCollectionId)
    }
}

impl TryFrom<WildliveLayerId> for LayerId {
    type Error = WildliveError;

    fn try_from(value: WildliveLayerId) -> std::result::Result<Self, Self::Error> {
        let string = serde_urlencoded::to_string(value)
            .boxed_context(error::UnableToSerializeCollectionId)?;
        Ok(LayerId(string))
    }
}

impl TryFrom<LayerId> for WildliveLayerId {
    type Error = WildliveError;

    fn try_from(value: LayerId) -> std::result::Result<Self, Self::Error> {
        serde_urlencoded::from_str(&value.0).boxed_context(error::UnableToSerializeCollectionId)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::{
        collections::VectorDataType,
        dataset::ExternalDataId,
        primitives::{
            BoundingBox2D, CacheTtlSeconds, ColumnSelection, Coordinate2D, FeatureDataType,
            Measurement, SpatialResolution, TimeInterval,
        },
        spatial_reference::SpatialReference,
    };
    use geoengine_operators::{
        engine::VectorColumnInfo,
        source::{OgrSourceColumnSpec, OgrSourceDatasetTimeType, OgrSourceErrorSpec},
    };

    #[test]
    fn it_serializes_collection_ids() {
        let root_collection_id = WildliveCollectionId::Projects;
        let root_collection_layer_id =
            LayerCollectionId::try_from(root_collection_id.clone()).unwrap();

        assert_eq!(root_collection_layer_id.to_string(), "type=projects");
        assert_eq!(
            WildliveCollectionId::try_from(root_collection_layer_id).unwrap(),
            root_collection_id
        );

        let project_collection_id = WildliveCollectionId::Project {
            project_id: "wildlive/853cb65c033fa30bfa1c".to_string(),
        };
        let project_collection_layer_id =
            LayerCollectionId::try_from(project_collection_id.clone()).unwrap();

        assert_eq!(
            project_collection_layer_id.to_string(),
            "type=project&projectId=wildlive%2F853cb65c033fa30bfa1c"
        );
        assert_eq!(
            WildliveCollectionId::try_from(project_collection_layer_id).unwrap(),
            project_collection_id
        );
    }

    #[test]
    fn it_serializes_layer_ids() {
        let projects_id = WildliveLayerId::Projects;
        let projects_layer_id = LayerId::try_from(projects_id.clone()).unwrap();

        assert_eq!(projects_layer_id.to_string(), "type=projects");
        assert_eq!(
            WildliveLayerId::try_from(projects_layer_id).unwrap(),
            projects_id
        );

        let stations_id = WildliveLayerId::Stations {
            project_id: "wildlive/853cb65c033fa30bfa1c".to_string(),
        };
        let stations_layer_id = LayerId::try_from(stations_id.clone()).unwrap();

        assert_eq!(
            stations_layer_id.to_string(),
            "type=stations&projectId=wildlive%2F853cb65c033fa30bfa1c"
        );
        assert_eq!(
            WildliveLayerId::try_from(stations_layer_id).unwrap(),
            stations_id
        );

        let captures_id = WildliveLayerId::Captures {
            project_id: "wildlive/853cb65c033fa30bfa1c".to_string(),
        };
        let captures_layer_id = LayerId::try_from(captures_id.clone()).unwrap();
        assert_eq!(
            captures_layer_id.to_string(),
            "type=captures&projectId=wildlive%2F853cb65c033fa30bfa1c"
        );
        assert_eq!(
            WildliveLayerId::try_from(captures_layer_id).unwrap(),
            captures_id
        );
    }

    #[tokio::test]
    async fn it_shows_an_overview_of_all_projects() {
        let connector = WildliveDataConnector {
            id: DataProviderId::from_u128(12_345_678_901_234_567_890_123_456_789_012_u128),
            api_endpoint: Url::parse("https://wildlive.senckenberg.de/api/").unwrap(),
            name: "WildLIVE! Portal Connector".to_string(),
            description: "WildLIVE! Portal Connector".to_string(),
            user: None,
            api_key: None,
            cache: WildliveCache::new().unwrap(),
        };

        let layer = connector
            .load_layer(&WildliveLayerId::Projects.try_into().unwrap())
            .await
            .unwrap();
        assert_eq!(
            layer,
            Layer {
                id: ProviderLayerId {
                    provider_id: connector.id,
                    layer_id: WildliveLayerId::Projects.try_into().unwrap(),
                },
                name: "Projects".to_string(),
                description: "Overview of all projects".to_string(),
                workflow: Workflow {
                    operator: OgrSource {
                        params: OgrSourceParameters {
                            data: connector.named_data(WildliveLayerId::Projects).unwrap(),
                            attribute_projection: None,
                            attribute_filters: None,
                        },
                    }
                    .boxed()
                    .into(),
                },
                symbology: None,
                properties: Vec::new(),
                metadata: Default::default(),
            }
        );

        let metadata: Box<
            dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        > = MetaDataProvider::<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
            ::meta_data(&connector, &DataId::External(ExternalDataId {
                provider_id: layer.id.provider_id,
                layer_id: layer.id.layer_id,
            }))
            .await
            .unwrap();

        let loading_info = metadata
            .loading_info(VectorQueryRectangle {
                spatial_bounds: BoundingBox2D::new(
                    Coordinate2D { x: 0.0, y: 0.0 },
                    Coordinate2D { x: 1.0, y: 1.0 },
                )
                .unwrap(),
                time_interval: TimeInterval::new(0, 1).unwrap(),
                spatial_resolution: SpatialResolution::new(1.0, 1.0).unwrap(),
                attributes: ColumnSelection::all(),
            })
            .await
            .unwrap();
        assert_eq!(
            loading_info,
            OgrSourceDataset {
                file_name: connector
                    .cache
                    .get(&WildliveLayerId::Projects.filename().unwrap())
                    .await
                    .unwrap()
                    .unwrap(),
                layer_name: WildliveLayerId::Projects.layer_name().unwrap(),
                data_type: Some(VectorDataType::MultiPolygon),
                time: OgrSourceDatasetTimeType::None,
                default_geometry: None,
                columns: Some(OgrSourceColumnSpec {
                    format_specifics: None,
                    x: String::new(),
                    y: None,
                    int: vec![],
                    float: vec![],
                    text: vec![
                        "id".to_string(),
                        "name".to_string(),
                        "description".to_string(),
                    ],
                    bool: vec![],
                    datetime: vec![],
                    rename: None,
                }),
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Abort,
                sql_query: None,
                attribute_query: None,
                cache_ttl: CacheTtlSeconds::default(),
            }
        );

        assert_eq!(
            metadata.result_descriptor().await.unwrap(),
            VectorResultDescriptor {
                data_type: VectorDataType::MultiPolygon,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: [
                    (
                        "id".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "name".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "description".to_string(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                ]
                .iter()
                .cloned()
                .collect(),
                time: None,
                bbox: None,
            }
        );

        assert!(loading_info.file_name.exists());
    }
}
