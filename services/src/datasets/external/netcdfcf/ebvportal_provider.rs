use super::{
    ebvportal_api::EbvPortalApi, loading::LayerCollectionIdFn, netcdf_entity_to_layer_id,
    netcdf_group_to_layer_collection_id, NetCdfCfDataProvider, NetCdfCfDataProviderDefinition,
    NetCdfLayerCollectionId,
};
use crate::{
    contexts::GeoEngineDb,
    datasets::external::netcdfcf::path_to_string,
    error::{Error, Result},
    layers::{
        external::{DataProvider, DataProviderDefinition},
        layer::{
            CollectionItem, Layer, LayerCollection, LayerCollectionListOptions,
            LayerCollectionListing, ProviderLayerCollectionId,
        },
        listing::{
            LayerCollectionId, LayerCollectionProvider, ProviderCapabilities, SearchCapabilities,
        },
    },
};
use async_trait::async_trait;
use geoengine_datatypes::{
    dataset::{DataId, DataProviderId, LayerId},
    primitives::{CacheTtlSeconds, RasterQueryRectangle, VectorQueryRectangle},
};
use geoengine_operators::{
    engine::{MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

/// Singleton Provider with id `77d0bf11-986e-43f5-b11d-898321f1854c`
pub const EBV_PROVIDER_ID: DataProviderId =
    DataProviderId::from_u128(0x77d0_bf11_986e_43f5_b11d_8983_21f1_854c);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EbvPortalDataProviderDefinition {
    pub name: String,
    pub description: String,
    pub priority: Option<i16>,
    pub base_url: Url,
    /// Path were the `NetCDF` data can be found
    pub data: PathBuf,
    /// Path were overview files are stored
    pub overviews: PathBuf,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
}

#[derive(Debug)]
pub struct EbvPortalDataProvider<D: GeoEngineDb> {
    pub name: String,
    pub description: String,
    pub ebv_api: EbvPortalApi,
    pub netcdf_cf_provider: NetCdfCfDataProvider<D>,
}

#[async_trait]
impl<D: GeoEngineDb> DataProviderDefinition<D> for EbvPortalDataProviderDefinition {
    async fn initialize(self: Box<Self>, db: D) -> crate::error::Result<Box<dyn DataProvider>> {
        let id = DataProviderDefinition::<D>::id(&*self);
        #[allow(clippy::used_underscore_items)] // TODO: maybe rename?
        Ok(Box::new(EbvPortalDataProvider {
            name: self.name.clone(),
            description: self.description.clone(),
            ebv_api: EbvPortalApi::new(self.base_url),
            netcdf_cf_provider: Box::new(NetCdfCfDataProviderDefinition {
                name: self.name,
                description: self.description,
                data: self.data,
                overviews: self.overviews,
                cache_ttl: self.cache_ttl,
                priority: self.priority,
            })
            ._initialize(id, db),
        }))
    }

    fn type_name(&self) -> &'static str {
        "EbvPortalProviderDefinition"
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DataProviderId {
        EBV_PROVIDER_ID
    }

    fn priority(&self) -> i16 {
        self.priority.unwrap_or(0)
    }
}

#[async_trait]
impl<D: GeoEngineDb> DataProvider for EbvPortalDataProvider<D> {
    async fn provenance(
        &self,
        id: &DataId,
    ) -> crate::error::Result<crate::datasets::listing::ProvenanceOutput> {
        self.netcdf_cf_provider.provenance(id).await
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum EbvCollectionId {
    Classes,
    Class {
        class: String,
    },
    Ebv {
        class: String,
        ebv: String,
    },
    Dataset {
        class: String,
        ebv: String,
        dataset: String,
    },
    Group {
        class: String,
        ebv: String,
        dataset: String,
        groups: Vec<String>,
    },
    Entity {
        class: String,
        ebv: String,
        dataset: String,
        groups: Vec<String>,
        entity: usize,
    },
}

impl FromStr for EbvCollectionId {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let split = s.split('/').collect::<Vec<_>>();

        Ok(match *split.as_slice() {
            ["classes"] => EbvCollectionId::Classes,
            ["classes", class] => EbvCollectionId::Class {
                class: class.to_string(),
            },
            ["classes", class, ebv] => EbvCollectionId::Ebv {
                class: class.to_string(),
                ebv: ebv.to_string(),
            },
            ["classes", class, ebv, dataset] => EbvCollectionId::Dataset {
                class: class.to_string(),
                ebv: ebv.to_string(),
                dataset: dataset.to_string(),
            },
            ["classes", class, ebv, dataset, .., entity] if entity.ends_with(".entity") => {
                EbvCollectionId::Entity {
                    class: class.to_string(),
                    ebv: ebv.to_string(),
                    dataset: dataset.to_string(),
                    groups: split[4..split.len() - 1]
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                    entity: entity[0..entity.len() - ".entity".len()]
                        .parse()
                        .map_err(|_| crate::error::Error::InvalidLayerCollectionId)?,
                }
            }
            ["classes", class, ebv, dataset, ..] => EbvCollectionId::Group {
                class: class.to_string(),
                ebv: ebv.to_string(),
                dataset: dataset.to_string(),
                groups: split[4..split.len()]
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            },
            _ => return Err(crate::error::Error::InvalidLayerCollectionId),
        })
    }
}

impl TryFrom<EbvCollectionId> for LayerCollectionId {
    type Error = crate::error::Error;

    fn try_from(id: EbvCollectionId) -> Result<Self> {
        let s = match id {
            EbvCollectionId::Classes => "classes".to_string(),
            EbvCollectionId::Class { class } => format!("classes/{class}"),
            EbvCollectionId::Ebv { class, ebv } => format!("classes/{class}/{ebv}"),
            EbvCollectionId::Dataset {
                class,
                ebv,
                dataset,
            } => format!("classes/{class}/{ebv}/{dataset}"),
            EbvCollectionId::Group {
                class,
                ebv,
                dataset,
                groups,
            } => format!("classes/{}/{}/{}/{}", class, ebv, dataset, groups.join("/")),
            EbvCollectionId::Entity { .. } => {
                return Err(crate::error::Error::InvalidLayerCollectionId)
            }
        };

        Ok(LayerCollectionId(s))
    }
}

impl TryFrom<EbvCollectionId> for LayerId {
    type Error = crate::error::Error;

    fn try_from(id: EbvCollectionId) -> Result<Self> {
        let s = match id {
            EbvCollectionId::Entity {
                class,
                ebv,
                dataset,
                groups,
                entity,
            } => format!(
                "classes/{}/{}/{}/{}/{}.entity",
                class,
                ebv,
                dataset,
                groups.join("/"),
                entity
            ),
            _ => return Err(crate::error::Error::InvalidLayerId),
        };

        Ok(LayerId(s))
    }
}

impl<D: GeoEngineDb> EbvPortalDataProvider<D> {
    async fn get_classes_collection(
        &self,
        collection: &LayerCollectionId,
        options: &LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
        let items = self
            .ebv_api
            .get_classes()
            .await?
            .into_iter()
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .map(|c| {
                Ok(CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: EBV_PROVIDER_ID,
                        collection_id: EbvCollectionId::Class {
                            class: c.name.clone(),
                        }
                        .try_into()?,
                    },
                    name: c.name,
                    description: String::new(),
                    properties: Default::default(),
                }))
            })
            .collect::<Result<Vec<CollectionItem>>>()?;

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: EBV_PROVIDER_ID,
                collection_id: collection.clone(),
            },
            name: self.name.clone(),
            description: "EbvPortalProviderDefinition".to_string(),
            items,
            entry_label: Some("EBV Class".to_string()),
            properties: vec![],
        })
    }

    async fn get_class_collections(
        &self,
        collection: &LayerCollectionId,
        class: &str,
        options: &LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
        let items = self
            .ebv_api
            .get_classes()
            .await?
            .into_iter()
            .find(|c| c.name == class)
            .ok_or(Error::UnknownLayerCollectionId {
                id: collection.clone(),
            })?
            .ebv_names
            .into_iter()
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .map(|ebv| {
                Ok(CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: EBV_PROVIDER_ID,
                        collection_id: EbvCollectionId::Ebv {
                            class: class.to_string(),
                            ebv: ebv.clone(),
                        }
                        .try_into()?,
                    },
                    name: ebv,
                    description: String::new(),
                    properties: Default::default(),
                }))
            })
            .collect::<Result<Vec<CollectionItem>>>()?;

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: EBV_PROVIDER_ID,
                collection_id: collection.clone(),
            },
            name: class.to_string(),
            description: String::new(),
            items,
            entry_label: Some("EBV Name".to_string()),
            properties: vec![],
        })
    }

    async fn get_ebv_collection(
        &self,
        collection: &LayerCollectionId,
        class: &str,
        ebv: &str,
        options: &LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
        let items = self
            .ebv_api
            .get_ebv_datasets(ebv)
            .await?
            .into_iter()
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .map(|d| {
                Ok(CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: EBV_PROVIDER_ID,
                        collection_id: EbvCollectionId::Dataset {
                            class: class.to_string(),
                            ebv: ebv.to_string(),
                            dataset: d.id,
                        }
                        .try_into()?,
                    },
                    name: d.name.clone(),
                    description: d.description,
                    properties: Default::default(),
                }))
            })
            .collect::<Result<Vec<CollectionItem>>>()?;

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: EBV_PROVIDER_ID,
                collection_id: collection.clone(),
            },
            name: ebv.to_string(),
            description: String::new(),
            items,
            entry_label: Some("EBV Dataset".to_string()),
            properties: vec![],
        })
    }

    async fn dataset_collection_of_items(
        &self,
        collection: &LayerCollectionId,
        ebv: &str,
        dataset_id: &str,
        items: Vec<CollectionItem>,
    ) -> Result<LayerCollection> {
        let dataset = self
            .ebv_api
            .get_ebv_datasets(ebv)
            .await?
            .into_iter()
            .find(|d| d.id == dataset_id)
            .ok_or(Error::UnknownLayerCollectionId {
                id: collection.clone(),
            })?;

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: EBV_PROVIDER_ID,
                collection_id: collection.clone(),
            },
            name: dataset.name.to_string(),
            description: dataset.description,
            items,
            entry_label: dataset
                .has_scenario
                .then_some("Scenario".to_string())
                .or_else(|| Some("Metric".to_string())),
            properties: [
                (
                    "by".to_string(),
                    format!("{} ({})", dataset.author_name, dataset.author_institution),
                )
                    .into(),
                ("with license".to_string(), dataset.license).into(),
            ]
            .into_iter()
            .collect(),
        })
    }
}

#[async_trait]
impl<D: GeoEngineDb> LayerCollectionProvider for EbvPortalDataProvider<D> {
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
        options: LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
        let id: EbvCollectionId = EbvCollectionId::from_str(&collection.0)?;

        Ok(match id {
            EbvCollectionId::Classes => self.get_classes_collection(collection, &options).await?,
            EbvCollectionId::Class { class } => {
                self.get_class_collections(collection, &class, &options)
                    .await?
            }

            EbvCollectionId::Ebv { class, ebv } => {
                self.get_ebv_collection(collection, &class, &ebv, &options)
                    .await?
            }
            EbvCollectionId::Dataset {
                class,
                ebv,
                dataset,
            } => {
                let dataset_path = self
                    .ebv_api
                    .get_dataset_metadata(&dataset)
                    .await
                    .map(|dataset| PathBuf::from(dataset.dataset_path.trim_start_matches('/')))?;

                let layer_collection_id = LayerCollectionId(path_to_string(&dataset_path));

                #[allow(clippy::used_underscore_items)] // TODO: maybe rename?
                let layer_collection = self
                    .netcdf_cf_provider
                    ._load_layer_collection(
                        &layer_collection_id,
                        options,
                        EbvPortalIdFn::new(class.clone(), ebv.clone(), dataset.clone()),
                    )
                    .await?;

                self.dataset_collection_of_items(collection, &ebv, &dataset, layer_collection.items)
                    .await?
            }
            EbvCollectionId::Group {
                class,
                ebv,
                dataset,
                groups,
            } => {
                let dataset_path = self
                    .ebv_api
                    .get_dataset_metadata(&dataset)
                    .await
                    .map(|dataset| PathBuf::from(dataset.dataset_path.trim_start_matches('/')))?;

                let layer_collection_id =
                    netcdf_group_to_layer_collection_id(&dataset_path, &groups);

                #[allow(clippy::used_underscore_items)] // TODO: maybe rename?
                let mut layer_collection = self
                    .netcdf_cf_provider
                    ._load_layer_collection(
                        &layer_collection_id,
                        options,
                        EbvPortalIdFn::new(class.clone(), ebv.clone(), dataset.clone()),
                    )
                    .await?;

                layer_collection.id = ProviderLayerCollectionId {
                    provider_id: EBV_PROVIDER_ID,
                    collection_id: collection.clone(),
                };
                layer_collection.entry_label = layer_collection
                    .items
                    .first()
                    .map_or(true, |item| matches!(item, CollectionItem::Layer(_)))
                    .then_some("Entity".to_string())
                    .or_else(|| Some("Metric".to_string()));

                layer_collection
            }
            EbvCollectionId::Entity { .. } => return Err(Error::InvalidLayerCollectionId),
        })
    }

    async fn get_root_layer_collection_id(&self) -> Result<LayerCollectionId> {
        EbvCollectionId::Classes.try_into()
    }

    async fn load_layer(&self, id: &LayerId) -> Result<Layer> {
        let ebv_id: EbvCollectionId = EbvCollectionId::from_str(&id.0)?;

        let EbvCollectionId::Entity {
            class: _,
            ebv: _,
            dataset,
            groups,
            entity,
        } = ebv_id
        else {
            return Err(Error::InvalidLayerId);
        };

        let dataset_path = self
            .ebv_api
            .get_dataset_metadata(&dataset)
            .await
            .map(|dataset| PathBuf::from(dataset.dataset_path.trim_start_matches('/')))?;

        let layer_id = netcdf_entity_to_layer_id(&dataset_path, &groups, entity);

        let layer = self.netcdf_cf_provider.load_layer(&layer_id).await?;

        Ok(layer)
    }
}

struct EbvPortalIdFn {
    class: String,
    ebv: String,
    dataset: String,
}

impl EbvPortalIdFn {
    pub fn new(class: String, ebv: String, dataset: String) -> Self {
        Self {
            class,
            ebv,
            dataset,
        }
    }
}

impl LayerCollectionIdFn for EbvPortalIdFn {
    fn layer_collection_id(&self, _file_name: &Path, group_path: &[String]) -> LayerCollectionId {
        if group_path.is_empty() {
            EbvCollectionId::Dataset {
                class: self.class.clone(),
                ebv: self.ebv.clone(),
                dataset: self.dataset.clone(),
            }
        } else {
            EbvCollectionId::Group {
                class: self.class.clone(),
                ebv: self.ebv.clone(),
                dataset: self.dataset.clone(),
                groups: group_path.to_vec(),
            }
        }
        .try_into()
        .unwrap_or_else(
            |_| LayerCollectionId(String::new()), // must not happen, but cover this defensively
        )
    }

    fn layer_id(&self, _file_name: &Path, group_path: &[String], entity_id: usize) -> LayerId {
        EbvCollectionId::Entity {
            class: self.class.clone(),
            ebv: self.ebv.clone(),
            dataset: self.dataset.clone(),
            groups: group_path.to_vec(),
            entity: entity_id,
        }
        .try_into()
        .unwrap_or_else(
            |_| LayerId(String::new()), // must not happen, but cover this defensively
        )
    }
}

fn _net_cdf_layer_collection_id_to_ebv_collection_id(
    id: NetCdfLayerCollectionId,
    class: String,
    ebv: String,
    dataset: String,
) -> EbvCollectionId {
    match id {
        NetCdfLayerCollectionId::Path { path: _ } => EbvCollectionId::Dataset {
            class,
            ebv,
            dataset,
        },
        NetCdfLayerCollectionId::Group { path: _, groups } => EbvCollectionId::Group {
            class,
            ebv,
            dataset,
            groups,
        },
        NetCdfLayerCollectionId::Entity {
            path: _,
            groups,
            entity,
        } => EbvCollectionId::Entity {
            class,
            ebv,
            dataset,
            groups,
            entity,
        },
    }
}

#[async_trait]
impl<D: GeoEngineDb> MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for EbvPortalDataProvider<D>
{
    async fn meta_data(
        &self,
        id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        self.netcdf_cf_provider.meta_data(id).await
    }
}

#[async_trait]
impl<D: GeoEngineDb>
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for EbvPortalDataProvider<D>
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
impl<D: GeoEngineDb>
    MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for EbvPortalDataProvider<D>
{
    async fn meta_data(
        &self,
        _id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        contexts::SessionContext,
        ge_context,
        layers::layer::{LayerListing, ProviderLayerId},
        pro::contexts::{PostgresSessionContext, ProPostgresContext},
    };
    use geoengine_datatypes::test_data;
    use httptest::{matchers::request, responders::status_code, Expectation};
    use std::str::FromStr;
    use tokio_postgres::NoTls;

    #[test]
    fn it_parses_layer_collection_ids() {
        assert!(matches!(
            EbvCollectionId::from_str("classes"),
            Ok(EbvCollectionId::Classes)
        ));

        assert!(matches!(
            EbvCollectionId::from_str("classes/FooClass"),
            Ok(EbvCollectionId::Class { class }) if class == "FooClass"
        ));

        assert!(matches!(
            EbvCollectionId::from_str("classes/FooClass/BarEbv"),
            Ok(EbvCollectionId::Ebv { class, ebv }) if class == "FooClass" && ebv == "BarEbv"
        ));

        assert!(matches!(
            EbvCollectionId::from_str("classes/FooClass/BarEbv/10"),
            Ok(EbvCollectionId::Dataset { class, ebv, dataset }) if class == "FooClass" && ebv == "BarEbv" && dataset == "10"
        ));

        assert!(matches!(
            EbvCollectionId::from_str("classes/FooClass/BarEbv/10/group1"),
            Ok(EbvCollectionId::Group { class, ebv, dataset, groups }) if class == "FooClass" && ebv == "BarEbv" && dataset == "10" && groups == ["group1"]
        ));

        assert!(matches!(
            EbvCollectionId::from_str("classes/FooClass/BarEbv/10/group1/group2"),
            Ok(EbvCollectionId::Group { class, ebv, dataset, groups }) if class == "FooClass" && ebv == "BarEbv" && dataset == "10" && groups == ["group1", "group2"]
        ));

        assert!(matches!(
            EbvCollectionId::from_str("classes/FooClass/BarEbv/10/group1/group2/group3"),
            Ok(EbvCollectionId::Group { class, ebv, dataset, groups }) if class == "FooClass" && ebv == "BarEbv" && dataset == "10" && groups == ["group1", "group2", "group3"]
        ));

        assert!(matches!(
            EbvCollectionId::from_str("classes/FooClass/BarEbv/10/group1/group2/group3/7.entity"),
            Ok(EbvCollectionId::Entity { class, ebv, dataset, groups, entity }) if class == "FooClass" && ebv == "BarEbv" && dataset == "10" && groups == ["group1", "group2", "group3"] && entity == 7
        ));

        assert!(matches!(
            EbvCollectionId::from_str("classes/FooClass/BarEbv/10/7.entity"),
            Ok(EbvCollectionId::Entity { class, ebv, dataset, groups, entity }) if class == "FooClass" && ebv == "BarEbv" && dataset == "10" && groups.is_empty() && entity == 7
        ));
    }

    #[test]
    fn it_serializes_layer_collection_ids() {
        let id: LayerCollectionId = EbvCollectionId::Classes.try_into().unwrap();
        assert_eq!(id.to_string(), "classes");

        let id: LayerCollectionId = EbvCollectionId::Class {
            class: "FooClass".to_string(),
        }
        .try_into()
        .unwrap();
        assert_eq!(id.to_string(), "classes/FooClass");

        let id: LayerCollectionId = EbvCollectionId::Ebv {
            class: "FooClass".to_string(),
            ebv: "BarEbv".to_string(),
        }
        .try_into()
        .unwrap();
        assert_eq!(id.to_string(), "classes/FooClass/BarEbv");

        let id: LayerCollectionId = EbvCollectionId::Dataset {
            class: "FooClass".to_string(),
            ebv: "BarEbv".to_string(),
            dataset: "10".to_string(),
        }
        .try_into()
        .unwrap();
        assert_eq!(id.to_string(), "classes/FooClass/BarEbv/10");

        let id: LayerCollectionId = EbvCollectionId::Group {
            class: "FooClass".to_string(),
            ebv: "BarEbv".to_string(),
            dataset: "10".to_string(),
            groups: vec!["group1".to_string()],
        }
        .try_into()
        .unwrap();
        assert_eq!(id.to_string(), "classes/FooClass/BarEbv/10/group1");

        let id: LayerId = EbvCollectionId::Entity {
            class: "FooClass".to_string(),
            ebv: "BarEbv".to_string(),
            dataset: "10".to_string(),
            groups: vec!["group1".to_string()],
            entity: 7,
        }
        .try_into()
        .unwrap();
        assert_eq!(id.to_string(), "classes/FooClass/BarEbv/10/group1/7.entity");
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn test_get_classes(
        _app_ctx: ProPostgresContext<NoTls>,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        let mock_server = httptest::Server::run();
        mock_server.expect(
            Expectation::matching(request::method_path("GET", "/api/v1/ebv-map")).respond_with(
                status_code(200)
                    .append_header("Content-Type", "application/json")
                    .body(
                        r#"{
                    "code": 200,
                    "message": "List of all EBV classes and names.",
                    "data": [
                        {
                            "ebv_class": "Community composition",
                            "ebv_name": [
                                "Community abundance",
                                "Taxonomic and phylogenetic diversity"
                            ]
                        },
                        {
                            "ebv_class": "Ecosystem functioning",
                            "ebv_name": [
                                "Ecosystem phenology",
                                "Primary productivity"
                            ]
                        },
                        {
                            "ebv_class": "Ecosystem structure",
                            "ebv_name": [
                                "Ecosystem distribution"
                            ]
                        },
                        {
                            "ebv_class": "Species populations",
                            "ebv_name": [
                                "Species distributions"
                            ]
                        }
                    ]
                }"#,
                    ),
            ),
        );

        let provider = Box::new(EbvPortalDataProviderDefinition {
            name: "EBV Portal".to_string(),
            description: "EbvPortalProviderDefinition".to_string(),
            priority: None,
            data: test_data!("netcdf4d").into(),
            base_url: Url::parse(&mock_server.url_str("/api/v1")).unwrap(),
            overviews: test_data!("netcdf4d/overviews").into(),
            cache_ttl: Default::default(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let root_id = provider.get_root_layer_collection_id().await.unwrap();

        let collection = provider
            .load_layer_collection(
                &root_id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: EBV_PROVIDER_ID,
                    collection_id: root_id,
                },
                name: "EBV Portal".to_string(),
                description: "EbvPortalProviderDefinition".to_string(),
                items: vec![
                    CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: DataProviderId::from_str(
                                "77d0bf11-986e-43f5-b11d-898321f1854c"
                            )
                            .unwrap(),
                            collection_id: LayerCollectionId(
                                "classes/Community composition".into()
                            )
                        },
                        name: "Community composition".to_string(),
                        description: String::new(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: DataProviderId::from_str(
                                "77d0bf11-986e-43f5-b11d-898321f1854c"
                            )
                            .unwrap(),
                            collection_id: LayerCollectionId(
                                "classes/Ecosystem functioning".into()
                            )
                        },
                        name: "Ecosystem functioning".to_string(),
                        description: String::new(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: DataProviderId::from_str(
                                "77d0bf11-986e-43f5-b11d-898321f1854c"
                            )
                            .unwrap(),
                            collection_id: LayerCollectionId("classes/Ecosystem structure".into())
                        },
                        name: "Ecosystem structure".to_string(),
                        description: String::new(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: DataProviderId::from_str(
                                "77d0bf11-986e-43f5-b11d-898321f1854c"
                            )
                            .unwrap(),
                            collection_id: LayerCollectionId("classes/Species populations".into())
                        },
                        name: "Species populations".to_string(),
                        description: String::new(),
                        properties: Default::default(),
                    })
                ],
                entry_label: Some("EBV Class".to_string()),
                properties: vec![],
            }
        );
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn test_get_class(
        _app_ctx: ProPostgresContext<NoTls>,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        let mock_server = httptest::Server::run();
        mock_server.expect(
            Expectation::matching(request::method_path("GET", "/api/v1/ebv-map")).respond_with(
                status_code(200)
                    .append_header("Content-Type", "application/json")
                    .body(
                        r#"{
                    "code": 200,
                    "message": "List of all EBV classes and names.",
                    "data": [
                        {
                            "ebv_class": "Community composition",
                            "ebv_name": [
                                "Community abundance",
                                "Taxonomic and phylogenetic diversity"
                            ]
                        },
                        {
                            "ebv_class": "Ecosystem functioning",
                            "ebv_name": [
                                "Ecosystem phenology",
                                "Primary productivity"
                            ]
                        },
                        {
                            "ebv_class": "Ecosystem structure",
                            "ebv_name": [
                                "Ecosystem distribution"
                            ]
                        },
                        {
                            "ebv_class": "Species populations",
                            "ebv_name": [
                                "Species distributions"
                            ]
                        }
                    ]
                }"#,
                    ),
            ),
        );

        let provider = Box::new(EbvPortalDataProviderDefinition {
            name: "EBV Portal".to_string(),
            description: "EbvPortalProviderDefinition".to_string(),
            priority: None,
            data: test_data!("netcdf4d").into(),
            base_url: Url::parse(&mock_server.url_str("/api/v1")).unwrap(),
            overviews: test_data!("netcdf4d/overviews").into(),
            cache_ttl: Default::default(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let id = LayerCollectionId("classes/Ecosystem functioning".into());
        let collection = provider
            .load_layer_collection(
                &id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: EBV_PROVIDER_ID,
                    collection_id: id,
                },
                name: "Ecosystem functioning".to_string(),
                description: String::new(),
                items: vec![
                    CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: DataProviderId::from_str(
                                "77d0bf11-986e-43f5-b11d-898321f1854c"
                            )
                            .unwrap(),
                            collection_id: LayerCollectionId(
                                "classes/Ecosystem functioning/Ecosystem phenology".into()
                            )
                        },
                        name: "Ecosystem phenology".to_string(),
                        description: String::new(),
                        properties: Default::default(),
                    }),
                    CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: DataProviderId::from_str(
                                "77d0bf11-986e-43f5-b11d-898321f1854c"
                            )
                            .unwrap(),
                            collection_id: LayerCollectionId(
                                "classes/Ecosystem functioning/Primary productivity".into()
                            )
                        },
                        name: "Primary productivity".to_string(),
                        description: String::new(),
                        properties: Default::default(),
                    })
                ],
                entry_label: Some("EBV Name".to_string()),
                properties: vec![],
            }
        );
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn test_get_ebv(_app_ctx: ProPostgresContext<NoTls>, ctx: PostgresSessionContext<NoTls>) {
        let mock_server = httptest::Server::run();

        mock_server.expect(
            Expectation::matching(request::method_path("GET", "/api/v1/datasets/filter")).respond_with(
                status_code(200)
                    .append_header("Content-Type", "application/json")
                    .body(r#"{
            "code": 200,
            "message": "List of dataset(s).",
            "data": [
                {
                    "id": "10",
                    "naming_authority": "The German Centre for Integrative Biodiversity Research (iDiv) Halle-Jena-Leipzig",
                    "title": "Vegetation Phenology in Finland",
                    "date_created": "2020-11-04",
                    "date_issued": "2022-02-25",
                    "summary": "Datasets present the yearly maps of the start of vegetation active period (VAP) in coniferous forests and deciduous vegetation during 2001-2019 in Finland. The start of the vegetation active period is defined as the day when coniferous trees start to photosynthesize and for deciduous vegetation as the day when trees unfold new leaves in spring. The datasets were derived from satellite observations of the Moderate Resolution Imaging Spectroradiometer (MODIS).",
                    "references": [
                        "10.1016\/j.rse.2013.09.022",
                        "10.3390\/rs8070580"
                    ],
                    "source": "Moderate Resolution Imaging Spectrometer (MODIS) Terra Level 1B (1 km, 500 m) were manually selected from the Level-1 and Atmosphere Archive and Distribution System (LAADS DAAC). From 2009 onwards data were obtained from the satellite receiving station of the Finnish Meteorological Institute (FMI) in Sodankyl\u00e4, Finland and gap-filled with data from LAADS DAAC. MODIS Level 1B data were calibrated to top-of-atmosphere reflectances and projected to a geographic latitude\/longitude grid (datum WGS-84) using the software envimon by Technical Research Centre of Finland (VTT). Fractional Snow Cover (FSC) and the Normalized Difference Water Index (NDWI) were calculated from MODIS top-of-atmosphere reflectances. Cloud covered observations were removed using an automatic cloud masking algorithm by the Finnish Environment Institute.\r\n For the extraction of the start of the VAP in coniferous forest, FSC was averaged at a spatial resolution of 0.05 x 0.05 degrees for the MODIS pixels with dominant coverage of coniferous forest. A sigmoid function was fitted to the averaged FSC-time series and the start of the VAP was determined based on a threshold value. For the extraction of the VAP in deciduous vegetation, daily NDWI time series were averaged for MODIS pixels with vegetation cover into the same spatial grid (0.05 x 0.05 degrees). The day of the VAP was determined from NDWI time series based on a threshold value. The yearly maps of the VAP were smoothed with a median filter to remove spurious outliers and fill spatial gaps. Open water areas were masked.",
                    "coverage_content_type": "modelResult",
                    "project": "MONIMET, SnowCarbo",
                    "project_url": [
                        "https:\/\/monimet.fmi.fi\/index.php?style=warm"
                    ],
                    "creator": {
                        "creator_name": "Kristin B\u00f6ttcher",
                        "creator_email": "Kristin.Bottcher@ymparisto.fi",
                        "creator_institution": "The Finnish Environment Institute (SYKE)",
                        "creator_country": "Finland"
                    },
                    "contributor_name": "N\/A",
                    "license": "https:\/\/creativecommons.org\/licenses\/by\/4.0",
                    "publisher": {
                        "publisher_name": "Kristin B\u00f6ttcher",
                        "publisher_email": "Kristin.Bottcher@ymparisto.fi",
                        "publisher_institution": "Finnish Environment Institute",
                        "publisher_country": "Finland"
                    },
                    "ebv": {
                        "ebv_class": "Ecosystem functioning",
                        "ebv_name": "Ecosystem phenology"
                    },
                    "ebv_entity": {
                        "ebv_entity_type": "Ecosystems",
                        "ebv_entity_scope": "Vegetation",
                        "ebv_entity_classification_name": "N\/A",
                        "ebv_entity_classification_url": "N\/A"
                    },
                    "ebv_metric": {
                        "ebv_metric_1": {
                            ":standard_name": "Phenology Coniferous",
                            ":long_name": "The data set consists of yearly maps of the start of the vegetation active period (VAP) in coniferous forest (Day of Year), which is defined as the day when coniferous trees start to photosynthesize in spring. The data set was derived from Moderate Resolution Imaging Spetroradiometer (MODIS) satellite observation of Fractional Snow Cover. The day when snow cover decreases during spring melt was used as a proxy indicator for the beginning of the start of the vegetation active period.",
                            ":units": "Day of year"
                        },
                        "ebv_metric_2": {
                            ":standard_name": "Phenology Deciduous",
                            ":long_name": "The data set consists of yearly maps of the start of the vegetation active period (VAP) in deciduous vegetation (Day of Year), which is defined as the day when deciduous trees unfold new leaves in spring. It is also often referred to as the green-up or greening day. The data set was derived from time series of the Normalized Difference Water Index (NDWI) calculated from Moderate Resolution Imaging Spetroradiometer (MODIS) satellite observation.",
                            ":units": "Day of year"
                        }
                    },
                    "ebv_scenario": "N\/A",
                    "ebv_geospatial": {
                        "ebv_geospatial_scope": "National",
                        "ebv_geospatial_description": "Finland"
                    },
                    "geospatial_lat_resolution": "0.066428063829064 degrees",
                    "geospatial_lon_resolution": "0.066428063829064 degrees",
                    "geospatial_bounds_crs": "EPSG:4326",
                    "geospatial_lat_min": "57.7532613743371",
                    "geospatial_lon_min": "14.1011663430375",
                    "geospatial_lat_max": "71.171730267808",
                    "geospatial_lon_max": "35.2252906406799",
                    "time_coverage": {
                        "time_coverage_resolution": "P0001-00-00",
                        "time_coverage_start": "2001-01-01",
                        "time_coverage_end": "2019-01-01"
                    },
                    "ebv_domain": "Terrestrial",
                    "comment": "The products were compared with ground observations. The start of the VAP in coniferous forest was well correlated with the day when the Gross Primary Production (GPP) exceeded 15% of its summer maximum at 3 eddy covariance measurement sites in Finland (R2=0.7). The accuracy was 9 days for the period 2001-2016. The satellite product was in average 3 days late compared to the ground observations. The accuracy was higher (6 days, R2=0.84) and no bias was observed in pine forest compared to spruce forest that showed larger deviations to ground observations. The start of the VAP in deciduous vegetation corresponded well with visual observations of the bud break of birch from the phenological network of the Natural Resource Institute of Finland (Luke). The accuracy was 7 days for the period 2001-2015 based on 84 site-years. The bias was negligible (0.4 days).",
                    "dataset": {
                        "pathname": "\/10\/public\/bottcher_ecofun_id10_20220215_v1.nc",
                        "download": "portal.geobon.org\/data\/upload\/10\/public\/bottcher_ecofun_id10_20220215_v1.nc",
                        "metadata_json": "portal.geobon.org\/data\/upload\/10\/public\/metadata_v1.json",
                        "metadata_xml": "portal.geobon.org\/data\/upload\/10\/public\/metadata_v1.xml"
                    },
                    "file": {
                        "download": "portal.geobon.org\/img\/10\/phenology-maps.jpg"
                    }
                }
            ]
        }"#)));

        let provider = Box::new(EbvPortalDataProviderDefinition {
            name: "EBV Portal".to_string(),
            description: "EbvPortalProviderDefinition".to_string(),
            priority: None,
            data: test_data!("netcdf4d").into(),
            base_url: Url::parse(&mock_server.url_str("/api/v1")).unwrap(),
            overviews: test_data!("netcdf4d/overviews").into(),
            cache_ttl: Default::default(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let id = LayerCollectionId("classes/Ecosystem functioning/Ecosystem phenology".into());

        let collection = provider
            .load_layer_collection(
                &id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                },
            )
            .await
            .unwrap();

        assert_eq!(collection, LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: EBV_PROVIDER_ID,
                collection_id: id,
            },
            name: "Ecosystem phenology".to_string(),
            description: String::new(),
            items: vec![
                CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: DataProviderId::from_str("77d0bf11-986e-43f5-b11d-898321f1854c").unwrap(),
                        collection_id: LayerCollectionId("classes/Ecosystem functioning/Ecosystem phenology/10".into())
                    },
                    name: "Vegetation Phenology in Finland".to_string(),
                    description: "Datasets present the yearly maps of the start of vegetation active period (VAP) in coniferous forests and deciduous vegetation during 2001-2019 in Finland. The start of the vegetation active period is defined as the day when coniferous trees start to photosynthesize and for deciduous vegetation as the day when trees unfold new leaves in spring. The datasets were derived from satellite observations of the Moderate Resolution Imaging Spectroradiometer (MODIS).".to_string(),
                    properties: Default::default(),
                })],
            entry_label: Some("EBV Dataset".to_string()),
            properties: vec![],
        });
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn test_get_dataset(
        _app_ctx: ProPostgresContext<NoTls>,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        let mock_server = httptest::Server::run();

        mock_server.expect(
            Expectation::matching(request::method_path("GET", "/api/v1/datasets/filter")).respond_with(
                status_code(200)
                    .append_header("Content-Type", "application/json")
                    .body(r#"{
            "code": 200,
            "message": "List of dataset(s).",
            "data": [
                {
                    "id": "10",
                    "naming_authority": "The German Centre for Integrative Biodiversity Research (iDiv) Halle-Jena-Leipzig",
                    "title": "Vegetation Phenology in Finland",
                    "date_created": "2020-11-04",
                    "date_issued": "2022-02-25",
                    "summary": "Datasets present the yearly maps of the start of vegetation active period (VAP) in coniferous forests and deciduous vegetation during 2001-2019 in Finland. The start of the vegetation active period is defined as the day when coniferous trees start to photosynthesize and for deciduous vegetation as the day when trees unfold new leaves in spring. The datasets were derived from satellite observations of the Moderate Resolution Imaging Spectroradiometer (MODIS).",
                    "references": [
                        "10.1016\/j.rse.2013.09.022",
                        "10.3390\/rs8070580"
                    ],
                    "source": "Moderate Resolution Imaging Spectrometer (MODIS) Terra Level 1B (1 km, 500 m) were manually selected from the Level-1 and Atmosphere Archive and Distribution System (LAADS DAAC). From 2009 onwards data were obtained from the satellite receiving station of the Finnish Meteorological Institute (FMI) in Sodankyl\u00e4, Finland and gap-filled with data from LAADS DAAC. MODIS Level 1B data were calibrated to top-of-atmosphere reflectances and projected to a geographic latitude\/longitude grid (datum WGS-84) using the software envimon by Technical Research Centre of Finland (VTT). Fractional Snow Cover (FSC) and the Normalized Difference Water Index (NDWI) were calculated from MODIS top-of-atmosphere reflectances. Cloud covered observations were removed using an automatic cloud masking algorithm by the Finnish Environment Institute.\r\n For the extraction of the start of the VAP in coniferous forest, FSC was averaged at a spatial resolution of 0.05 x 0.05 degrees for the MODIS pixels with dominant coverage of coniferous forest. A sigmoid function was fitted to the averaged FSC-time series and the start of the VAP was determined based on a threshold value. For the extraction of the VAP in deciduous vegetation, daily NDWI time series were averaged for MODIS pixels with vegetation cover into the same spatial grid (0.05 x 0.05 degrees). The day of the VAP was determined from NDWI time series based on a threshold value. The yearly maps of the VAP were smoothed with a median filter to remove spurious outliers and fill spatial gaps. Open water areas were masked.",
                    "coverage_content_type": "modelResult",
                    "project": "MONIMET, SnowCarbo",
                    "project_url": [
                        "https:\/\/monimet.fmi.fi\/index.php?style=warm"
                    ],
                    "creator": {
                        "creator_name": "Kristin B\u00f6ttcher",
                        "creator_email": "Kristin.Bottcher@ymparisto.fi",
                        "creator_institution": "The Finnish Environment Institute (SYKE)",
                        "creator_country": "Finland"
                    },
                    "contributor_name": "N\/A",
                    "license": "https:\/\/creativecommons.org\/licenses\/by\/4.0",
                    "publisher": {
                        "publisher_name": "Kristin B\u00f6ttcher",
                        "publisher_email": "Kristin.Bottcher@ymparisto.fi",
                        "publisher_institution": "Finnish Environment Institute",
                        "publisher_country": "Finland"
                    },
                    "ebv": {
                        "ebv_class": "Ecosystem functioning",
                        "ebv_name": "Ecosystem phenology"
                    },
                    "ebv_entity": {
                        "ebv_entity_type": "Ecosystems",
                        "ebv_entity_scope": "Vegetation",
                        "ebv_entity_classification_name": "N\/A",
                        "ebv_entity_classification_url": "N\/A"
                    },
                    "ebv_metric": {
                        "ebv_metric_1": {
                            ":standard_name": "Phenology Coniferous",
                            ":long_name": "The data set consists of yearly maps of the start of the vegetation active period (VAP) in coniferous forest (Day of Year), which is defined as the day when coniferous trees start to photosynthesize in spring. The data set was derived from Moderate Resolution Imaging Spetroradiometer (MODIS) satellite observation of Fractional Snow Cover. The day when snow cover decreases during spring melt was used as a proxy indicator for the beginning of the start of the vegetation active period.",
                            ":units": "Day of year"
                        },
                        "ebv_metric_2": {
                            ":standard_name": "Phenology Deciduous",
                            ":long_name": "The data set consists of yearly maps of the start of the vegetation active period (VAP) in deciduous vegetation (Day of Year), which is defined as the day when deciduous trees unfold new leaves in spring. It is also often referred to as the green-up or greening day. The data set was derived from time series of the Normalized Difference Water Index (NDWI) calculated from Moderate Resolution Imaging Spetroradiometer (MODIS) satellite observation.",
                            ":units": "Day of year"
                        }
                    },
                    "ebv_scenario": "N\/A",
                    "ebv_geospatial": {
                        "ebv_geospatial_scope": "National",
                        "ebv_geospatial_description": "Finland"
                    },
                    "geospatial_lat_resolution": "0.066428063829064 degrees",
                    "geospatial_lon_resolution": "0.066428063829064 degrees",
                    "geospatial_bounds_crs": "EPSG:4326",
                    "geospatial_lat_min": "57.7532613743371",
                    "geospatial_lon_min": "14.1011663430375",
                    "geospatial_lat_max": "71.171730267808",
                    "geospatial_lon_max": "35.2252906406799",
                    "time_coverage": {
                        "time_coverage_resolution": "P0001-00-00",
                        "time_coverage_start": "2001-01-01",
                        "time_coverage_end": "2019-01-01"
                    },
                    "ebv_domain": "Terrestrial",
                    "comment": "The products were compared with ground observations. The start of the VAP in coniferous forest was well correlated with the day when the Gross Primary Production (GPP) exceeded 15% of its summer maximum at 3 eddy covariance measurement sites in Finland (R2=0.7). The accuracy was 9 days for the period 2001-2016. The satellite product was in average 3 days late compared to the ground observations. The accuracy was higher (6 days, R2=0.84) and no bias was observed in pine forest compared to spruce forest that showed larger deviations to ground observations. The start of the VAP in deciduous vegetation corresponded well with visual observations of the bud break of birch from the phenological network of the Natural Resource Institute of Finland (Luke). The accuracy was 7 days for the period 2001-2015 based on 84 site-years. The bias was negligible (0.4 days).",
                    "dataset": {
                        "pathname": "\/10\/public\/bottcher_ecofun_id10_20220215_v1.nc",
                        "download": "portal.geobon.org\/data\/upload\/10\/public\/bottcher_ecofun_id10_20220215_v1.nc",
                        "metadata_json": "portal.geobon.org\/data\/upload\/10\/public\/metadata_v1.json",
                        "metadata_xml": "portal.geobon.org\/data\/upload\/10\/public\/metadata_v1.xml"
                    },
                    "file": {
                        "download": "portal.geobon.org\/img\/10\/phenology-maps.jpg"
                    }
                }
            ]
        }"#)));

        mock_server.expect(
            Expectation::matching(request::method_path("GET", "/api/v1/datasets/10")).respond_with(
                status_code(200)
                    .append_header("Content-Type", "application/json")
                    .body(r#"{
                    "code": 200,
                    "message": "List of dataset(s).",
                    "data": [
                        {
                            "id": "10",
                            "naming_authority": "The German Centre for Integrative Biodiversity Research (iDiv) Halle-Jena-Leipzig",
                            "title": "Vegetation Phenology in Finland",
                            "date_created": "2020-11-04",
                            "date_issued": "2022-02-25",
                            "summary": "Datasets present the yearly maps of the start of vegetation active period (VAP) in coniferous forests and deciduous vegetation during 2001-2019 in Finland. The start of the vegetation active period is defined as the day when coniferous trees start to photosynthesize and for deciduous vegetation as the day when trees unfold new leaves in spring. The datasets were derived from satellite observations of the Moderate Resolution Imaging Spectroradiometer (MODIS).",
                            "references": [
                                "10.1016\/j.rse.2013.09.022",
                                "10.3390\/rs8070580"
                            ],
                            "source": "Moderate Resolution Imaging Spectrometer (MODIS) Terra Level 1B (1 km, 500 m) were manually selected from the Level-1 and Atmosphere Archive and Distribution System (LAADS DAAC). From 2009 onwards data were obtained from the satellite receiving station of the Finnish Meteorological Institute (FMI) in Sodankyl\u00e4, Finland and gap-filled with data from LAADS DAAC. MODIS Level 1B data were calibrated to top-of-atmosphere reflectances and projected to a geographic latitude\/longitude grid (datum WGS-84) using the software envimon by Technical Research Centre of Finland (VTT). Fractional Snow Cover (FSC) and the Normalized Difference Water Index (NDWI) were calculated from MODIS top-of-atmosphere reflectances. Cloud covered observations were removed using an automatic cloud masking algorithm by the Finnish Environment Institute.\r\n For the extraction of the start of the VAP in coniferous forest, FSC was averaged at a spatial resolution of 0.05 x 0.05 degrees for the MODIS pixels with dominant coverage of coniferous forest. A sigmoid function was fitted to the averaged FSC-time series and the start of the VAP was determined based on a threshold value. For the extraction of the VAP in deciduous vegetation, daily NDWI time series were averaged for MODIS pixels with vegetation cover into the same spatial grid (0.05 x 0.05 degrees). The day of the VAP was determined from NDWI time series based on a threshold value. The yearly maps of the VAP were smoothed with a median filter to remove spurious outliers and fill spatial gaps. Open water areas were masked.",
                            "coverage_content_type": "modelResult",
                            "project": "MONIMET, SnowCarbo",
                            "project_url": [
                                "https:\/\/monimet.fmi.fi\/index.php?style=warm"
                            ],
                            "creator": {
                                "creator_name": "Kristin B\u00f6ttcher",
                                "creator_email": "Kristin.Bottcher@ymparisto.fi",
                                "creator_institution": "The Finnish Environment Institute (SYKE)",
                                "creator_country": "Finland"
                            },
                            "contributor_name": "N\/A",
                            "license": "https:\/\/creativecommons.org\/licenses\/by\/4.0",
                            "publisher": {
                                "publisher_name": "Kristin B\u00f6ttcher",
                                "publisher_email": "Kristin.Bottcher@ymparisto.fi",
                                "publisher_institution": "Finnish Environment Institute",
                                "publisher_country": "Finland"
                            },
                            "ebv": {
                                "ebv_class": "Ecosystem functioning",
                                "ebv_name": "Ecosystem phenology"
                            },
                            "ebv_entity": {
                                "ebv_entity_type": "Ecosystems",
                                "ebv_entity_scope": "Vegetation",
                                "ebv_entity_classification_name": "N\/A",
                                "ebv_entity_classification_url": "N\/A"
                            },
                            "ebv_metric": {
                                "ebv_metric_1": {
                                    ":standard_name": "Phenology Coniferous",
                                    ":long_name": "The data set consists of yearly maps of the start of the vegetation active period (VAP) in coniferous forest (Day of Year), which is defined as the day when coniferous trees start to photosynthesize in spring. The data set was derived from Moderate Resolution Imaging Spetroradiometer (MODIS) satellite observation of Fractional Snow Cover. The day when snow cover decreases during spring melt was used as a proxy indicator for the beginning of the start of the vegetation active period.",
                                    ":units": "Day of year"
                                },
                                "ebv_metric_2": {
                                    ":standard_name": "Phenology Deciduous",
                                    ":long_name": "The data set consists of yearly maps of the start of the vegetation active period (VAP) in deciduous vegetation (Day of Year), which is defined as the day when deciduous trees unfold new leaves in spring. It is also often referred to as the green-up or greening day. The data set was derived from time series of the Normalized Difference Water Index (NDWI) calculated from Moderate Resolution Imaging Spetroradiometer (MODIS) satellite observation.",
                                    ":units": "Day of year"
                                }
                            },
                            "ebv_scenario": "N\/A",
                            "ebv_geospatial": {
                                "ebv_geospatial_scope": "National",
                                "ebv_geospatial_description": "Finland"
                            },
                            "geospatial_lat_resolution": "0.066428063829064 degrees",
                            "geospatial_lon_resolution": "0.066428063829064 degrees",
                            "geospatial_bounds_crs": "EPSG:4326",
                            "geospatial_lat_min": "57.7532613743371",
                            "geospatial_lon_min": "14.1011663430375",
                            "geospatial_lat_max": "71.171730267808",
                            "geospatial_lon_max": "35.2252906406799",
                            "time_coverage": {
                                "time_coverage_resolution": "P0001-00-00",
                                "time_coverage_start": "2001-01-01",
                                "time_coverage_end": "2019-01-01"
                            },
                            "ebv_domain": "Terrestrial",
                            "comment": "The products were compared with ground observations. The start of the VAP in coniferous forest was well correlated with the day when the Gross Primary Production (GPP) exceeded 15% of its summer maximum at 3 eddy covariance measurement sites in Finland (R2=0.7). The accuracy was 9 days for the period 2001-2016. The satellite product was in average 3 days late compared to the ground observations. The accuracy was higher (6 days, R2=0.84) and no bias was observed in pine forest compared to spruce forest that showed larger deviations to ground observations. The start of the VAP in deciduous vegetation corresponded well with visual observations of the bud break of birch from the phenological network of the Natural Resource Institute of Finland (Luke). The accuracy was 7 days for the period 2001-2015 based on 84 site-years. The bias was negligible (0.4 days).",
                            "dataset": {
                                "pathname": "\/dataset_m.nc",
                                "download": "portal.geobon.org\/data\/upload\/10\/public\/bottcher_ecofun_id10_20220215_v1.nc",
                                "metadata_json": "portal.geobon.org\/data\/upload\/10\/public\/metadata_v1.json",
                                "metadata_xml": "portal.geobon.org\/data\/upload\/10\/public\/metadata_v1.xml"
                            },
                            "file": {
                                "download": "portal.geobon.org\/img\/10\/phenology-maps.jpg"
                            }
                        }
                    ]
                }"#)));

        let provider = Box::new(EbvPortalDataProviderDefinition {
            name: "EBV Portal".to_string(),
            description: "EbvPortalProviderDefinition".to_string(),
            priority: None,
            data: test_data!("netcdf4d").into(),
            base_url: Url::parse(&mock_server.url_str("/api/v1")).unwrap(),
            overviews: test_data!("netcdf4d/overviews").into(),
            cache_ttl: Default::default(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let id = LayerCollectionId("classes/Ecosystem functioning/Ecosystem phenology/10".into());

        let collection = provider
            .load_layer_collection(
                &id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                },
            )
            .await
            .unwrap();

        pretty_assertions::assert_eq!(
            collection, LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: EBV_PROVIDER_ID,
                    collection_id: id,
                },
                name: "Vegetation Phenology in Finland".to_string(),
                description: "Datasets present the yearly maps of the start of vegetation active period (VAP) in coniferous forests and deciduous vegetation during 2001-2019 in Finland. The start of the vegetation active period is defined as the day when coniferous trees start to photosynthesize and for deciduous vegetation as the day when trees unfold new leaves in spring. The datasets were derived from satellite observations of the Moderate Resolution Imaging Spectroradiometer (MODIS).".to_string(),
                items: vec![CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: DataProviderId::from_str(
                            "77d0bf11-986e-43f5-b11d-898321f1854c"
                        )
                        .unwrap(),
                        collection_id: LayerCollectionId(
                            "classes/Ecosystem functioning/Ecosystem phenology/10/metric_1".into()
                        )
                    },
                    name: "Random metric 1".to_string(),
                    description: "Randomly created data".to_string(),
                    properties: Default::default(),
                }),
                CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: DataProviderId::from_str("77d0bf11-986e-43f5-b11d-898321f1854c").unwrap(),
                        collection_id: LayerCollectionId("classes/Ecosystem functioning/Ecosystem phenology/10/metric_2".into())
                    },
                    name: "Random metric 2".to_string(),
                    description: "Randomly created data".to_string(),
                    properties: Default::default(),
                })],
                entry_label: Some("Metric".to_string()),
                properties: vec![("by".to_string(), "Kristin Böttcher (The Finnish Environment Institute (SYKE))".to_string()).into(),
                    ("with license".to_string(), "https://creativecommons.org/licenses/by/4.0".to_string()).into()]
        });
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn test_get_groups(
        _app_ctx: ProPostgresContext<NoTls>,
        ctx: PostgresSessionContext<NoTls>,
    ) {
        // crate::util::tests::initialize_debugging_in_test(); // TODO: remove

        let mock_server = httptest::Server::run();

        mock_server.expect(
            Expectation::matching(request::method_path("GET", "/api/v1/datasets/10")).respond_with(
                status_code(200)
                    .append_header("Content-Type", "application/json")
                    .body(r#"{
                    "code": 200,
                    "message": "List of dataset(s).",
                    "data": [
                        {
                            "id": "10",
                            "naming_authority": "The German Centre for Integrative Biodiversity Research (iDiv) Halle-Jena-Leipzig",
                            "title": "Vegetation Phenology in Finland",
                            "date_created": "2020-11-04",
                            "date_issued": "2022-02-25",
                            "summary": "Datasets present the yearly maps of the start of vegetation active period (VAP) in coniferous forests and deciduous vegetation during 2001-2019 in Finland. The start of the vegetation active period is defined as the day when coniferous trees start to photosynthesize and for deciduous vegetation as the day when trees unfold new leaves in spring. The datasets were derived from satellite observations of the Moderate Resolution Imaging Spectroradiometer (MODIS).",
                            "references": [
                                "10.1016\/j.rse.2013.09.022",
                                "10.3390\/rs8070580"
                            ],
                            "source": "Moderate Resolution Imaging Spectrometer (MODIS) Terra Level 1B (1 km, 500 m) were manually selected from the Level-1 and Atmosphere Archive and Distribution System (LAADS DAAC). From 2009 onwards data were obtained from the satellite receiving station of the Finnish Meteorological Institute (FMI) in Sodankyl\u00e4, Finland and gap-filled with data from LAADS DAAC. MODIS Level 1B data were calibrated to top-of-atmosphere reflectances and projected to a geographic latitude\/longitude grid (datum WGS-84) using the software envimon by Technical Research Centre of Finland (VTT). Fractional Snow Cover (FSC) and the Normalized Difference Water Index (NDWI) were calculated from MODIS top-of-atmosphere reflectances. Cloud covered observations were removed using an automatic cloud masking algorithm by the Finnish Environment Institute.\r\n For the extraction of the start of the VAP in coniferous forest, FSC was averaged at a spatial resolution of 0.05 x 0.05 degrees for the MODIS pixels with dominant coverage of coniferous forest. A sigmoid function was fitted to the averaged FSC-time series and the start of the VAP was determined based on a threshold value. For the extraction of the VAP in deciduous vegetation, daily NDWI time series were averaged for MODIS pixels with vegetation cover into the same spatial grid (0.05 x 0.05 degrees). The day of the VAP was determined from NDWI time series based on a threshold value. The yearly maps of the VAP were smoothed with a median filter to remove spurious outliers and fill spatial gaps. Open water areas were masked.",
                            "coverage_content_type": "modelResult",
                            "project": "MONIMET, SnowCarbo",
                            "project_url": [
                                "https:\/\/monimet.fmi.fi\/index.php?style=warm"
                            ],
                            "creator": {
                                "creator_name": "Kristin B\u00f6ttcher",
                                "creator_email": "Kristin.Bottcher@ymparisto.fi",
                                "creator_institution": "The Finnish Environment Institute (SYKE)",
                                "creator_country": "Finland"
                            },
                            "contributor_name": "N\/A",
                            "license": "https:\/\/creativecommons.org\/licenses\/by\/4.0",
                            "publisher": {
                                "publisher_name": "Kristin B\u00f6ttcher",
                                "publisher_email": "Kristin.Bottcher@ymparisto.fi",
                                "publisher_institution": "Finnish Environment Institute",
                                "publisher_country": "Finland"
                            },
                            "ebv": {
                                "ebv_class": "Ecosystem functioning",
                                "ebv_name": "Ecosystem phenology"
                            },
                            "ebv_entity": {
                                "ebv_entity_type": "Ecosystems",
                                "ebv_entity_scope": "Vegetation",
                                "ebv_entity_classification_name": "N\/A",
                                "ebv_entity_classification_url": "N\/A"
                            },
                            "ebv_metric": {
                                "ebv_metric_1": {
                                    ":standard_name": "Phenology Coniferous",
                                    ":long_name": "The data set consists of yearly maps of the start of the vegetation active period (VAP) in coniferous forest (Day of Year), which is defined as the day when coniferous trees start to photosynthesize in spring. The data set was derived from Moderate Resolution Imaging Spetroradiometer (MODIS) satellite observation of Fractional Snow Cover. The day when snow cover decreases during spring melt was used as a proxy indicator for the beginning of the start of the vegetation active period.",
                                    ":units": "Day of year"
                                },
                                "ebv_metric_2": {
                                    ":standard_name": "Phenology Deciduous",
                                    ":long_name": "The data set consists of yearly maps of the start of the vegetation active period (VAP) in deciduous vegetation (Day of Year), which is defined as the day when deciduous trees unfold new leaves in spring. It is also often referred to as the green-up or greening day. The data set was derived from time series of the Normalized Difference Water Index (NDWI) calculated from Moderate Resolution Imaging Spetroradiometer (MODIS) satellite observation.",
                                    ":units": "Day of year"
                                }
                            },
                            "ebv_scenario": "N\/A",
                            "ebv_geospatial": {
                                "ebv_geospatial_scope": "National",
                                "ebv_geospatial_description": "Finland"
                            },
                            "geospatial_lat_resolution": "0.066428063829064 degrees",
                            "geospatial_lon_resolution": "0.066428063829064 degrees",
                            "geospatial_bounds_crs": "EPSG:4326",
                            "geospatial_lat_min": "57.7532613743371",
                            "geospatial_lon_min": "14.1011663430375",
                            "geospatial_lat_max": "71.171730267808",
                            "geospatial_lon_max": "35.2252906406799",
                            "time_coverage": {
                                "time_coverage_resolution": "P0001-00-00",
                                "time_coverage_start": "2001-01-01",
                                "time_coverage_end": "2019-01-01"
                            },
                            "ebv_domain": "Terrestrial",
                            "comment": "The products were compared with ground observations. The start of the VAP in coniferous forest was well correlated with the day when the Gross Primary Production (GPP) exceeded 15% of its summer maximum at 3 eddy covariance measurement sites in Finland (R2=0.7). The accuracy was 9 days for the period 2001-2016. The satellite product was in average 3 days late compared to the ground observations. The accuracy was higher (6 days, R2=0.84) and no bias was observed in pine forest compared to spruce forest that showed larger deviations to ground observations. The start of the VAP in deciduous vegetation corresponded well with visual observations of the bud break of birch from the phenological network of the Natural Resource Institute of Finland (Luke). The accuracy was 7 days for the period 2001-2015 based on 84 site-years. The bias was negligible (0.4 days).",
                            "dataset": {
                                "pathname": "\/dataset_m.nc",
                                "download": "portal.geobon.org\/data\/upload\/10\/public\/bottcher_ecofun_id10_20220215_v1.nc",
                                "metadata_json": "portal.geobon.org\/data\/upload\/10\/public\/metadata_v1.json",
                                "metadata_xml": "portal.geobon.org\/data\/upload\/10\/public\/metadata_v1.xml"
                            },
                            "file": {
                                "download": "portal.geobon.org\/img\/10\/phenology-maps.jpg"
                            }
                        }
                    ]
                }"#)));

        let provider = Box::new(EbvPortalDataProviderDefinition {
            name: "EBV Portal".to_string(),
            description: "EbvPortalProviderDefinition".to_string(),
            priority: None,
            data: test_data!("netcdf4d").into(),
            base_url: Url::parse(&mock_server.url_str("/api/v1")).unwrap(),
            overviews: test_data!("netcdf4d/overviews").into(),
            cache_ttl: Default::default(),
        })
        .initialize(ctx.db())
        .await
        .unwrap();

        let id = LayerCollectionId(
            "classes/Ecosystem functioning/Ecosystem phenology/10/metric_1".into(),
        );

        let collection = provider
            .load_layer_collection(
                &id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                },
            )
            .await
            .unwrap();

        pretty_assertions::assert_eq!(
            collection,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: EBV_PROVIDER_ID,
                    collection_id: id,
                },
                name: "Random metric 1".to_string(),
                description: "Randomly created data".to_string(),
                items: vec![CollectionItem::Layer(LayerListing {
                        id: ProviderLayerId {
                            provider_id: DataProviderId::from_str("77d0bf11-986e-43f5-b11d-898321f1854c").unwrap(),
                            layer_id: LayerId("classes/Ecosystem functioning/Ecosystem phenology/10/metric_1/0.entity".into())
                        },
                        name: "entity01".to_string(),
                        description: String::new(),
                        properties: vec![],
                    }), CollectionItem::Layer(LayerListing {
                        id: ProviderLayerId {
                            provider_id: DataProviderId::from_str("77d0bf11-986e-43f5-b11d-898321f1854c").unwrap(),
                            layer_id: LayerId("classes/Ecosystem functioning/Ecosystem phenology/10/metric_1/1.entity".into())
                        },
                        name: "entity02".to_string(),
                        description: String::new(),
                        properties: vec![],
                    }), CollectionItem::Layer(LayerListing {
                        id: ProviderLayerId {
                            provider_id: DataProviderId::from_str("77d0bf11-986e-43f5-b11d-898321f1854c").unwrap(),
                            layer_id: LayerId("classes/Ecosystem functioning/Ecosystem phenology/10/metric_1/2.entity".into())
                        },
                        name: "entity03".to_string(),
                        description: String::new(),
                        properties: vec![],
                    })],
                entry_label: Some("Entity".to_string()),
                properties: vec![],
            }
        );
    }
}
