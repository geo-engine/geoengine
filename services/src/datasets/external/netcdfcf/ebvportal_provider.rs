use std::path::PathBuf;

use async_trait::async_trait;
use geoengine_datatypes::{
    dataset::{DataId, DataProviderId, ExternalDataId, LayerId},
    primitives::{RasterQueryRectangle, VectorQueryRectangle},
};
use geoengine_operators::{
    engine::{
        MetaData, MetaDataProvider, RasterOperator, RasterResultDescriptor, TypedOperator,
        VectorResultDescriptor,
    },
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, GdalSource, GdalSourceParameters, OgrSourceDataset},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use snafu::ensure;

use crate::{
    datasets::external::netcdfcf::list_groups,
    error::{Error, Result},
    layers::{
        external::{DataProvider, DataProviderDefinition},
        layer::{
            CollectionItem, Layer, LayerCollectionListOptions, LayerCollectionListing,
            LayerListing, ProviderLayerCollectionId, ProviderLayerId,
        },
        listing::{LayerCollectionId, LayerCollectionProvider},
    },
    util::user_input::Validated,
    workflows::workflow::Workflow,
};

use super::{
    ebvportal_api::{
        get_classes, get_ebv_datasets, get_ebv_subdatasets, NetCdfCfDataProviderPaths,
    },
    NetCdfCfDataProvider,
};

/// Singleton Provider with id `77d0bf11-986e-43f5-b11d-898321f1854c`
pub const EBV_PROVIDER_ID: DataProviderId =
    DataProviderId::from_u128(0x77d0_bf11_986e_43f5_b11d_8983_21f1_854c);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EbvPortalDataProviderDefinition {
    pub name: String,
    pub path: PathBuf,
    pub overviews: PathBuf,
}

#[derive(Debug)]
pub struct EbvPortalDataProvider {
    pub netcdf_cf_provider: NetCdfCfDataProvider,
}

#[typetag::serde]
#[async_trait]
impl DataProviderDefinition for EbvPortalDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> crate::error::Result<Box<dyn DataProvider>> {
        Ok(Box::new(EbvPortalDataProvider {
            netcdf_cf_provider: NetCdfCfDataProvider {
                path: self.path,
                overviews: self.overviews,
            },
        }))
    }

    fn type_name(&self) -> String {
        "EbvPortalProviderDefinition".to_owned()
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DataProviderId {
        EBV_PROVIDER_ID
    }
}

#[async_trait]
impl DataProvider for EbvPortalDataProvider {
    async fn provenance(
        &self,
        id: &geoengine_datatypes::dataset::DataId,
    ) -> crate::error::Result<crate::datasets::listing::ProvenanceOutput> {
        self.netcdf_cf_provider.provenance(id).await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Serialize, Deserialize)]
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

impl TryFrom<EbvCollectionId> for LayerCollectionId {
    type Error = crate::error::Error;

    fn try_from(id: EbvCollectionId) -> Result<Self> {
        Ok(LayerCollectionId(serde_json::to_string(&id)?))
    }
}

impl TryFrom<EbvCollectionId> for LayerId {
    type Error = crate::error::Error;

    fn try_from(id: EbvCollectionId) -> Result<Self> {
        Ok(LayerId(serde_json::to_string(&id)?))
    }
}

impl EbvPortalDataProvider {
    async fn get_classes_collections(
        options: &LayerCollectionListOptions,
    ) -> Result<Vec<CollectionItem>> {
        get_classes()
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
                    description: "".to_string(),
                }))
            })
            .collect()
    }

    async fn get_class_collections(
        collection: &LayerCollectionId,
        class: &str,
        options: &LayerCollectionListOptions,
    ) -> Result<Vec<CollectionItem>> {
        get_classes()
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
                    description: "".to_string(),
                }))
            })
            .collect()
    }

    async fn get_ebv_collections(
        class: &str,
        ebv: &str,
        options: &LayerCollectionListOptions,
    ) -> Result<Vec<CollectionItem>> {
        get_ebv_datasets(ebv)
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
                }))
            })
            .collect()
    }

    async fn get_dataset_collections(
        &self,
        class: &str,
        ebv: &str,
        dataset: &str,
        options: &LayerCollectionListOptions,
    ) -> Result<Vec<CollectionItem>> {
        get_ebv_subdatasets(
            NetCdfCfDataProviderPaths {
                provider_path: self.netcdf_cf_provider.path.clone(),
                overview_path: self.netcdf_cf_provider.overviews.clone(),
            },
            dataset,
        )
        .await?
        .tree
        .groups
        .into_iter()
        .skip(options.offset as usize)
        .take(options.limit as usize)
        .map(|g| {
            Ok(CollectionItem::Collection(LayerCollectionListing {
                id: ProviderLayerCollectionId {
                    provider_id: EBV_PROVIDER_ID,
                    collection_id: EbvCollectionId::Group {
                        class: class.to_string(),
                        ebv: ebv.to_string(),
                        dataset: dataset.to_string(),
                        groups: vec![g.name.clone()],
                    }
                    .try_into()?,
                },
                name: g.title,
                description: g.description,
            }))
        })
        .collect()
    }

    async fn get_group_collections(
        &self,
        class: &str,
        ebv: &str,
        dataset: &str,
        groups: &[String],
        options: &LayerCollectionListOptions,
    ) -> Result<Vec<CollectionItem>> {
        ensure!(!groups.is_empty(), crate::error::InvalidLayerCollectionId);

        let tree = get_ebv_subdatasets(
            NetCdfCfDataProviderPaths {
                provider_path: self.netcdf_cf_provider.path.clone(),
                overview_path: self.netcdf_cf_provider.overviews.clone(),
            },
            dataset,
        )
        .await?
        .tree;

        let groups_list = list_groups(tree.groups, groups)?;

        if groups_list.is_empty() {
            tree.entities
                .into_iter()
                .skip(options.offset as usize)
                .take(options.limit as usize)
                .map(|entity| {
                    Ok(CollectionItem::Layer(LayerListing {
                        id: ProviderLayerId {
                            provider_id: EBV_PROVIDER_ID,
                            layer_id: EbvCollectionId::Entity {
                                class: class.to_string(),
                                ebv: ebv.to_string(),
                                dataset: dataset.to_string(),
                                groups: groups.to_owned(),
                                entity: entity.id,
                            }
                            .try_into()?,
                        },
                        name: entity.name,
                        description: "".to_string(),
                    }))
                })
                .collect()
        } else {
            let out_groups = groups.to_owned();

            groups_list
                .into_iter()
                .skip(options.offset as usize)
                .take(options.limit as usize)
                .map(|group| {
                    let mut out_groups = out_groups.clone();
                    out_groups.push(group.name.clone());
                    Ok(CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: EBV_PROVIDER_ID,
                            collection_id: EbvCollectionId::Group {
                                class: class.to_string(),
                                ebv: ebv.to_string(),
                                dataset: dataset.to_string(),
                                groups: out_groups,
                            }
                            .try_into()?,
                        },
                        name: group.title,
                        description: group.description,
                    }))
                })
                .collect()
        }
    }
}

#[async_trait]
impl LayerCollectionProvider for EbvPortalDataProvider {
    async fn collection_items(
        &self,
        collection: &LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>> {
        let id: EbvCollectionId = serde_json::from_str(&collection.0)?;

        let options = options.user_input;

        Ok(match id {
            EbvCollectionId::Classes => Self::get_classes_collections(&options).await?,
            EbvCollectionId::Class { class } => {
                Self::get_class_collections(collection, &class, &options).await?
            }

            EbvCollectionId::Ebv { class, ebv } => {
                Self::get_ebv_collections(&class, &ebv, &options).await?
            }
            EbvCollectionId::Dataset {
                class,
                ebv,
                dataset,
            } => {
                self.get_dataset_collections(&class, &ebv, &dataset, &options)
                    .await?
            }
            EbvCollectionId::Group {
                class,
                ebv,
                dataset,
                groups,
            } => {
                self.get_group_collections(&class, &ebv, &dataset, &groups, &options)
                    .await?
            }
            EbvCollectionId::Entity {
                class: _,
                ebv: _,
                dataset: _,
                groups: _,
                entity: _,
            } => return Err(Error::InvalidLayerCollectionId),
        })
    }

    async fn root_collection_id(&self) -> Result<LayerCollectionId> {
        EbvCollectionId::Classes.try_into()
    }

    async fn get_layer(&self, id: &LayerId) -> Result<Layer> {
        let ebv_id: EbvCollectionId = serde_json::from_str(&id.0)?;

        match &ebv_id {
            EbvCollectionId::Entity {
                class,
                ebv,
                dataset,
                groups,
                entity,
            } => {
                let ebv_hierarchy = get_ebv_subdatasets(
                    NetCdfCfDataProviderPaths {
                        provider_path: self.netcdf_cf_provider.path.clone(),
                        overview_path: self.netcdf_cf_provider.overviews.clone(),
                    },
                    dataset,
                )
                .await?;

                let netcdf_entity = ebv_hierarchy
                    .tree
                    .entities
                    .into_iter()
                    .find(|e| e.id == *entity)
                    .ok_or(Error::UnknownLayerId { id: id.clone() })?;

                Ok(Layer {
                    id: ProviderLayerId {
                        provider_id: EBV_PROVIDER_ID,
                        layer_id: id.clone(),
                    },
                    name: format!("{} > {} > {} > {}", class, ebv, ebv_hierarchy.tree.title, /*netcdf_metric.title, */netcdf_entity.name),
                    description: format!("{} > {} > {} > {}", class, ebv, ebv_hierarchy.tree.title, /* netcdf_metric.title,*/ netcdf_entity.name),
                    workflow: Workflow {
                        operator: TypedOperator::Raster(
                            GdalSource {
                                params: GdalSourceParameters {
                                    data: DataId::External(ExternalDataId {
                                        provider_id: EBV_PROVIDER_ID,
                                        layer_id: LayerId(
                                            json!({
                                                "fileName": ebv_hierarchy.tree.file_name,
                                                "groupNames": groups,
                                                "entity": entity
                                            })
                                            .to_string(),
                                        ),
                                    }),
                                },
                            }
                            .boxed(),
                        ),
                    },
                    symbology: None,
                })
            }
            _ => return Err(Error::InvalidLayerId),
        }
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for EbvPortalDataProvider
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        self.netcdf_cf_provider.meta_data(id).await
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for EbvPortalDataProvider
{
    async fn meta_data(
        &self,
        _id: &DataId,
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
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for EbvPortalDataProvider
{
    async fn meta_data(
        &self,
        _id: &DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}
