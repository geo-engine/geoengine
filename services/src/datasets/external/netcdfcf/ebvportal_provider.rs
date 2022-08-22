use std::path::PathBuf;

use async_trait::async_trait;
use geoengine_datatypes::{
    dataset::{DataId, DataProviderId, LayerId},
    primitives::{RasterQueryRectangle, VectorQueryRectangle},
};
use geoengine_operators::{
    engine::{MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::{
    datasets::external::netcdfcf::find_group,
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
};

use super::{
    ebvportal_api::{EbvPortalApi, NetCdfCfDataProviderPaths},
    layer_from_netcdf_overview, NetCdfCfDataProvider,
};

/// Singleton Provider with id `77d0bf11-986e-43f5-b11d-898321f1854c`
pub const EBV_PROVIDER_ID: DataProviderId =
    DataProviderId::from_u128(0x77d0_bf11_986e_43f5_b11d_8983_21f1_854c);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EbvPortalDataProviderDefinition {
    pub name: String,
    pub path: PathBuf,
    pub base_url: Url,
    pub overviews: PathBuf,
}

#[derive(Debug)]
pub struct EbvPortalDataProvider {
    pub ebv_api: EbvPortalApi,
    pub netcdf_cf_provider: NetCdfCfDataProvider,
}

#[typetag::serde]
#[async_trait]
impl DataProviderDefinition for EbvPortalDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> crate::error::Result<Box<dyn DataProvider>> {
        Ok(Box::new(EbvPortalDataProvider {
            ebv_api: EbvPortalApi::new(self.base_url),
            netcdf_cf_provider: NetCdfCfDataProvider {
                path: self.path,
                overviews: self.overviews,
            },
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
}

#[async_trait]
impl DataProvider for EbvPortalDataProvider {
    async fn provenance(
        &self,
        id: &geoengine_datatypes::dataset::DataId,
    ) -> crate::error::Result<crate::datasets::listing::ProvenanceOutput> {
        self.netcdf_cf_provider.provenance(id).await
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
        &self,
        options: &LayerCollectionListOptions,
    ) -> Result<Vec<CollectionItem>> {
        self.ebv_api
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
                    description: "".to_string(),
                    entry_label: Some("EBV Name".to_string()),
                    properties: vec![],
                }))
            })
            .collect()
    }

    async fn get_class_collections(
        &self,
        collection: &LayerCollectionId,
        class: &str,
        options: &LayerCollectionListOptions,
    ) -> Result<Vec<CollectionItem>> {
        self.ebv_api
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
                    description: "".to_string(),
                    entry_label: Some("EBV Dataset".to_string()),
                    properties: vec![],
                }))
            })
            .collect()
    }

    async fn get_ebv_collections(
        &self,
        class: &str,
        ebv: &str,
        options: &LayerCollectionListOptions,
    ) -> Result<Vec<CollectionItem>> {
        self.ebv_api
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
                    entry_label: d
                        .has_scenario
                        .then_some("Scenario".to_string())
                        .or_else(|| Some("Metric".to_string())),
                    properties: [
                        (
                            "by".to_string(),
                            format!("{} ({})", d.author_name, d.author_institution),
                        ),
                        ("with license".to_string(), d.license),
                    ]
                    .into_iter()
                    .collect(),
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
        self.ebv_api
            .get_ebv_subdatasets(
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
                    entry_label: g
                        .groups
                        .is_empty()
                        .then_some("Entity".to_string())
                        .or_else(|| Some("Metric".to_string())),
                    properties: vec![],
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

        let tree = self
            .ebv_api
            .get_ebv_subdatasets(
                NetCdfCfDataProviderPaths {
                    provider_path: self.netcdf_cf_provider.path.clone(),
                    overview_path: self.netcdf_cf_provider.overviews.clone(),
                },
                dataset,
            )
            .await?
            .tree;

        let groups_list =
            find_group(tree.groups.clone(), groups)?.map_or(tree.groups, |g| g.groups);

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
                        properties: vec![],
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
                        entry_label: None,
                        properties: vec![],
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
            EbvCollectionId::Classes => self.get_classes_collections(&options).await?,
            EbvCollectionId::Class { class } => {
                self.get_class_collections(collection, &class, &options)
                    .await?
            }

            EbvCollectionId::Ebv { class, ebv } => {
                self.get_ebv_collections(&class, &ebv, &options).await?
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
            EbvCollectionId::Entity { .. } => return Err(Error::InvalidLayerCollectionId),
        })
    }

    async fn root_collection_id(&self) -> Result<LayerCollectionId> {
        EbvCollectionId::Classes.try_into()
    }

    async fn get_layer(&self, id: &LayerId) -> Result<Layer> {
        let ebv_id: EbvCollectionId = serde_json::from_str(&id.0)?;

        match &ebv_id {
            EbvCollectionId::Entity {
                class: _,
                ebv: _,
                dataset,
                groups,
                entity,
            } => {
                let ebv_hierarchy = self
                    .ebv_api
                    .get_ebv_subdatasets(
                        NetCdfCfDataProviderPaths {
                            provider_path: self.netcdf_cf_provider.path.clone(),
                            overview_path: self.netcdf_cf_provider.overviews.clone(),
                        },
                        dataset,
                    )
                    .await?;

                layer_from_netcdf_overview(EBV_PROVIDER_ID, id, ebv_hierarchy.tree, groups, *entity)
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use geoengine_datatypes::test_data;
    use httptest::{matchers::request, responders::status_code, Expectation};

    use crate::util::user_input::UserInput;

    use super::*;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_get_classes() {
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
            path: test_data!("netcdf4d").into(),
            base_url: Url::parse(&mock_server.url_str("/api/v1")).unwrap(),
            overviews: test_data!("netcdf4d/overviews").into(),
        })
        .initialize()
        .await
        .unwrap();

        let items = provider
            .collection_items(
                &provider.root_collection_id().await.unwrap(),
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            items,
            vec![
                CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: DataProviderId::from_str(
                            "77d0bf11-986e-43f5-b11d-898321f1854c"
                        )
                        .unwrap(),
                        collection_id: LayerCollectionId(
                            "{\"Class\":{\"class\":\"Community composition\"}}".into()
                        )
                    },
                    name: "Community composition".to_string(),
                    description: "".to_string(),
                    entry_label: Some("EBV Name".to_string()),
                    properties: vec![]
                }),
                CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: DataProviderId::from_str(
                            "77d0bf11-986e-43f5-b11d-898321f1854c"
                        )
                        .unwrap(),
                        collection_id: LayerCollectionId(
                            "{\"Class\":{\"class\":\"Ecosystem functioning\"}}".into()
                        )
                    },
                    name: "Ecosystem functioning".to_string(),
                    description: "".to_string(),
                    entry_label: Some("EBV Name".to_string()),
                    properties: vec![]
                }),
                CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: DataProviderId::from_str(
                            "77d0bf11-986e-43f5-b11d-898321f1854c"
                        )
                        .unwrap(),
                        collection_id: LayerCollectionId(
                            "{\"Class\":{\"class\":\"Ecosystem structure\"}}".into()
                        )
                    },
                    name: "Ecosystem structure".to_string(),
                    description: "".to_string(),
                    entry_label: Some("EBV Name".to_string()),
                    properties: vec![]
                }),
                CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: DataProviderId::from_str(
                            "77d0bf11-986e-43f5-b11d-898321f1854c"
                        )
                        .unwrap(),
                        collection_id: LayerCollectionId(
                            "{\"Class\":{\"class\":\"Species populations\"}}".into()
                        )
                    },
                    name: "Species populations".to_string(),
                    description: "".to_string(),
                    entry_label: Some("EBV Name".to_string()),
                    properties: vec![]
                })
            ]
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_get_class() {
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
            path: test_data!("netcdf4d").into(),
            base_url: Url::parse(&mock_server.url_str("/api/v1")).unwrap(),
            overviews: test_data!("netcdf4d/overviews").into(),
        })
        .initialize()
        .await
        .unwrap();

        let items = provider
            .collection_items(
                &LayerCollectionId("{\"Class\":{\"class\":\"Ecosystem functioning\"}}".into()),
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(items, vec![
            CollectionItem::Collection(LayerCollectionListing {
                id: ProviderLayerCollectionId {
                    provider_id: DataProviderId::from_str("77d0bf11-986e-43f5-b11d-898321f1854c").unwrap(), 
                    collection_id: LayerCollectionId("{\"Ebv\":{\"class\":\"Ecosystem functioning\",\"ebv\":\"Ecosystem phenology\"}}".into()) 
                },
                name: "Ecosystem phenology".to_string(), 
                description: "".to_string(), 
                entry_label: Some("EBV Dataset".to_string()), 
                properties: vec![],
            }),
            CollectionItem::Collection(LayerCollectionListing {
                 id: ProviderLayerCollectionId {
                    provider_id: DataProviderId::from_str("77d0bf11-986e-43f5-b11d-898321f1854c").unwrap(), 
                    collection_id: LayerCollectionId("{\"Ebv\":{\"class\":\"Ecosystem functioning\",\"ebv\":\"Primary productivity\"}}".into()) 
                },
                name: "Primary productivity".to_string(), 
                description: "".to_string(), 
                entry_label: Some("EBV Dataset".to_string()), 
                properties: vec![],
            })]);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_get_ebv() {
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
            path: test_data!("netcdf4d").into(),
            base_url: Url::parse(&mock_server.url_str("/api/v1")).unwrap(),
            overviews: test_data!("netcdf4d/overviews").into(),
        })
        .initialize()
        .await
        .unwrap();

        let items = provider
            .collection_items(
                &LayerCollectionId("{\"Ebv\":{\"class\":\"Ecosystem functioning\",\"ebv\":\"Ecosystem phenology\"}}".into()),
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(items, vec![
            CollectionItem::Collection(LayerCollectionListing {
                id: ProviderLayerCollectionId {
                    provider_id: DataProviderId::from_str("77d0bf11-986e-43f5-b11d-898321f1854c").unwrap(),
                    collection_id: LayerCollectionId("{\"Dataset\":{\"class\":\"Ecosystem functioning\",\"ebv\":\"Ecosystem phenology\",\"dataset\":\"10\"}}".into()) 
                },
                name: "Vegetation Phenology in Finland".to_string(),
                description: "Datasets present the yearly maps of the start of vegetation active period (VAP) in coniferous forests and deciduous vegetation during 2001-2019 in Finland. The start of the vegetation active period is defined as the day when coniferous trees start to photosynthesize and for deciduous vegetation as the day when trees unfold new leaves in spring. The datasets were derived from satellite observations of the Moderate Resolution Imaging Spectroradiometer (MODIS).".to_string(), 
                entry_label: Some("Metric".to_string()),
                properties: vec![("by".to_string(), "Kristin Böttcher (The Finnish Environment Institute (SYKE))".to_string()),
                    ("with license".to_string(), "https://creativecommons.org/licenses/by/4.0".to_string())]              
            })]
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_get_dataset() {
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
            path: test_data!("netcdf4d").into(),
            base_url: Url::parse(&mock_server.url_str("/api/v1")).unwrap(),
            overviews: test_data!("netcdf4d/overviews").into(),
        })
        .initialize()
        .await
        .unwrap();

        let items = provider
            .collection_items(
                &LayerCollectionId("{\"Dataset\":{\"class\":\"Ecosystem functioning\",\"ebv\":\"Ecosystem phenology\",\"dataset\":\"10\"}}".into()),
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            items,
            vec![CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: DataProviderId::from_str("77d0bf11-986e-43f5-b11d-898321f1854c").unwrap(),
                        collection_id: LayerCollectionId("{\"Group\":{\"class\":\"Ecosystem functioning\",\"ebv\":\"Ecosystem phenology\",\"dataset\":\"10\",\"groups\":[\"metric_1\"]}}".into())
                    },
                    name: "Random metric 1".to_string(),
                    description: "Randomly created data".to_string(),
                    entry_label: Some("Entity".to_string()),
                    properties: vec![] }),
                CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: DataProviderId::from_str("77d0bf11-986e-43f5-b11d-898321f1854c").unwrap(),
                        collection_id: LayerCollectionId("{\"Group\":{\"class\":\"Ecosystem functioning\",\"ebv\":\"Ecosystem phenology\",\"dataset\":\"10\",\"groups\":[\"metric_2\"]}}".into()) 
                    },
                    name: "Random metric 2".to_string(),
                    description: "Randomly created data".to_string(),
                    entry_label: Some("Entity".to_string()),
                    properties: vec![]
                })
            ]
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_get_groups() {
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
            path: test_data!("netcdf4d").into(),
            base_url: Url::parse(&mock_server.url_str("/api/v1")).unwrap(),
            overviews: test_data!("netcdf4d/overviews").into(),
        })
        .initialize()
        .await
        .unwrap();

        let items = provider
            .collection_items(
                &LayerCollectionId("{\"Group\":{\"class\":\"Ecosystem functioning\",\"ebv\":\"Ecosystem phenology\",\"dataset\":\"10\",\"groups\":[\"metric_1\"]}}".into()),
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            items,
            vec![CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: DataProviderId::from_str("77d0bf11-986e-43f5-b11d-898321f1854c").unwrap(),
                        layer_id: LayerId("{\"Entity\":{\"class\":\"Ecosystem functioning\",\"ebv\":\"Ecosystem phenology\",\"dataset\":\"10\",\"groups\":[\"metric_1\"],\"entity\":0}}".into()) 
                    },
                    name: "entity01".to_string(),
                    description: "".to_string(),
                    properties: vec![]
                }), CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: DataProviderId::from_str("77d0bf11-986e-43f5-b11d-898321f1854c").unwrap(),
                        layer_id: LayerId("{\"Entity\":{\"class\":\"Ecosystem functioning\",\"ebv\":\"Ecosystem phenology\",\"dataset\":\"10\",\"groups\":[\"metric_1\"],\"entity\":1}}".into()) 
                    },
                    name: "entity02".to_string(),
                    description: "".to_string(),
                    properties: vec![]
                }), CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: DataProviderId::from_str("77d0bf11-986e-43f5-b11d-898321f1854c").unwrap(),
                        layer_id: LayerId("{\"Entity\":{\"class\":\"Ecosystem functioning\",\"ebv\":\"Ecosystem phenology\",\"dataset\":\"10\",\"groups\":[\"metric_1\"],\"entity\":2}}".into()) 
                    },
                    name: "entity03".to_string(),
                    description: "".to_string(),
                    properties: vec![]
                })]
        );
    }
}
