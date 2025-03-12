use std::{collections::HashMap, str::FromStr};

use crate::{
    api::model::datatypes::GdalConfigOption,
    datasets::listing::ProvenanceOutput,
    error::{Error, Result},
    layers::{
        layer::{
            CollectionItem, Layer, LayerCollection, LayerCollectionListOptions,
            LayerCollectionListing, LayerListing, ProviderLayerCollectionId, ProviderLayerId,
        },
        listing::LayerCollectionId,
    },
    projects::RasterSymbology,
    workflows::workflow::Workflow,
};
use async_trait::async_trait;
use geoengine_datatypes::{
    dataset::{DataId, DataProviderId, LayerId},
    operations::image::{Colorizer, RasterColorizer, RgbaColor},
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
use ordered_float::NotNan;
use serde::{Deserialize, Serialize};
use strum::{EnumIter, IntoEnumIterator};

use crate::{
    contexts::GeoEngineDb,
    layers::{
        external::{DataProvider, DataProviderDefinition},
        listing::{LayerCollectionProvider, ProviderCapabilities, SearchCapabilities},
    },
};

use super::{
    ids::{
        CopernicusDataId, CopernicusDataspaceLayerCollectionId, CopernicusDataspaceLayerId,
        Sentinel2LayerCollectionId, Sentinel2LayerId, Sentinel2Product, Sentinel2ProductBand,
        UtmZone,
    },
    sentinel2::Sentinel2Metadata,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CopernicusDataspaceDataProviderDefinition {
    pub name: String,
    pub description: String,
    pub id: DataProviderId,
    pub stac_url: String,
    pub s3_url: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub gdal_config: Vec<GdalConfigOption>,
    pub priority: Option<i16>,
}

#[async_trait]
impl<D: GeoEngineDb> DataProviderDefinition<D> for CopernicusDataspaceDataProviderDefinition {
    async fn initialize(self: Box<Self>, _db: D) -> crate::error::Result<Box<dyn DataProvider>> {
        Ok(Box::new(CopernicusDataspaceDataProvider::new(
            self.id,
            self.name,
            self.description,
            self.stac_url,
            self.s3_url,
            self.s3_access_key,
            self.s3_secret_key,
            self.gdal_config
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
        )))
    }

    fn type_name(&self) -> &'static str {
        "CopernicusDataspace"
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

#[derive(Clone, Debug)]
pub struct CopernicusDataspaceDataProvider {
    pub id: DataProviderId,
    pub name: String,
    pub description: String,
    pub stac_url: String,
    pub s3_url: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub gdal_config: Vec<(String, String)>,
}

impl CopernicusDataspaceDataProvider {
    #[allow(clippy::too_many_arguments)]
    fn new(
        id: DataProviderId,
        name: String,
        description: String,
        stac_url: String,
        s3_url: String,
        s3_access_key: String,
        s3_secret_key: String,
        gdal_config: Vec<(String, String)>,
    ) -> Self {
        Self {
            id,
            name,
            description,
            stac_url,
            s3_url,
            s3_access_key,
            s3_secret_key,
            gdal_config,
        }
    }

    fn root_collection(&self) -> LayerCollection {
        LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: self.id,
                collection_id: CopernicusDataspaceLayerCollectionId::Datasets.into(),
            },
            name: "Datasets".to_string(),
            description: "Datasets from the Copernicus Dataspace".to_string(),
            items: Datasets::iter()
                .map(|item| {
                    CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                        id: ProviderLayerCollectionId {
                            provider_id: self.id,
                            collection_id: LayerCollectionId(format!("datasets/{item}")),
                        },
                        name: format!("{item}"),
                        description: format!("{item}"),
                        properties: vec![],
                    })
                })
                .collect(),
            entry_label: None,
            properties: vec![],
        }
    }

    // TODO: move to sentinel2.rs
    fn load_sentinel2_collection(&self, id: &Sentinel2LayerCollectionId) -> LayerCollection {
        match *id {
            Sentinel2LayerCollectionId::Products => self.load_sentinel2_products_collection(),
            Sentinel2LayerCollectionId::Product { product } => {
                self.load_sentinel2_product_collection(product)
            }
            Sentinel2LayerCollectionId::ProductZone { product, zone } => {
                self.load_sentinel2_product_zone_collection(product, zone)
            }
        }
    }

    fn load_sentinel2_products_collection(&self) -> LayerCollection {
        LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: self.id,
                collection_id: CopernicusDataspaceLayerCollectionId::Datasets.into(),
            },
            name: "Sentinel-2 Products".to_string(),
            description: "Sentinel-2 Products".to_string(),
            items: Sentinel2Product::iter()
                .map(|product| {
                    CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                        id: ProviderLayerCollectionId {
                            provider_id: self.id,
                            collection_id: CopernicusDataspaceLayerCollectionId::Sentinel2(
                                Sentinel2LayerCollectionId::Product { product },
                            )
                            .into(),
                        },
                        name: format!("{product}"),
                        description: format!("{product}"),
                        properties: vec![],
                    })
                })
                .collect(),
            entry_label: None,
            properties: vec![],
        }
    }

    fn load_sentinel2_product_collection(&self, product: Sentinel2Product) -> LayerCollection {
        LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: self.id,
                collection_id: CopernicusDataspaceLayerCollectionId::Datasets.into(),
            },
            name: "Zones".to_string(),
            description: "Zones".to_string(),
            items: UtmZone::zones()
                .map(|zone| {
                    CollectionItem::Collection(LayerCollectionListing { r#type: Default::default(),
                        id: ProviderLayerCollectionId {
                            provider_id: self.id,
                            collection_id: CopernicusDataspaceLayerCollectionId::Sentinel2(
                                Sentinel2LayerCollectionId::ProductZone { product, zone },
                            )
                            .into(),
                        },
                        name: format!("{zone}"),
                        description: format!("{zone}"),
                        properties: vec![],
                    })
                })
                .collect(),
            entry_label: None,
            properties: vec![],
        }
    }

    fn load_sentinel2_product_zone_collection(
        &self,
        product: Sentinel2Product,
        zone: UtmZone,
    ) -> LayerCollection {
        LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: self.id,
                collection_id: CopernicusDataspaceLayerCollectionId::Datasets.into(),
            },
            name: "Bands".to_string(),
            description: "Bands".to_string(),
            items: product
                .product_bands()
                .map(|product_band| {
                    CollectionItem::Layer(LayerListing { r#type: Default::default(),
                        id: ProviderLayerId {
                            provider_id: self.id,
                            layer_id: CopernicusDataspaceLayerId::Sentinel2(Sentinel2LayerId {
                                product_band,
                                zone,
                            })
                            .into(),
                        },
                        name: product_band.band_name(),
                        description: product_band.band_name(),
                        properties: vec![],
                    })
                })
                .collect(),
            entry_label: None,
            properties: vec![],
        }
    }

    fn load_sentinel2_layer(&self, id: &Sentinel2LayerId) -> Layer {
        Layer {
            id: ProviderLayerId {
                provider_id: self.id,
                layer_id: id.clone().into(),
            },
            name: format!(
                "Sentinel-2 {} {} {}",
                id.product_band.product_name(),
                id.zone,
                id.product_band.band_name()
            ),
            description: String::new(),
            workflow: Workflow {
                operator: TypedOperator::Raster(
                    GdalSource {
                        params: GdalSourceParameters {
                            data: CopernicusDataId(
                                CopernicusDataspaceLayerId::Sentinel2(id.clone()),
                                self.id,
                            )
                            .into(),
                        },
                    }
                    .boxed(),
                ),
            },
            symbology: Some(crate::projects::Symbology::Raster(RasterSymbology {
                opacity: 1.0,
                raster_colorizer: RasterColorizer::SingleBand {
                    band: 0,
                    band_colorizer: Colorizer::linear_gradient(
                        vec![
                            (NotNan::<f64>::from(0), RgbaColor::black()).into(),
                            (NotNan::<f64>::from(u16::MAX), RgbaColor::white()).into(),
                        ],
                        RgbaColor::transparent(),
                        RgbaColor::black(),
                        RgbaColor::white(),
                    )
                    .expect(
                        "colorizer should be valid because it was created from valid breakpoints",
                    ),
                },
            })),
            properties: vec![],
            metadata: HashMap::new(),
        }
    }

    fn sentinel2_meta_data(
        &self,
        product_band: Sentinel2ProductBand,
        zone: UtmZone,
    ) -> Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>> {
        Box::new(Sentinel2Metadata {
            stac_url: self.stac_url.clone(),
            s3_url: self.s3_url.clone(),
            s3_use_https: true,
            s3_access_key: self.s3_access_key.clone(),
            s3_secret_key: self.s3_secret_key.clone(),
            product_band,
            zone,
            gdal_config: self.gdal_config.clone(),
        })
    }
}

#[derive(Debug, Clone, strum::Display, EnumIter)]
pub enum Datasets {
    #[strum(to_string = "SENTINEL-1")]
    Sentinel1,
    #[strum(to_string = "SENTINEL-2")]
    Sentinel2,
    // Landsat, ...
}

#[async_trait]
impl DataProvider for CopernicusDataspaceDataProvider {
    async fn provenance(&self, id: &DataId) -> Result<ProvenanceOutput> {
        Ok(ProvenanceOutput {
            data: id.clone(),
            provenance: None, // TODO
        })
    }
}

#[async_trait]
impl LayerCollectionProvider for CopernicusDataspaceDataProvider {
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
        _options: LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
        // TODO: use options

        let id = CopernicusDataspaceLayerCollectionId::from_str(&collection.0)?;

        match id {
            CopernicusDataspaceLayerCollectionId::Datasets => Ok(self.root_collection()),
            CopernicusDataspaceLayerCollectionId::Sentinel1 => Err(Error::NotImplemented {
                message: "Sentinel-1 not implemented yet".to_string(),
            }),
            CopernicusDataspaceLayerCollectionId::Sentinel2(sentinel2) => {
                Ok(self.load_sentinel2_collection(&sentinel2))
            }
        }
    }

    async fn get_root_layer_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(CopernicusDataspaceLayerCollectionId::Datasets.into())
    }

    async fn load_layer(&self, id: &LayerId) -> Result<Layer> {
        let id: CopernicusDataspaceLayerId = id.clone().try_into()?;

        match id {
            CopernicusDataspaceLayerId::Sentinel2(sentinel2) => {
                Ok(self.load_sentinel2_layer(&sentinel2))
            }
        }
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for CopernicusDataspaceDataProvider
{
    async fn meta_data(
        &self,
        id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let id: CopernicusDataId =
            id.clone()
                .try_into()
                .map_err(|_| geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(Error::DataIdTypeMissMatch),
                })?;

        match id.0 {
            CopernicusDataspaceLayerId::Sentinel2(Sentinel2LayerId { product_band, zone }) => {
                Ok(self.sentinel2_meta_data(product_band, zone))
            }
        }
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for CopernicusDataspaceDataProvider
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
        Err(geoengine_operators::error::Error::NotImplemented)
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for CopernicusDataspaceDataProvider
{
    async fn meta_data(
        &self,
        _id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotImplemented)
    }
}
