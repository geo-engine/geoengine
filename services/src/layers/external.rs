use super::listing::LayerCollectionProvider;
use crate::contexts::GeoEngineDb;
use crate::datasets::dataset_listing_provider::DatasetLayerListingProviderDefinition;
use crate::datasets::external::WildliveDataConnectorDefinition;
use crate::datasets::external::aruna::ArunaDataProviderDefinition;
use crate::datasets::external::edr::EdrDataProviderDefinition;
use crate::datasets::external::gbif::GbifDataProviderDefinition;
use crate::datasets::external::gfbio_abcd::GfbioAbcdDataProviderDefinition;
use crate::datasets::external::gfbio_collections::GfbioCollectionsDataProviderDefinition;
use crate::datasets::external::netcdfcf::EbvPortalDataProviderDefinition;
use crate::datasets::external::netcdfcf::NetCdfCfDataProviderDefinition;
use crate::datasets::external::pangaea::PangaeaDataProviderDefinition;
use crate::datasets::external::{
    CopernicusDataspaceDataProviderDefinition, SentinelS2L2ACogsProviderDefinition,
};
use crate::datasets::listing::ProvenanceOutput;
use crate::error::Result;
use async_trait::async_trait;
use geoengine_datatypes::dataset::DataId;
use geoengine_datatypes::dataset::DataProviderId;
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_datatypes::util::AsAny;
use geoengine_operators::engine::{
    MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};
use serde::{Deserialize, Serialize};

#[async_trait]
pub trait DataProviderDefinition<D: GeoEngineDb>: Send + Sync + std::fmt::Debug {
    /// create the actual provider for data listing and access
    async fn initialize(self: Box<Self>, db: D) -> Result<Box<dyn DataProvider>>;

    /// the type of the provider
    fn type_name(&self) -> &'static str;

    /// name of the external data source
    fn name(&self) -> String;

    /// id of the provider
    fn id(&self) -> DataProviderId;

    /// priority of the provider
    fn priority(&self) -> i16 {
        0
    }
}

/// A provider of layers that are not hosted by Geo Engine itself but some external party
// TODO: Authorization: the provider needs to accept credentials for the external data source.
//       The credentials should be generic s.t. they are independent of the Session type and
//       extensible to new provider types. E.g. a key-value map of strings where the provider
//       checks that the necessary information is present and how they are incorporated in
//       the requests.
#[async_trait]
pub trait DataProvider: LayerCollectionProvider
    + MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    + MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    + MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    + Send
    + Sync
    + std::fmt::Debug
    + AsAny
{
    // TODO: unify provenance method for internal and external provider as a separate trait. We need to figure out session handling before, though.
    async fn provenance(&self, id: &DataId) -> Result<ProvenanceOutput>;
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")] // TODO: rename_all = "camelCase"
#[allow(clippy::enum_variant_names)] // TODO: think about better names
pub enum TypedDataProviderDefinition {
    ArunaDataProviderDefinition(ArunaDataProviderDefinition),
    CopernicusDataspaceDataProviderDefinition(CopernicusDataspaceDataProviderDefinition),
    DatasetLayerListingProviderDefinition(DatasetLayerListingProviderDefinition),
    EbvPortalDataProviderDefinition(EbvPortalDataProviderDefinition),
    EdrDataProviderDefinition(EdrDataProviderDefinition),
    GbifDataProviderDefinition(GbifDataProviderDefinition),
    GfbioAbcdDataProviderDefinition(GfbioAbcdDataProviderDefinition),
    GfbioCollectionsDataProviderDefinition(GfbioCollectionsDataProviderDefinition),
    NetCdfCfDataProviderDefinition(NetCdfCfDataProviderDefinition),
    PangaeaDataProviderDefinition(PangaeaDataProviderDefinition),
    SentinelS2L2ACogsProviderDefinition(SentinelS2L2ACogsProviderDefinition),
    WildliveDataConnectorDefinition(WildliveDataConnectorDefinition),
}

impl From<ArunaDataProviderDefinition> for TypedDataProviderDefinition {
    fn from(def: ArunaDataProviderDefinition) -> Self {
        Self::ArunaDataProviderDefinition(def)
    }
}

impl From<GbifDataProviderDefinition> for TypedDataProviderDefinition {
    fn from(def: GbifDataProviderDefinition) -> Self {
        Self::GbifDataProviderDefinition(def)
    }
}

impl From<GfbioAbcdDataProviderDefinition> for TypedDataProviderDefinition {
    fn from(def: GfbioAbcdDataProviderDefinition) -> Self {
        Self::GfbioAbcdDataProviderDefinition(def)
    }
}

impl From<GfbioCollectionsDataProviderDefinition> for TypedDataProviderDefinition {
    fn from(def: GfbioCollectionsDataProviderDefinition) -> Self {
        Self::GfbioCollectionsDataProviderDefinition(def)
    }
}

impl From<EbvPortalDataProviderDefinition> for TypedDataProviderDefinition {
    fn from(def: EbvPortalDataProviderDefinition) -> Self {
        Self::EbvPortalDataProviderDefinition(def)
    }
}

impl From<NetCdfCfDataProviderDefinition> for TypedDataProviderDefinition {
    fn from(def: NetCdfCfDataProviderDefinition) -> Self {
        Self::NetCdfCfDataProviderDefinition(def)
    }
}

impl From<PangaeaDataProviderDefinition> for TypedDataProviderDefinition {
    fn from(def: PangaeaDataProviderDefinition) -> Self {
        Self::PangaeaDataProviderDefinition(def)
    }
}

impl From<EdrDataProviderDefinition> for TypedDataProviderDefinition {
    fn from(def: EdrDataProviderDefinition) -> Self {
        Self::EdrDataProviderDefinition(def)
    }
}

impl From<DatasetLayerListingProviderDefinition> for TypedDataProviderDefinition {
    fn from(def: DatasetLayerListingProviderDefinition) -> Self {
        Self::DatasetLayerListingProviderDefinition(def)
    }
}

impl From<SentinelS2L2ACogsProviderDefinition> for TypedDataProviderDefinition {
    fn from(def: SentinelS2L2ACogsProviderDefinition) -> Self {
        Self::SentinelS2L2ACogsProviderDefinition(def)
    }
}

impl From<WildliveDataConnectorDefinition> for TypedDataProviderDefinition {
    fn from(def: WildliveDataConnectorDefinition) -> Self {
        Self::WildliveDataConnectorDefinition(def)
    }
}

impl<D: GeoEngineDb> From<TypedDataProviderDefinition> for Box<dyn DataProviderDefinition<D>> {
    fn from(typed: TypedDataProviderDefinition) -> Self {
        match typed {
            TypedDataProviderDefinition::ArunaDataProviderDefinition(def) => Box::new(def),
            TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(def) => {
                Box::new(def)
            }
            TypedDataProviderDefinition::GbifDataProviderDefinition(def) => Box::new(def),
            TypedDataProviderDefinition::GfbioAbcdDataProviderDefinition(def) => Box::new(def),
            TypedDataProviderDefinition::GfbioCollectionsDataProviderDefinition(def) => {
                Box::new(def)
            }
            TypedDataProviderDefinition::EbvPortalDataProviderDefinition(def) => Box::new(def),
            TypedDataProviderDefinition::NetCdfCfDataProviderDefinition(def) => Box::new(def),
            TypedDataProviderDefinition::PangaeaDataProviderDefinition(def) => Box::new(def),
            TypedDataProviderDefinition::EdrDataProviderDefinition(def) => Box::new(def),
            TypedDataProviderDefinition::SentinelS2L2ACogsProviderDefinition(def) => Box::new(def),
            TypedDataProviderDefinition::CopernicusDataspaceDataProviderDefinition(def) => {
                Box::new(def)
            }
            TypedDataProviderDefinition::WildliveDataConnectorDefinition(def) => Box::new(def),
        }
    }
}

impl<D: GeoEngineDb> AsRef<dyn DataProviderDefinition<D>> for TypedDataProviderDefinition {
    fn as_ref(&self) -> &(dyn DataProviderDefinition<D> + 'static) {
        match self {
            Self::ArunaDataProviderDefinition(def) => def,
            Self::DatasetLayerListingProviderDefinition(def) => def,
            Self::GbifDataProviderDefinition(def) => def,
            Self::GfbioAbcdDataProviderDefinition(def) => def,
            Self::GfbioCollectionsDataProviderDefinition(def) => def,
            Self::EbvPortalDataProviderDefinition(def) => def,
            Self::NetCdfCfDataProviderDefinition(def) => def,
            Self::PangaeaDataProviderDefinition(def) => def,
            Self::EdrDataProviderDefinition(def) => def,
            Self::CopernicusDataspaceDataProviderDefinition(def) => def,
            Self::SentinelS2L2ACogsProviderDefinition(def) => def,
            Self::WildliveDataConnectorDefinition(def) => def,
        }
    }
}

#[async_trait]
impl<D: GeoEngineDb> DataProviderDefinition<D> for TypedDataProviderDefinition {
    async fn initialize(self: Box<Self>, db: D) -> Result<Box<dyn DataProvider>> {
        match *self {
            Self::ArunaDataProviderDefinition(def) => Box::new(def).initialize(db).await,
            Self::DatasetLayerListingProviderDefinition(def) => Box::new(def).initialize(db).await,
            Self::GbifDataProviderDefinition(def) => Box::new(def).initialize(db).await,
            Self::GfbioAbcdDataProviderDefinition(def) => Box::new(def).initialize(db).await,
            Self::GfbioCollectionsDataProviderDefinition(def) => Box::new(def).initialize(db).await,
            Self::EbvPortalDataProviderDefinition(def) => Box::new(def).initialize(db).await,
            Self::NetCdfCfDataProviderDefinition(def) => Box::new(def).initialize(db).await,
            Self::PangaeaDataProviderDefinition(def) => Box::new(def).initialize(db).await,
            Self::EdrDataProviderDefinition(def) => Box::new(def).initialize(db).await,
            Self::CopernicusDataspaceDataProviderDefinition(def) => {
                Box::new(def).initialize(db).await
            }
            Self::SentinelS2L2ACogsProviderDefinition(def) => Box::new(def).initialize(db).await,
            Self::WildliveDataConnectorDefinition(def) => Box::new(def).initialize(db).await,
        }
    }

    fn type_name(&self) -> &'static str {
        match self {
            Self::ArunaDataProviderDefinition(def) => DataProviderDefinition::<D>::type_name(def),
            Self::DatasetLayerListingProviderDefinition(def) => {
                DataProviderDefinition::<D>::type_name(def)
            }
            Self::GbifDataProviderDefinition(def) => DataProviderDefinition::<D>::type_name(def),
            Self::GfbioAbcdDataProviderDefinition(def) => {
                DataProviderDefinition::<D>::type_name(def)
            }
            Self::GfbioCollectionsDataProviderDefinition(def) => {
                DataProviderDefinition::<D>::type_name(def)
            }
            Self::EbvPortalDataProviderDefinition(def) => {
                DataProviderDefinition::<D>::type_name(def)
            }
            Self::NetCdfCfDataProviderDefinition(def) => {
                DataProviderDefinition::<D>::type_name(def)
            }
            Self::PangaeaDataProviderDefinition(def) => DataProviderDefinition::<D>::type_name(def),
            Self::EdrDataProviderDefinition(def) => DataProviderDefinition::<D>::type_name(def),
            Self::CopernicusDataspaceDataProviderDefinition(_) => "CioDataProviderDefinition",
            Self::SentinelS2L2ACogsProviderDefinition(_) => "SentinelS2L2ACogsProviderDefinition",
            Self::WildliveDataConnectorDefinition(_) => "WildliveDataConnectorDefinition",
        }
    }

    fn name(&self) -> String {
        match self {
            Self::ArunaDataProviderDefinition(def) => DataProviderDefinition::<D>::name(def),
            Self::DatasetLayerListingProviderDefinition(def) => {
                DataProviderDefinition::<D>::name(def)
            }
            Self::GbifDataProviderDefinition(def) => DataProviderDefinition::<D>::name(def),
            Self::GfbioAbcdDataProviderDefinition(def) => DataProviderDefinition::<D>::name(def),
            Self::GfbioCollectionsDataProviderDefinition(def) => {
                DataProviderDefinition::<D>::name(def)
            }
            Self::EbvPortalDataProviderDefinition(def) => DataProviderDefinition::<D>::name(def),
            Self::NetCdfCfDataProviderDefinition(def) => DataProviderDefinition::<D>::name(def),
            Self::PangaeaDataProviderDefinition(def) => DataProviderDefinition::<D>::name(def),
            Self::EdrDataProviderDefinition(def) => DataProviderDefinition::<D>::name(def),
            Self::CopernicusDataspaceDataProviderDefinition(def) => {
                DataProviderDefinition::<D>::name(def)
            }
            Self::SentinelS2L2ACogsProviderDefinition(def) => {
                DataProviderDefinition::<D>::name(def)
            }
            Self::WildliveDataConnectorDefinition(def) => DataProviderDefinition::<D>::name(def),
        }
    }

    fn id(&self) -> DataProviderId {
        match self {
            Self::ArunaDataProviderDefinition(def) => DataProviderDefinition::<D>::id(def),
            Self::DatasetLayerListingProviderDefinition(def) => {
                DataProviderDefinition::<D>::id(def)
            }
            Self::GbifDataProviderDefinition(def) => DataProviderDefinition::<D>::id(def),
            Self::GfbioAbcdDataProviderDefinition(def) => DataProviderDefinition::<D>::id(def),
            Self::GfbioCollectionsDataProviderDefinition(def) => {
                DataProviderDefinition::<D>::id(def)
            }
            Self::EbvPortalDataProviderDefinition(def) => DataProviderDefinition::<D>::id(def),
            Self::NetCdfCfDataProviderDefinition(def) => DataProviderDefinition::<D>::id(def),
            Self::PangaeaDataProviderDefinition(def) => DataProviderDefinition::<D>::id(def),
            Self::EdrDataProviderDefinition(def) => DataProviderDefinition::<D>::id(def),
            Self::CopernicusDataspaceDataProviderDefinition(def) => {
                DataProviderDefinition::<D>::id(def)
            }
            Self::SentinelS2L2ACogsProviderDefinition(def) => DataProviderDefinition::<D>::id(def),
            Self::WildliveDataConnectorDefinition(def) => DataProviderDefinition::<D>::id(def),
        }
    }

    fn priority(&self) -> i16 {
        match self {
            Self::ArunaDataProviderDefinition(def) => DataProviderDefinition::<D>::priority(def),
            Self::DatasetLayerListingProviderDefinition(def) => {
                DataProviderDefinition::<D>::priority(def)
            }
            Self::GbifDataProviderDefinition(def) => DataProviderDefinition::<D>::priority(def),
            Self::GfbioAbcdDataProviderDefinition(def) => {
                DataProviderDefinition::<D>::priority(def)
            }
            Self::GfbioCollectionsDataProviderDefinition(def) => {
                DataProviderDefinition::<D>::priority(def)
            }
            Self::EbvPortalDataProviderDefinition(def) => {
                DataProviderDefinition::<D>::priority(def)
            }
            Self::NetCdfCfDataProviderDefinition(def) => DataProviderDefinition::<D>::priority(def),
            Self::PangaeaDataProviderDefinition(def) => DataProviderDefinition::<D>::priority(def),
            Self::EdrDataProviderDefinition(def) => DataProviderDefinition::<D>::priority(def),
            Self::CopernicusDataspaceDataProviderDefinition(def) => {
                DataProviderDefinition::<D>::priority(def)
            }
            Self::SentinelS2L2ACogsProviderDefinition(def) => {
                DataProviderDefinition::<D>::priority(def)
            }
            Self::WildliveDataConnectorDefinition(def) => {
                DataProviderDefinition::<D>::priority(def)
            }
        }
    }
}
