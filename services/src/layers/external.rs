use super::listing::LayerCollectionProvider;
use crate::api::model::datatypes::{DataId, DataProviderId};
use crate::datasets::external::aruna::ArunaDataProviderDefinition;
use crate::datasets::external::edr::EdrDataProviderDefinition;
use crate::datasets::external::gbif::GbifDataProviderDefinition;
use crate::datasets::external::gfbio_abcd::GfbioAbcdDataProviderDefinition;
use crate::datasets::external::gfbio_collections::GfbioCollectionsDataProviderDefinition;
use crate::datasets::external::netcdfcf::EbvPortalDataProviderDefinition;
use crate::datasets::external::netcdfcf::NetCdfCfDataProviderDefinition;
use crate::datasets::external::pangaea::PangaeaDataProviderDefinition;
use crate::datasets::listing::ProvenanceOutput;
use crate::error::Result;
use async_trait::async_trait;
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_datatypes::util::AsAny;
use geoengine_operators::engine::{
    MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};
use serde::{Deserialize, Serialize};

#[async_trait]
pub trait DataProviderDefinition:
    CloneableDataProviderDefinition + Send + Sync + std::fmt::Debug
{
    /// create the actual provider for data listing and access
    async fn initialize(self: Box<Self>) -> Result<Box<dyn DataProvider>>;

    /// the type of the provider
    fn type_name(&self) -> &'static str;

    /// name of the external data source
    fn name(&self) -> String;

    /// id of the provider
    fn id(&self) -> DataProviderId;
}

pub trait CloneableDataProviderDefinition {
    fn clone_boxed_provider(&self) -> Box<dyn DataProviderDefinition>;
}

impl<T> CloneableDataProviderDefinition for T
where
    T: 'static + DataProviderDefinition + Clone,
{
    fn clone_boxed_provider(&self) -> Box<dyn DataProviderDefinition> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn DataProviderDefinition> {
    fn clone(&self) -> Box<dyn DataProviderDefinition> {
        self.clone_boxed_provider()
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
    GbifDataProviderDefinition(GbifDataProviderDefinition),
    GfbioAbcdDataProviderDefinition(GfbioAbcdDataProviderDefinition),
    GfbioCollectionsDataProviderDefinition(GfbioCollectionsDataProviderDefinition),
    EbvPortalDataProviderDefinition(EbvPortalDataProviderDefinition),
    NetCdfCfDataProviderDefinition(NetCdfCfDataProviderDefinition),
    PangaeaDataProviderDefinition(PangaeaDataProviderDefinition),
    EdrDataProviderDefinition(EdrDataProviderDefinition),
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

impl From<TypedDataProviderDefinition> for Box<dyn DataProviderDefinition> {
    fn from(typed: TypedDataProviderDefinition) -> Self {
        match typed {
            TypedDataProviderDefinition::ArunaDataProviderDefinition(def) => Box::new(def),
            TypedDataProviderDefinition::GbifDataProviderDefinition(def) => Box::new(def),
            TypedDataProviderDefinition::GfbioAbcdDataProviderDefinition(def) => Box::new(def),
            TypedDataProviderDefinition::GfbioCollectionsDataProviderDefinition(def) => {
                Box::new(def)
            }
            TypedDataProviderDefinition::EbvPortalDataProviderDefinition(def) => Box::new(def),
            TypedDataProviderDefinition::NetCdfCfDataProviderDefinition(def) => Box::new(def),
            TypedDataProviderDefinition::PangaeaDataProviderDefinition(def) => Box::new(def),
            TypedDataProviderDefinition::EdrDataProviderDefinition(def) => Box::new(def),
        }
    }
}

impl AsRef<dyn DataProviderDefinition> for TypedDataProviderDefinition {
    fn as_ref(&self) -> &(dyn DataProviderDefinition + 'static) {
        match self {
            TypedDataProviderDefinition::ArunaDataProviderDefinition(def) => def,
            TypedDataProviderDefinition::GbifDataProviderDefinition(def) => def,
            TypedDataProviderDefinition::GfbioAbcdDataProviderDefinition(def) => def,
            TypedDataProviderDefinition::GfbioCollectionsDataProviderDefinition(def) => def,
            TypedDataProviderDefinition::EbvPortalDataProviderDefinition(def) => def,
            TypedDataProviderDefinition::NetCdfCfDataProviderDefinition(def) => def,
            TypedDataProviderDefinition::PangaeaDataProviderDefinition(def) => def,
            TypedDataProviderDefinition::EdrDataProviderDefinition(def) => def,
        }
    }
}

#[async_trait]
impl DataProviderDefinition for TypedDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> Result<Box<dyn DataProvider>> {
        match *self {
            TypedDataProviderDefinition::ArunaDataProviderDefinition(def) => {
                Box::new(def).initialize().await
            }
            TypedDataProviderDefinition::GbifDataProviderDefinition(def) => {
                Box::new(def).initialize().await
            }
            TypedDataProviderDefinition::GfbioAbcdDataProviderDefinition(def) => {
                Box::new(def).initialize().await
            }
            TypedDataProviderDefinition::GfbioCollectionsDataProviderDefinition(def) => {
                Box::new(def).initialize().await
            }
            TypedDataProviderDefinition::EbvPortalDataProviderDefinition(def) => {
                Box::new(def).initialize().await
            }
            TypedDataProviderDefinition::NetCdfCfDataProviderDefinition(def) => {
                Box::new(def).initialize().await
            }
            TypedDataProviderDefinition::PangaeaDataProviderDefinition(def) => {
                Box::new(def).initialize().await
            }
            TypedDataProviderDefinition::EdrDataProviderDefinition(def) => {
                Box::new(def).initialize().await
            }
        }
    }

    fn type_name(&self) -> &'static str {
        match self {
            TypedDataProviderDefinition::ArunaDataProviderDefinition(def) => def.type_name(),
            TypedDataProviderDefinition::GbifDataProviderDefinition(def) => def.type_name(),
            TypedDataProviderDefinition::GfbioAbcdDataProviderDefinition(def) => def.type_name(),
            TypedDataProviderDefinition::GfbioCollectionsDataProviderDefinition(def) => {
                def.type_name()
            }
            TypedDataProviderDefinition::EbvPortalDataProviderDefinition(def) => def.type_name(),
            TypedDataProviderDefinition::NetCdfCfDataProviderDefinition(def) => def.type_name(),
            TypedDataProviderDefinition::PangaeaDataProviderDefinition(def) => def.type_name(),
            TypedDataProviderDefinition::EdrDataProviderDefinition(def) => def.type_name(),
        }
    }

    fn name(&self) -> String {
        match self {
            TypedDataProviderDefinition::ArunaDataProviderDefinition(def) => def.name(),
            TypedDataProviderDefinition::GbifDataProviderDefinition(def) => def.name(),
            TypedDataProviderDefinition::GfbioAbcdDataProviderDefinition(def) => def.name(),
            TypedDataProviderDefinition::GfbioCollectionsDataProviderDefinition(def) => def.name(),
            TypedDataProviderDefinition::EbvPortalDataProviderDefinition(def) => def.name(),
            TypedDataProviderDefinition::NetCdfCfDataProviderDefinition(def) => def.name(),
            TypedDataProviderDefinition::PangaeaDataProviderDefinition(def) => def.name(),
            TypedDataProviderDefinition::EdrDataProviderDefinition(def) => def.name(),
        }
    }

    fn id(&self) -> DataProviderId {
        match self {
            TypedDataProviderDefinition::ArunaDataProviderDefinition(def) => def.id(),
            TypedDataProviderDefinition::GbifDataProviderDefinition(def) => def.id(),
            TypedDataProviderDefinition::GfbioAbcdDataProviderDefinition(def) => def.id(),
            TypedDataProviderDefinition::GfbioCollectionsDataProviderDefinition(def) => def.id(),
            TypedDataProviderDefinition::EbvPortalDataProviderDefinition(def) => def.id(),
            TypedDataProviderDefinition::NetCdfCfDataProviderDefinition(def) => def.id(),
            TypedDataProviderDefinition::PangaeaDataProviderDefinition(def) => def.id(),
            TypedDataProviderDefinition::EdrDataProviderDefinition(def) => def.id(),
        }
    }
}
