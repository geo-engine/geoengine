mod sentinel_s2_l2a_cogs;

use crate::contexts::GeoEngineDb;
use crate::error::Result;
use crate::layers::external::{DataProvider, DataProviderDefinition};
use async_trait::async_trait;
use geoengine_datatypes::dataset::DataProviderId;
pub use sentinel_s2_l2a_cogs::{
    GdalRetries, SentinelS2L2ACogsProviderDefinition, StacApiRetries, StacBand, StacZone,
};
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")] // TODO: rename_all = "camelCase"
#[allow(clippy::enum_variant_names)] // TODO: think about better names
pub enum TypedProDataProviderDefinition {
    SentinelS2L2ACogsProviderDefinition(SentinelS2L2ACogsProviderDefinition),
}

impl From<SentinelS2L2ACogsProviderDefinition> for TypedProDataProviderDefinition {
    fn from(def: SentinelS2L2ACogsProviderDefinition) -> Self {
        Self::SentinelS2L2ACogsProviderDefinition(def)
    }
}

impl<D: GeoEngineDb> From<TypedProDataProviderDefinition> for Box<dyn DataProviderDefinition<D>> {
    fn from(typed: TypedProDataProviderDefinition) -> Self {
        match typed {
            TypedProDataProviderDefinition::SentinelS2L2ACogsProviderDefinition(def) => {
                Box::new(def)
            }
        }
    }
}

impl<D: GeoEngineDb> AsRef<dyn DataProviderDefinition<D>> for TypedProDataProviderDefinition {
    fn as_ref(&self) -> &(dyn DataProviderDefinition<D> + 'static) {
        match self {
            Self::SentinelS2L2ACogsProviderDefinition(def) => def,
        }
    }
}

#[async_trait]
impl<D: GeoEngineDb> DataProviderDefinition<D> for TypedProDataProviderDefinition {
    async fn initialize(self: Box<Self>, db: D) -> Result<Box<dyn DataProvider>> {
        match *self {
            Self::SentinelS2L2ACogsProviderDefinition(def) => Box::new(def).initialize(db).await,
        }
    }

    fn type_name(&self) -> &'static str {
        match self {
            Self::SentinelS2L2ACogsProviderDefinition(_) => "SentinelS2L2ACogsProviderDefinition",
        }
    }

    fn name(&self) -> String {
        match self {
            Self::SentinelS2L2ACogsProviderDefinition(def) => {
                DataProviderDefinition::<D>::name(def)
            }
        }
    }

    fn id(&self) -> DataProviderId {
        match self {
            Self::SentinelS2L2ACogsProviderDefinition(def) => DataProviderDefinition::<D>::id(def),
        }
    }
}
