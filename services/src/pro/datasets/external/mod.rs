mod sentinel_s2_l2a_cogs;

use crate::api::model::datatypes::DataProviderId;
use crate::error::Result;
use crate::layers::external::{DataProvider, DataProviderDefinition};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use sentinel_s2_l2a_cogs::{
    GdalRetries, SentinelDataset, SentinelS2L2ACogsProviderDefinition,
    SentinelS2L2aCogsDataProvider, SentinelS2L2aCogsMetaData, StacApiRetries, StacBand, StacZone,
};

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

impl From<TypedProDataProviderDefinition> for Box<dyn DataProviderDefinition> {
    fn from(typed: TypedProDataProviderDefinition) -> Self {
        match typed {
            TypedProDataProviderDefinition::SentinelS2L2ACogsProviderDefinition(def) => {
                Box::new(def)
            }
        }
    }
}

impl AsRef<dyn DataProviderDefinition> for TypedProDataProviderDefinition {
    fn as_ref(&self) -> &(dyn DataProviderDefinition + 'static) {
        match self {
            Self::SentinelS2L2ACogsProviderDefinition(def) => def,
        }
    }
}

#[async_trait]
impl DataProviderDefinition for TypedProDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> Result<Box<dyn DataProvider>> {
        match *self {
            Self::SentinelS2L2ACogsProviderDefinition(def) => Box::new(def).initialize().await,
        }
    }

    fn type_name(&self) -> &'static str {
        match self {
            Self::SentinelS2L2ACogsProviderDefinition(_) => "SentinelS2L2ACogsProviderDefinition",
        }
    }

    fn name(&self) -> String {
        match self {
            Self::SentinelS2L2ACogsProviderDefinition(def) => def.name(),
        }
    }

    fn id(&self) -> DataProviderId {
        match self {
            Self::SentinelS2L2ACogsProviderDefinition(def) => def.id(),
        }
    }
}
