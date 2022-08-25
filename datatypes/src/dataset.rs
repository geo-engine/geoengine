use crate::identifier;
use serde::{Deserialize, Serialize};

identifier!(DataProviderId);

// Identifier for datasets managed by Geo Engine
identifier!(DatasetId);

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize, utoipa::Component)]
#[serde(rename_all = "camelCase", tag = "type")]
/// The identifier for loadable data. It is used in the source operators to get the loading info (aka parametrization)
/// for accessing the data. Internal data is loaded from datasets, external from `DataProvider`s.
pub enum DataId {
    #[serde(rename_all = "camelCase")]
    Internal {
        dataset_id: DatasetId,
    },
    External(ExternalDataId),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, utoipa::Component)]
pub struct LayerId(pub String);

impl std::fmt::Display for LayerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize, utoipa::Component)]
#[serde(rename_all = "camelCase")]
pub struct ExternalDataId {
    pub provider_id: DataProviderId,
    pub layer_id: LayerId,
}

impl DataId {
    pub fn internal(&self) -> Option<DatasetId> {
        if let Self::Internal {
            dataset_id: dataset,
        } = self
        {
            return Some(*dataset);
        }
        None
    }

    pub fn external(&self) -> Option<ExternalDataId> {
        if let Self::External(id) = self {
            return Some(id.clone());
        }
        None
    }
}

impl From<DatasetId> for DataId {
    fn from(value: DatasetId) -> Self {
        DataId::Internal { dataset_id: value }
    }
}

impl From<ExternalDataId> for DataId {
    fn from(value: ExternalDataId) -> Self {
        DataId::External(value)
    }
}
