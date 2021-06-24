use crate::identifier;
use serde::{Deserialize, Serialize};

identifier!(DatasetProviderId);

identifier!(InternalDatasetId);

identifier!(StagingDatasetId);

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum DatasetId {
    Internal { dataset_id: InternalDatasetId },
    External(ExternalDatasetId),
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub struct ExternalDatasetId {
    pub provider_id: DatasetProviderId,
    pub dataset_id: String,
}

impl DatasetId {
    pub fn internal(&self) -> Option<InternalDatasetId> {
        if let Self::Internal {
            dataset_id: dataset,
        } = self
        {
            return Some(*dataset);
        }
        None
    }

    pub fn external(&self) -> Option<ExternalDatasetId> {
        if let Self::External(id) = self {
            return Some(id.clone());
        }
        None
    }
}

impl From<InternalDatasetId> for DatasetId {
    fn from(value: InternalDatasetId) -> Self {
        DatasetId::Internal { dataset_id: value }
    }
}

impl From<ExternalDatasetId> for DatasetId {
    fn from(value: ExternalDatasetId) -> Self {
        DatasetId::External(value)
    }
}
