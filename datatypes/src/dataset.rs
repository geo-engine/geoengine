use crate::identifier;
use serde::{Deserialize, Serialize};

identifier!(DatasetProviderId);

identifier!(InternalDatasetId);

identifier!(StagingDatasetId);

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub enum DatasetId {
    Internal(InternalDatasetId),
    Staging(StagingDatasetId),
    External(ExternalDatasetId),
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub struct ExternalDatasetId {
    pub provider: DatasetProviderId,
    pub id: String, // TODO: generic or enum?
}

impl DatasetId {
    pub fn internal(&self) -> Option<InternalDatasetId> {
        if let Self::Internal(id) = self {
            return Some(*id);
        }
        None
    }

    pub fn staging(&self) -> Option<StagingDatasetId> {
        if let Self::Staging(id) = self {
            return Some(*id);
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
        DatasetId::Internal(value)
    }
}

impl From<StagingDatasetId> for DatasetId {
    fn from(value: StagingDatasetId) -> Self {
        DatasetId::Staging(value)
    }
}

impl From<ExternalDatasetId> for DatasetId {
    fn from(value: ExternalDatasetId) -> Self {
        DatasetId::External(value)
    }
}
