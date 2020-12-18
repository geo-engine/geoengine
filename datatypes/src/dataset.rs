use crate::identifier;
use serde::{Deserialize, Serialize};

identifier!(DataSetProviderId);

identifier!(InternalDataSetId);

identifier!(StagingDataSetId);

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub enum DataSetId {
    Internal(InternalDataSetId),
    Staging(StagingDataSetId),
    External(ExternalDataSetId),
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub struct ExternalDataSetId {
    pub provider: DataSetProviderId,
    pub id: String, // TODO: generic or enum?
}

impl DataSetId {
    pub fn internal(&self) -> Option<InternalDataSetId> {
        if let Self::Internal(id) = self {
            return Some(*id);
        }
        None
    }

    pub fn staging(&self) -> Option<StagingDataSetId> {
        if let Self::Staging(id) = self {
            return Some(*id);
        }
        None
    }

    pub fn external(&self) -> Option<ExternalDataSetId> {
        if let Self::External(id) = self {
            return Some(id.clone());
        }
        None
    }
}
