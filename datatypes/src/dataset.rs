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
