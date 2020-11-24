use crate::identifier;
use serde::{Deserialize, Serialize};

identifier!(DataSetProviderId);

identifier!(InternalDataSetId);

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub enum DataSetId {
    Internal(InternalDataSetId),
    External(ExternalDataSetId),
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub struct ExternalDataSetId {
    pub provider: DataSetProviderId,
    pub id: String, // TODO: generic or enum?
}
