use crate::identifier;
use serde::{Deserialize, Serialize};

identifier!(LayerProviderId);

identifier!(InternalDatasetId); // TODO: rename to DatasetId as there are no external datasets anymore

identifier!(StagingDatasetId);

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
// TODO: distinguish between Datasets (stuff MANAGED by Geo Engine) and Data (stuff LOADABLE by Geo Engine)
//       DatasetId should be used to refer to local dataset and DataId (or similar) should differentiate
//       between local datasets and external providers
pub enum DatasetId {
    #[serde(rename_all = "camelCase")]
    Internal {
        dataset_id: InternalDatasetId,
    },
    External(ExternalDatasetId),
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalDatasetId {
    pub provider_id: LayerProviderId,
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
